use {
  self::{inscription_updater::InscriptionUpdater, rune_updater::RuneUpdater},
  super::{fetcher::Fetcher, transaction_cache::TransactionCache, *},
  futures::future::try_join_all,
  std::sync::MutexGuard,
  tokio::sync::{
    broadcast::{self, error::TryRecvError},
    mpsc::{self},
  },
};

mod inscription_updater;
mod rune_updater;

pub(crate) struct BlockData {
  pub(crate) header: Header,
  pub(crate) txdata: Vec<(Transaction, Txid)>,
}

impl From<Block> for BlockData {
  fn from(block: Block) -> Self {
    BlockData {
      header: block.header,
      txdata: block
        .txdata
        .into_iter()
        .map(|transaction| {
          let txid = transaction.txid();
          (transaction, txid)
        })
        .collect(),
    }
  }
}

pub(crate) struct Updater<'index, 'cache> {
  pub(super) height: u32,
  pub(super) index: &'index Index,
  pub(super) outputs_cached: u64,
  pub(super) outputs_cached0: u64,
  pub(super) outputs_cached1: u64,
  pub(super) outputs_cached2: u64,
  pub(super) outputs_cached3: u64,
  pub(super) outputs_dust_count: u64,
  pub(super) outputs_count: u64,
  pub(super) outputs_traversed: u64,
  pub(super) sat_ranges_since_flush: u64,
  pub(super) cache: &'cache mut MutexGuard<'index, TransactionCache>,
}

impl<'index, 'cache> Updater<'index, 'cache>
where
  'index: 'cache, // 显式声明
{
  pub(crate) fn update_index(&mut self) -> Result {
    let start = Instant::now();
    let starting_height = u32::try_from(self.index.client.get_block_count()?).unwrap() + 1;
    let starting_index_height = self.height;

    // 使用事务缓存添加写入事务时间戳
    let timestamp = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .map(|duration| duration.as_millis())
      .unwrap_or(0);
    self
      .cache
      .add_write_transaction_timestamp(self.height, timestamp);

    let mut progress_bar = if cfg!(test)
      || log_enabled!(log::Level::Info)
      || starting_height <= self.height
      || self.index.settings.integration_test()
    {
      None
    } else {
      let progress_bar = ProgressBar::new(starting_height.into());
      progress_bar.set_position(self.height.into());
      progress_bar.set_style(
        ProgressStyle::with_template("[indexing blocks] {wide_bar} {pos}/{len}").unwrap(),
      );
      Some(progress_bar)
    };

    let rx = Self::fetch_blocks_from(self.index, self.height, self.index.index_sats)?;

    let (mut output_sender, mut txout_receiver) = Self::spawn_fetcher(self.index)?;

    let mut uncommitted = 0;
    let mut utxo_cache = HashMap::new();
    while let Ok(block) = rx.recv() {
      self.index_block(
        &mut output_sender,
        &mut txout_receiver,
        block,
        &mut utxo_cache,
      )?;

      if let Some(progress_bar) = &mut progress_bar {
        progress_bar.inc(1);

        if progress_bar.position() > progress_bar.length().unwrap() {
          if let Ok(count) = self.index.client.get_block_count() {
            progress_bar.set_length(count + 1);
          } else {
            log::warn!("Failed to fetch latest block height");
          }
        }
      }

      uncommitted += 1;

      let mut last_commit = false;
      if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
        last_commit = true;
      }

      if uncommitted == self.index.settings.commit_interval() {
        self.commit(utxo_cache, last_commit)?;
        utxo_cache = HashMap::new();
        uncommitted = 0;
        let height = self
          .cache
          .get_block_height(&self.index.database)?
          .map(|h| h + 1)
          .unwrap_or(0);
        if height != self.height {
          // another update has run between committing and beginning the new
          // write transaction
          break;
        }
        // 使用事务缓存添加写入事务时间戳
        let timestamp = SystemTime::now()
          .duration_since(SystemTime::UNIX_EPOCH)?
          .as_millis();
        self
          .cache
          .add_write_transaction_timestamp(self.height, timestamp);
      }

      if last_commit {
        break;
      }
    }

    if starting_index_height == 0 && self.height > 0 {
      // 使用事务缓存更新统计信息
      self.cache.update_statistic(
        Statistic::InitialSyncTime,
        u64::try_from(start.elapsed().as_micros())?,
      );
    }

    if uncommitted > 0 {
      self.commit(utxo_cache, true)?;
    }

    if let Some(progress_bar) = &mut progress_bar {
      progress_bar.finish_and_clear();
    }

    Ok(())
  }

  fn fetch_blocks_from(
    index: &Index,
    height: u32,
    index_sats: bool,
  ) -> Result<std::sync::mpsc::Receiver<BlockData>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(32);

    let first_inscription_height = index.first_inscription_height;

    let height_limit = index.height_limit;
    let height_fetch = Arc::new(atomic::AtomicU32::new(height));
    let height_send = Arc::new(atomic::AtomicU32::new(height));
    let should_exit = Arc::new(atomic::AtomicBool::new(false));

    let settings = index.settings.clone();

    thread::spawn(move || {
      let worker_count = 4;
      for _ in 0..worker_count {
        if SHUTTING_DOWN.load(atomic::Ordering::Relaxed)
          || should_exit.load(atomic::Ordering::Relaxed)
        {
          log::debug!("Block receiver shutting down");
          break;
        }

        let height_fetch = Arc::clone(&height_fetch);
        let height_send = Arc::clone(&height_send);
        let should_exit = Arc::clone(&should_exit);
        let tx = tx.clone();
        let settings = settings.clone();

        let client = match settings.bitcoin_rpc_client(None) {
          Ok(client) => client,
          Err(err) => {
            log::error!(
              "updater.fetch_blocks_from get client error: {}",
              err.to_string()
            );
            return;
          }
        };

        thread::spawn(move || loop {
          if SHUTTING_DOWN.load(atomic::Ordering::Relaxed)
            || should_exit.load(atomic::Ordering::Relaxed)
          {
            log::debug!("Block receiver shutting down");
            break;
          }

          let height = height_fetch.fetch_add(1, atomic::Ordering::SeqCst);

          if let Some(height_limit) = height_limit {
            if height >= height_limit {
              break;
            }
          }

          match Self::get_block_with_retries(&client, height, index_sats, first_inscription_height)
          {
            Ok(Some(block)) => loop {
              if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
                log::debug!("Block receiver shutting down");
                return;
              }

              if height_send.load(atomic::Ordering::SeqCst) < height {
                thread::sleep(Duration::from_millis(1));
                continue;
              }

              if let Err(err) = tx.send(block.into()) {
                log::info!("Block receiver disconnected: {err}");
              }
              height_send.fetch_add(1, atomic::Ordering::SeqCst);
              log::info!("Block sent: {height}");
              break;
            },
            Ok(None) => {
              if height_send.load(atomic::Ordering::SeqCst) == height {
                log::debug!(
                  "Failed to fetch block at expected height {height}, exiting all worker threads"
                );
                should_exit.store(true, atomic::Ordering::Relaxed);
              }
              break;
            }
            Err(err) => {
              log::error!("failed to fetch block {height}: {err}");
              if height_send.load(atomic::Ordering::SeqCst) == height {
                log::error!(
                  "Failed to fetch block at expected height {height}, exiting all worker threads"
                );
                should_exit.store(true, atomic::Ordering::Relaxed);
              }
              break;
            }
          }
        });
      }
    });

    Ok(rx)
  }

  #[allow(dead_code)]
  fn fetch_blocks_from_x(
    index: &Index,
    mut height: u32,
    index_sats: bool,
  ) -> Result<std::sync::mpsc::Receiver<BlockData>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(32);

    let height_limit = index.height_limit;

    let client = index.settings.bitcoin_rpc_client(None)?;

    let first_inscription_height = index.first_inscription_height;

    thread::spawn(move || loop {
      if let Some(height_limit) = height_limit {
        if height >= height_limit {
          break;
        }
      }

      match Self::get_block_with_retries(&client, height, index_sats, first_inscription_height) {
        Ok(Some(block)) => {
          if let Err(err) = tx.send(block.into()) {
            log::info!("Block receiver disconnected: {err}");
            break;
          }
          height += 1;
        }
        Ok(None) => break,
        Err(err) => {
          log::error!("failed to fetch block {height}: {err}");
          break;
        }
      }
    });

    Ok(rx)
  }

  fn get_block_with_retries(
    client: &Client,
    height: u32,
    index_sats: bool,
    first_inscription_height: u32,
  ) -> Result<Option<Block>> {
    let mut errors = 0;
    loop {
      match client
        .get_block_hash(height.into())
        .into_option()
        .and_then(|option| {
          option
            .map(|hash| {
              if index_sats || height >= first_inscription_height {
                Ok(client.get_block(&hash)?)
              } else {
                Ok(Block {
                  header: client.get_block_header(&hash)?,
                  txdata: Vec::new(),
                })
              }
            })
            .transpose()
        }) {
        Err(err) => {
          if cfg!(test) {
            return Err(err);
          }

          errors += 1;
          let seconds = 1 << errors;
          log::warn!("failed to fetch block {height}, retrying in {seconds}s: {err}");

          if seconds > 120 {
            log::error!("would sleep for more than 120s, giving up");
            return Err(err);
          }

          thread::sleep(Duration::from_secs(seconds));
        }
        Ok(result) => return Ok(result),
      }
    }
  }

  fn spawn_fetcher(index: &Index) -> Result<(mpsc::Sender<OutPoint>, broadcast::Receiver<TxOut>)> {
    let fetcher = Fetcher::new(&index.settings)?;

    // A block probably has no more than 20k inputs
    const CHANNEL_BUFFER_SIZE: usize = 20_000;

    // Batch 2048 missing inputs at a time, arbitrarily chosen size
    const BATCH_SIZE: usize = 2048;

    let (outpoint_sender, mut outpoint_receiver) = mpsc::channel::<OutPoint>(CHANNEL_BUFFER_SIZE);

    let (txout_sender, txout_receiver) = broadcast::channel::<TxOut>(CHANNEL_BUFFER_SIZE);

    // Default rpcworkqueue in bitcoind is 16, meaning more than 16 concurrent requests will be rejected.
    // Since we are already requesting blocks on a separate thread, and we don't want to break if anything
    // else runs a request, we keep this to 12.
    let parallel_requests: usize = index.settings.bitcoin_rpc_limit().try_into().unwrap();

    thread::spawn(move || {
      let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
      rt.block_on(async move {
        loop {
          let Some(outpoint) = outpoint_receiver.recv().await else {
            log::debug!("Outpoint channel closed");
            return;
          };

          // There's no try_iter on tokio::sync::mpsc::Receiver like std::sync::mpsc::Receiver.
          // So we just loop until BATCH_SIZE doing try_recv until it returns None.
          let mut outpoints = vec![outpoint];
          for _ in 0..BATCH_SIZE - 1 {
            let Ok(outpoint) = outpoint_receiver.try_recv() else {
              break;
            };
            outpoints.push(outpoint);
          }

          // Break outputs into chunks for parallel requests
          let chunk_size = (outpoints.len() / parallel_requests) + 1;
          let mut futs = Vec::with_capacity(parallel_requests);
          for chunk in outpoints.chunks(chunk_size) {
            let txids = chunk.iter().map(|outpoint| outpoint.txid).collect();
            let fut = fetcher.get_transactions(txids);
            futs.push(fut);
          }

          let txs = match try_join_all(futs).await {
            Ok(txs) => txs,
            Err(e) => {
              log::error!("Couldn't receive txs {e}");
              return;
            }
          };

          // Send all tx outputs back in order
          for (i, tx) in txs.iter().flatten().enumerate() {
            let Ok(_) =
              txout_sender.send(tx.output[usize::try_from(outpoints[i].vout).unwrap()].clone())
            else {
              log::error!("Value channel closed unexpectedly");
              return;
            };
          }
        }
      })
    });

    Ok((outpoint_sender, txout_receiver))
  }

  fn index_block(
    &mut self,
    output_sender: &mut mpsc::Sender<OutPoint>,
    txout_receiver: &mut broadcast::Receiver<TxOut>,
    block: BlockData,
    utxo_cache: &mut HashMap<OutPoint, UtxoEntryBuf>,
  ) -> Result<()> {
    log::info!("index_block enter, height: {:?}", self.height);
    Reorg::detect_reorg(&block, self.height, self.index)?;

    let start = Instant::now();
    let mut sat_ranges_written = 0;
    let mut outputs_in_block = 0;

    log::info!(
      "Block {} at {} with {} transactions…",
      self.height,
      timestamp(block.header.time.into()),
      block.txdata.len()
    );

    if self.index.index_inscriptions || self.index.index_addresses || self.index.index_sats {
      self.index_utxo_entries(
        &block,
        txout_receiver,
        output_sender,
        utxo_cache,
        &mut sat_ranges_written,
        &mut outputs_in_block,
      )?;
    }

    if self.index.index_runes && self.height >= self.index.settings.first_rune_height() {
      let runes = self
        .cache
        .get_statistic(&self.index.database, Statistic::Runes)?;

      let mut rune_updater = RuneUpdater {
        event_sender: self.index.event_sender.as_ref(),
        block_time: block.header.time,
        burned: HashMap::new(),
        cache: &mut self.cache,
        client: &self.index.client,
        height: self.height,
        minimum: Rune::minimum_at_height(
          self.index.settings.chain().network(),
          Height(self.height),
        ),
        runes,
        database: &self.index.database,
      };

      for (i, (tx, txid)) in block.txdata.iter().enumerate() {
        rune_updater.index_runes(u32::try_from(i).unwrap(), tx, *txid)?;
      }

      rune_updater.update()?;
    }

    // 使用事务缓存添加区块头
    self.cache.add_block_header(self.height, block.header);

    // Light flush for critical column families after each block
    if self.height % 100 == 0 {
      // Flush every 100 blocks to balance performance and persistence
      let light_flush_opts = {
        let mut opts = FlushOptions::default();
        opts.set_wait(false); // Don't wait for flush to complete
        opts
      };

      let critical_cfs = [CF_HEIGHT_TO_BLOCK_HEADER, CF_STATISTIC_TO_COUNT];
      for cf_name in &critical_cfs {
        if let Some(cf) = self.index.database.cf_handle(cf_name) {
          if let Err(e) = self.index.database.flush_cf_opt(&cf, &light_flush_opts) {
            log::debug!("Light flush failed for {}: {}", cf_name, e);
          }
        }
      }
    }

    self.height += 1;
    self.outputs_traversed += outputs_in_block;

    log::info!(
      "Wrote {sat_ranges_written} sat ranges from {outputs_in_block} outputs in {} ms",
      (Instant::now() - start).as_millis(),
    );

    Ok(())
  }

  fn index_utxo_entries(
    &mut self,
    block: &BlockData,
    txout_receiver: &mut broadcast::Receiver<TxOut>,
    output_sender: &mut mpsc::Sender<OutPoint>,
    utxo_cache: &mut HashMap<OutPoint, UtxoEntryBuf>,
    sat_ranges_written: &mut u64,
    outputs_in_block: &mut u64,
  ) -> Result<(), Error> {
    let index_inscriptions =
      self.height >= self.index.first_inscription_height && self.index.index_inscriptions;

    // If the receiver still has inputs something went wrong in the last
    // block and we shouldn't recover from this and commit the last block
    if index_inscriptions {
      assert!(
        matches!(txout_receiver.try_recv(), Err(TryRecvError::Empty)),
        "Previous block did not consume all inputs"
      );
    }

    if !self.index.index_sats {
      // Send all missing input outpoints to be fetched
      let txids = block
        .txdata
        .iter()
        .map(|(_, txid)| txid)
        .collect::<HashSet<_>>();

      for (tx, _) in &block.txdata {
        for input in &tx.input {
          let prev_output = input.previous_output;
          // We don't need coinbase inputs
          if prev_output.is_null() {
            continue;
          }
          // We don't need inputs from txs earlier in the block, since
          // they'll be added to cache when the tx is indexed
          if txids.contains(&prev_output.txid) {
            continue;
          }
          // We don't need inputs we already have in our cache from earlier blocks
          if utxo_cache.contains_key(&prev_output) {
            continue;
          }
          // We don't need inputs we already have in our cache from earlier commit
          if self.cache.long_utxo_cache.contains_key(&prev_output) {
            continue;
          }
          // We don't need inputs we already have in our nondust database
          if self
            .cache
            .get_nondust_utxo_entry(&self.index.database, &prev_output)?
            .is_some()
          {
            continue;
          }
          // We don't need inputs we already have in our dust database
          if self
            .cache
            .get_utxo_entry(&self.index.database, &prev_output)?
            .is_some()
          {
            continue;
          }
          // Send this outpoint to background thread to be fetched
          output_sender.blocking_send(prev_output)?;
        }
      }
    }

    // 使用事务缓存获取统计信息（读穿透）
    let mut lost_sats = self
      .cache
      .get_statistic(&self.index.database, Statistic::LostSats)?;
    let cursed_inscription_count = self
      .cache
      .get_statistic(&self.index.database, Statistic::CursedInscriptions)?;
    let blessed_inscription_count = self
      .cache
      .get_statistic(&self.index.database, Statistic::BlessedInscriptions)?;
    let unbound_inscriptions = self
      .cache
      .get_statistic(&self.index.database, Statistic::UnboundInscriptions)?;

    // 使用缓存获取下一个序列号
    let next_sequence_number = self.cache.get_next_sequence_number();

    // // 使用缓存获取home inscription计数
    let home_inscription_count = 0;
    // let home_inscription_count = self
    //   .cache
    //   .get_home_inscription_count(&self.index.database)?;

    let mut inscription_updater = InscriptionUpdater {
      blessed_inscription_count,
      cursed_inscription_count,
      flotsam: Vec::new(),
      height: self.height,
      home_inscription_count,
      lost_sats,
      next_sequence_number,
      reward: Height(self.height).subsidy(),
      timestamp: block.header.time,
      transaction_buffer: Vec::new(),
      unbound_inscriptions,
      database: &self.index.database,
    };

    let mut coinbase_inputs = VecDeque::new();

    if self.index.index_sats {
      let h = Height(self.height);
      if h.subsidy() > 0 {
        let start = h.starting_sat();
        coinbase_inputs.push_front((start.n(), (start + h.subsidy()).n()));
        self.sat_ranges_since_flush += 1;
      }
    }

    for (tx_offset, (tx, txid)) in block
      .txdata
      .iter()
      .enumerate()
      .skip(1)
      .chain(block.txdata.iter().enumerate().take(1))
    {
      log::trace!("Indexing transaction {tx_offset}…");

      // 处理输入UTXO条目
      let input_utxo_entries = if tx_offset == 0 {
        Vec::new()
      } else {
        tx.input
          .iter()
          .map(|input| {
            let outpoint = input.previous_output.store();

            self.outputs_count += 1;
            let entry = if let Some(entry) = utxo_cache.remove(&OutPoint::load(outpoint.clone())) {
              self.outputs_cached += 1;
              self.outputs_cached0 += 1;
              entry
            } else if let Some(entry) = self
              .cache
              .long_utxo_cache
              .remove(&OutPoint::load(outpoint.clone()))
            {
              self.outputs_cached += 1;
              self.outputs_cached1 += 1;
              entry
            } else if let Some(entry) = self
              .cache
              .get_nondust_utxo_entry(&self.index.database, &OutPoint::load(outpoint.clone()))?
            {
              self.outputs_cached2 += 1;
              // 从数据库中获取UTXO，并标记为待删除
              self
                .cache
                .mark_nondust_utxo_for_deletion(OutPoint::load(outpoint.clone()));

              if self.index.index_addresses {
                // 处理地址索引删除
                let script_pubkey = entry.parse(self.index).script_pubkey();
                self.cache.mark_address_index_for_deletion(
                  script_pubkey.to_vec(),
                  OutPoint::load(outpoint.clone()),
                );
              }

              entry
            } else if let Some(entry) = self
              .cache
              .get_utxo_entry(&self.index.database, &OutPoint::load(outpoint.clone()))?
            {
              self.outputs_cached3 += 1;
              // 从数据库中获取UTXO，并标记为待删除
              self
                .cache
                .mark_utxo_for_deletion(OutPoint::load(outpoint.clone()));

              if self.index.index_addresses {
                // 处理地址索引删除
                let script_pubkey = entry.parse(self.index).script_pubkey();
                self.cache.mark_address_index_for_deletion(
                  script_pubkey.to_vec(),
                  OutPoint::load(outpoint.clone()),
                );
              }

              entry
            } else {
              assert!(!self.index.index_sats);
              let txout = txout_receiver.blocking_recv().map_err(|err| {
                anyhow!(
                  "failed to get transaction for {}: {err}",
                  input.previous_output
                )
              })?;

              let mut entry = UtxoEntryBuf::new();
              entry.push_value(txout.value, self.index);
              if self.index.index_addresses {
                entry.push_script_pubkey(txout.script_pubkey.as_bytes(), self.index);
              }

              entry
            };

            Ok(entry)
          })
          .collect::<Result<Vec<UtxoEntryBuf>>>()?
      };

      let input_utxo_entries = input_utxo_entries
        .iter()
        .map(|entry| entry.parse(self.index))
        .collect::<Vec<ParsedUtxoEntry>>();

      let mut output_utxo_entries = tx
        .output
        .iter()
        .map(|_| UtxoEntryBuf::new())
        .collect::<Vec<UtxoEntryBuf>>();

      let mut orig_input_sat_ranges = None;
      if self.index.index_sats {
        let mut input_sat_ranges;

        if tx_offset == 0 {
          // We use mem::take() because the borrow checker isn't smart enough
          // to realize that coinbase_inputs won't be used again.
          input_sat_ranges = mem::take(&mut coinbase_inputs);
        } else {
          input_sat_ranges = VecDeque::new();

          for input_utxo_entry in &input_utxo_entries {
            for chunk in input_utxo_entry.sat_ranges().chunks_exact(14) {
              input_sat_ranges.push_back(SatRange::load(chunk.try_into().unwrap()));
            }
          }
        }

        orig_input_sat_ranges = Some(input_sat_ranges.clone());

        self.index_transaction_sats(
          tx,
          *txid,
          &mut output_utxo_entries,
          &mut input_sat_ranges,
          sat_ranges_written,
          outputs_in_block,
        )?;

        if tx_offset == 0 {
          if !input_sat_ranges.is_empty() {
            // Note that the lost-sats outpoint is special, because (unlike real
            // outputs) it gets written to more than once.  commit() will merge
            // our new entry with any existing one.
            let utxo_entry = utxo_cache
              .entry(OutPoint::null())
              .or_insert(UtxoEntryBuf::empty(self.index));

            let mut lost_sat_ranges = Vec::new();
            for (start, end) in input_sat_ranges {
              if !Sat(start).common() {
                // 使用事务缓存机制处理lost sats的sat到satpoint映射
                self.cache.add_sat_satpoint_mapping(
                  start,
                  SatPoint {
                    outpoint: OutPoint::null(),
                    offset: lost_sats,
                  },
                );
              }

              lost_sat_ranges.extend_from_slice(&(start, end).store());
              lost_sats += end - start;
            }

            let mut new_utxo_entry = UtxoEntryBuf::new();
            new_utxo_entry.push_sat_ranges(&lost_sat_ranges, self.index);
            if self.index.index_addresses {
              new_utxo_entry.push_script_pubkey(&[], self.index);
            }

            *utxo_entry = UtxoEntryBuf::merged(utxo_entry, &new_utxo_entry, self.index);
          }
        } else {
          coinbase_inputs.extend(input_sat_ranges);
        }
      } else {
        for (vout, txout) in tx.output.iter().enumerate() {
          output_utxo_entries[vout].push_value(txout.value, self.index);
        }
      }

      if self.index.index_addresses {
        self.index_transaction_output_script_pubkeys(tx, &mut output_utxo_entries);
      }

      if index_inscriptions {
        inscription_updater.index_inscriptions(
          tx,
          *txid,
          &input_utxo_entries,
          &mut output_utxo_entries,
          utxo_cache,
          self.index,
          orig_input_sat_ranges.as_ref(),
          &mut self.cache,
        )?;
      }

      for (vout, output_utxo_entry) in output_utxo_entries.into_iter().enumerate() {
        let vout = u32::try_from(vout).unwrap();
        utxo_cache.insert(OutPoint { txid: *txid, vout }, output_utxo_entry);
      }
    }

    if index_inscriptions {
      // 使用事务缓存机制处理高度到最后一个序列号的映射
      self
        .cache
        .set_height_to_last_sequence_number(self.height, inscription_updater.next_sequence_number);
    }

    // 更新缓存中的统计信息
    self.cache.update_statistic(
      Statistic::LostSats,
      if self.index.index_sats {
        lost_sats
      } else {
        inscription_updater.lost_sats
      },
    );
    self.cache.update_statistic(
      Statistic::CursedInscriptions,
      inscription_updater.cursed_inscription_count,
    );
    self.cache.update_statistic(
      Statistic::BlessedInscriptions,
      inscription_updater.blessed_inscription_count,
    );
    self.cache.update_statistic(
      Statistic::UnboundInscriptions,
      inscription_updater.unbound_inscriptions,
    );

    Ok(())
  }

  fn index_transaction_output_script_pubkeys(
    &mut self,
    tx: &Transaction,
    output_utxo_entries: &mut [UtxoEntryBuf],
  ) {
    for (vout, txout) in tx.output.iter().enumerate() {
      output_utxo_entries[vout].push_script_pubkey(txout.script_pubkey.as_bytes(), self.index);
    }
  }

  fn index_transaction_sats(
    &mut self,
    tx: &Transaction,
    txid: Txid,
    output_utxo_entries: &mut [UtxoEntryBuf],
    input_sat_ranges: &mut VecDeque<(u64, u64)>,
    sat_ranges_written: &mut u64,
    outputs_traversed: &mut u64,
  ) -> Result {
    for (vout, output) in tx.output.iter().enumerate() {
      let outpoint = OutPoint {
        vout: vout.try_into().unwrap(),
        txid,
      };
      let mut sats = Vec::new();

      let mut remaining = output.value;
      while remaining > 0 {
        let range = input_sat_ranges
          .pop_front()
          .ok_or_else(|| anyhow!("insufficient inputs for transaction outputs"))?;

        if !Sat(range.0).common() {
          // 使用事务缓存机制处理sat到satpoint的映射
          self.cache.add_sat_satpoint_mapping(
            range.0,
            SatPoint {
              outpoint,
              offset: output.value - remaining,
            },
          );
        }

        let count = range.1 - range.0;

        let assigned = if count > remaining {
          self.sat_ranges_since_flush += 1;
          let middle = range.0 + remaining;
          input_sat_ranges.push_front((middle, range.1));
          (range.0, middle)
        } else {
          range
        };

        sats.extend_from_slice(&assigned.store());

        remaining -= assigned.1 - assigned.0;

        *sat_ranges_written += 1;
      }

      *outputs_traversed += 1;

      output_utxo_entries[vout].push_sat_ranges(&sats, self.index);
    }

    Ok(())
  }

  fn commit(&mut self, utxo_cache: HashMap<OutPoint, UtxoEntryBuf>, last_commit: bool) -> Result {
    log::info!(
      "Committing at block height {}, {} outputs traversed, {} in map, {} cached, {} cache0, {} cache1, {} cache2, {} cache3, {} rpc, {} all outputs",
      self.height,
      self.outputs_traversed,
      utxo_cache.len(),
      self.outputs_cached,
      self.outputs_cached0,
      self.outputs_cached1,
      self.outputs_cached2,
      self.outputs_cached3,
      self.outputs_count-self.outputs_cached0-self.outputs_cached1-self.outputs_cached2-self.outputs_cached3,
      self.outputs_count,
    );

    // 更新统计信息到缓存
    self
      .cache
      .update_statistic(Statistic::OutputsTraversed, self.outputs_traversed);
    self
      .cache
      .update_statistic(Statistic::SatRanges, self.sat_ranges_since_flush);
    self.cache.update_statistic(Statistic::Commits, 1);

    // 准备地址索引和铭文索引数据
    let mut address_index_data = Vec::new();
    let mut inscription_index_data = Vec::new();

    // 处理UTXO数据并收集索引数据
    for (outpoint, mut utxo_entry) in utxo_cache {
      if Index::is_special_outpoint(outpoint) {
        if let Some(old_entry) = self.cache.get_utxo_entry(&self.index.database, &outpoint)? {
          utxo_entry = old_entry;
        }
      }

      // 将UTXO数据添加到缓存
      self.cache.add_utxo_entry(outpoint, utxo_entry.clone());

      // 解析UTXO条目以获取索引数据
      let parsed_utxo_entry = utxo_entry.parse(self.index);

      // 收集地址索引数据
      if self.index.index_addresses {
        let script_pubkey = parsed_utxo_entry.script_pubkey();
        address_index_data.push((script_pubkey.to_vec(), outpoint));
      }

      // 收集铭文索引数据
      if self.index.index_inscriptions {
        for (sequence_number, offset) in parsed_utxo_entry.parse_inscriptions() {
          let satpoint = SatPoint { outpoint, offset };
          inscription_index_data.push((sequence_number, satpoint));
        }
      }
    }

    // 统一提交所有操作（包括UTXO数据、缓存数据和索引数据）
    self.cache.commit_with_extra_data(
      &self.index.database,
      &self.index.write_options,
      &address_index_data,
      &inscription_index_data,
      last_commit,
    )?;

    // 重置计数器
    self.outputs_traversed = 0;
    self.sat_ranges_since_flush = 0;
    self.outputs_cached = 0;
    self.outputs_cached0 = 0;
    self.outputs_cached1 = 0;
    self.outputs_cached2 = 0;
    self.outputs_cached3 = 0;
    self.outputs_dust_count = 0;
    self.outputs_count = 0;

    // For RocksDB, manually flush all column families to ensure data persistence
    // This simulates the commit behavior of redb transactions
    let flush_opts = {
      let mut opts = FlushOptions::default();
      opts.set_wait(true); // Wait for flush to complete
      opts
    };

    // Flush all relevant column families
    let cf_names = [
      CF_OUTPOINT_TO_UTXO_ENTRY,
      CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY,
      CF_SCRIPT_PUBKEY_TO_OUTPOINT,
      CF_SEQUENCE_NUMBER_TO_SATPOINT,
      CF_HEIGHT_TO_BLOCK_HEADER,
      CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER,
      CF_STATISTIC_TO_COUNT,
      CF_HEIGHT_TO_LAST_SEQUENCE_NUMBER,
    ];

    log::debug!(
      "Flushing {} column families to ensure data persistence",
      cf_names.len()
    );

    let mut flush_errors = Vec::new();
    for cf_name in &cf_names {
      if let Some(cf) = self.index.database.cf_handle(cf_name) {
        if let Err(e) = self.index.database.flush_cf_opt(&cf, &flush_opts) {
          let error_msg = format!("Failed to flush column family {}: {}", cf_name, e);
          log::warn!("{}", error_msg);
          flush_errors.push(error_msg);
        }
      } else {
        log::warn!("Column family {} not found", cf_name);
      }
    }

    if !flush_errors.is_empty() {
      log::warn!("Some column families failed to flush: {:?}", flush_errors);
    } else {
      log::debug!("All column families flushed successfully");
    }

    if let Err(e) = self.index.database.flush() {
      log::warn!("Failed to flush: {}", e);
    }
    log::debug!("Flushing db to ensure all data persistence",);

    // 当前数据高度为 self.height - 1
    Reorg::update_savepoints(self.index, self.height - 1)?;

    Ok(())
  }
}

use {
  self::{
    entry::{Entry, InscriptionEntry, SatRange},
    event::Event,
    lot::Lot,
    reorg::Reorg,
    updater::Updater,
    utxo_entry::{ParsedUtxoEntry, UtxoEntry, UtxoEntryBuf},
  },
  super::*,
  crate::{
    runes::MintError,
    subcommand::{find::FindRangeOutput, server::query},
    templates::StatusHtml,
    timestamp,
  },
  bitcoin::block::Header,
  bitcoincore_rpc::{
    json::{GetBlockHeaderResult, GetBlockStatsResult},
    Client,
  },
  chrono::SubsecRound,
  indicatif::{ProgressBar, ProgressStyle},
  log::log_enabled,
  ref_cast::RefCast,
  rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Direction, FlushOptions, IteratorMode,
    Options, WriteBatch, WriteOptions, DB,
  },
  std::{
    collections::{HashMap, HashSet},
    io::{BufWriter, Write},
    process::exit,
  },
};

pub use self::entry::RuneEntry;

pub(crate) mod entry;
pub mod event;
mod fetcher;
mod lot;
mod reorg;
mod rtx;
mod transaction_cache;
mod updater;
mod utxo_entry;

#[cfg(test)]
pub(crate) mod testing;

const SCHEMA_VERSION: u64 = 28;

// Column family names
const CF_SAT_TO_SEQUENCE_NUMBER: &str = "sat_to_sequence_number";
const CF_SEQUENCE_NUMBER_TO_CHILDREN: &str = "sequence_number_to_children";
const CF_SCRIPT_PUBKEY_TO_OUTPOINT: &str = "script_pubkey_to_outpoint";
const CF_HEIGHT_TO_BLOCK_HEADER: &str = "height_to_block_header";
const CF_HEIGHT_TO_LAST_SEQUENCE_NUMBER: &str = "height_to_last_sequence_number";
const CF_HOME_INSCRIPTIONS: &str = "home_inscriptions";
const CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER: &str = "inscription_id_to_sequence_number";
const CF_INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER: &str = "inscription_number_to_sequence_number";
const CF_OUTPOINT_TO_RUNE_BALANCES: &str = "outpoint_to_rune_balances";
const CF_OUTPOINT_TO_UTXO_ENTRY: &str = "outpoint_to_utxo_entry";
const CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY: &str = "outpoint_to_nondust_utxo_entry";
const CF_RUNE_ID_TO_RUNE_ENTRY: &str = "rune_id_to_rune_entry";
const CF_RUNE_TO_RUNE_ID: &str = "rune_to_rune_id";
const CF_SAT_TO_SATPOINT: &str = "sat_to_satpoint";
const CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY: &str = "sequence_number_to_inscription_entry";
const CF_SEQUENCE_NUMBER_TO_RUNE_ID: &str = "sequence_number_to_rune_id";
const CF_SEQUENCE_NUMBER_TO_SATPOINT: &str = "sequence_number_to_satpoint";
const CF_STATISTIC_TO_COUNT: &str = "statistic_to_count";
const CF_TRANSACTION_ID_TO_RUNE: &str = "transaction_id_to_rune";
const CF_TRANSACTION_ID_TO_TRANSACTION: &str = "transaction_id_to_transaction";
const CF_WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP: &str =
  "write_transaction_starting_block_count_to_timestamp";

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub(crate) enum Statistic {
  Schema = 0,
  BlessedInscriptions = 1,
  Commits = 2,
  CursedInscriptions = 3,
  IndexAddresses = 4,
  IndexInscriptions = 5,
  IndexRunes = 6,
  IndexSats = 7,
  IndexTransactions = 8,
  InitialSyncTime = 9,
  LostSats = 10,
  OutputsTraversed = 11,
  ReservedRunes = 12,
  Runes = 13,
  SatRanges = 14,
  LastSavepointHeight = 15,
  UnboundInscriptions = 16,
}

impl Statistic {
  fn key(self) -> u64 {
    self.into()
  }
}

impl From<Statistic> for u64 {
  fn from(statistic: Statistic) -> Self {
    statistic as u64
  }
}

#[derive(Serialize)]
pub struct Info {
  blocks_indexed: u32,
  branch_pages: u64,
  fragmented_bytes: u64,
  index_file_size: u64,
  index_path: PathBuf,
  leaf_pages: u64,
  metadata_bytes: u64,
  outputs_traversed: u64,
  page_size: usize,
  sat_ranges: u64,
  stored_bytes: u64,
  tables: BTreeMap<String, TableInfo>,
  total_bytes: u64,
  pub transactions: Vec<TransactionInfo>,
  tree_height: u32,
  utxos_indexed: u64,
}

#[derive(Serialize)]
pub(crate) struct TableInfo {
  branch_pages: u64,
  fragmented_bytes: u64,
  leaf_pages: u64,
  metadata_bytes: u64,
  proportion: f64,
  stored_bytes: u64,
  total_bytes: u64,
  tree_height: u32,
}

// TableStats implementation removed - not compatible with RocksDB
// impl From<TableStats> for TableInfo {
//   fn from(stats: TableStats) -> Self {
//     Self {
//       branch_pages: stats.branch_pages(),
//       fragmented_bytes: stats.fragmented_bytes(),
//       leaf_pages: stats.leaf_pages(),
//       metadata_bytes: stats.metadata_bytes(),
//       proportion: 0.0,
//       stored_bytes: stats.stored_bytes(),
//       total_bytes: stats.stored_bytes() + stats.metadata_bytes() + stats.fragmented_bytes(),
//       tree_height: stats.tree_height(),
//     }
//   }
// }

#[derive(Serialize)]
pub struct TransactionInfo {
  pub starting_block_count: u32,
  pub starting_timestamp: u128,
}

pub(crate) trait BitcoinCoreRpcResultExt<T> {
  fn into_option(self) -> Result<Option<T>>;
}

impl<T> BitcoinCoreRpcResultExt<T> for Result<T, bitcoincore_rpc::Error> {
  fn into_option(self) -> Result<Option<T>> {
    match self {
      Ok(ok) => Ok(Some(ok)),
      Err(bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(
        bitcoincore_rpc::jsonrpc::error::RpcError { code: -8, .. },
      ))) => Ok(None),
      Err(bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(
        bitcoincore_rpc::jsonrpc::error::RpcError { message, .. },
      )))
        if message.ends_with("not found") =>
      {
        Ok(None)
      }
      Err(err) => Err(err.into()),
    }
  }
}

pub struct Index {
  pub(crate) client: Client,
  database: DB,
  write_options: WriteOptions,
  event_sender: Option<tokio::sync::mpsc::Sender<Event>>,
  first_inscription_height: u32,
  genesis_block_coinbase_transaction: Transaction,
  genesis_block_coinbase_txid: Txid,
  height_limit: Option<u32>,
  index_addresses: bool,
  index_inscriptions: bool,
  index_runes: bool,
  index_sats: bool,
  index_transactions: bool,
  path: PathBuf,
  settings: Settings,
  started: DateTime<Utc>,
  unrecoverably_reorged: AtomicBool,
}

impl Index {
  pub fn open(settings: &Settings) -> Result<Self> {
    Index::open_with_event_sender(settings, None)
  }

  pub fn open_with_event_sender(
    settings: &Settings,
    event_sender: Option<tokio::sync::mpsc::Sender<Event>>,
  ) -> Result<Self> {
    let client = settings.bitcoin_rpc_client(None)?;

    let path = settings.index().to_owned();

    let data_dir = path.parent().unwrap();

    fs::create_dir_all(data_dir).snafu_context(error::Io { path: data_dir })?;

    let index_cache_size = settings.index_cache_size();

    log::info!("Setting index cache size to {} bytes", index_cache_size);

    let write_options = {
      let mut write_options = WriteOptions::default();
      if cfg!(test) {
        write_options.disable_wal(true);
      }
      write_options
    };

    // RocksDB doesn't use repair callbacks like redb
    // RocksDB has built-in recovery mechanisms

    let column_families = vec![
      CF_SAT_TO_SEQUENCE_NUMBER,
      CF_SEQUENCE_NUMBER_TO_CHILDREN,
      CF_SCRIPT_PUBKEY_TO_OUTPOINT,
      CF_HEIGHT_TO_BLOCK_HEADER,
      CF_HEIGHT_TO_LAST_SEQUENCE_NUMBER,
      CF_HOME_INSCRIPTIONS,
      CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER,
      CF_INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER,
      CF_OUTPOINT_TO_RUNE_BALANCES,
      CF_OUTPOINT_TO_UTXO_ENTRY,
      CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY,
      CF_RUNE_ID_TO_RUNE_ENTRY,
      CF_RUNE_TO_RUNE_ID,
      CF_SAT_TO_SATPOINT,
      CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY,
      CF_SEQUENCE_NUMBER_TO_RUNE_ID,
      CF_SEQUENCE_NUMBER_TO_SATPOINT,
      CF_STATISTIC_TO_COUNT,
      CF_TRANSACTION_ID_TO_RUNE,
      CF_TRANSACTION_ID_TO_TRANSACTION,
      CF_WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP,
    ];

    // Create options for opening existing database
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(false);

    opts.set_max_total_wal_size(1024 * 1024 * 1024);
    opts.set_max_open_files(256);
    opts.set_max_background_jobs(6);
    opts.set_bytes_per_sync(4 * 1024 * 1024); // 1MB
    opts.set_wal_bytes_per_sync(4 * 1024 * 1024); // 1MB

    let database = match DB::open_cf_descriptors(
      &opts,
      &path,
      column_families
        .iter()
        .map(|name| {
          let mut cf_opts = Options::default();

          cf_opts.set_write_buffer_size(256 * 1024 * 1024);
          cf_opts.set_max_write_buffer_number(32);
          cf_opts.set_compression_type(DBCompressionType::None);
          cf_opts.set_target_file_size_base(128 * 1024 * 1024);
          cf_opts.set_max_bytes_for_level_base(512 * 1024 * 1024);

          cf_opts.set_level_zero_file_num_compaction_trigger(4);
          cf_opts.set_level_zero_slowdown_writes_trigger(8);
          cf_opts.set_level_zero_stop_writes_trigger(12);

          ColumnFamilyDescriptor::new(*name, cf_opts)
        })
        .collect::<Vec<_>>(),
    ) {
      Ok(database) => {
        // Check schema version
        let schema_version = database
          .get_cf(
            database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
            &Statistic::Schema.key().to_be_bytes(),
          )?
          .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
          .unwrap_or(0);

        match schema_version.cmp(&SCHEMA_VERSION) {
          cmp::Ordering::Less =>
            bail!(
              "index at `{}` appears to have been built with an older, incompatible version of ord, consider deleting and rebuilding the index: index schema {schema_version}, ord schema {SCHEMA_VERSION}",
              path.display()
            ),
          cmp::Ordering::Greater =>
            bail!(
              "index at `{}` appears to have been built with a newer, incompatible version of ord, consider updating ord: index schema {schema_version}, ord schema {SCHEMA_VERSION}",
              path.display()
            ),
          cmp::Ordering::Equal => {
          }
        }

        database
      }
      Err(_) => {
        // Create new database with column families
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let database = DB::open_cf_descriptors(
          &opts,
          &path,
          column_families
            .iter()
            .map(|name| {
              let mut cf_opts = Options::default();

              cf_opts.set_write_buffer_size(256 * 1024 * 1024);
              cf_opts.set_max_write_buffer_number(32);
              cf_opts.set_compression_type(DBCompressionType::None);
              cf_opts.set_target_file_size_base(128 * 1024 * 1024);
              cf_opts.set_max_bytes_for_level_base(512 * 1024 * 1024);

              cf_opts.set_level_zero_file_num_compaction_trigger(4);
              cf_opts.set_level_zero_slowdown_writes_trigger(8);
              cf_opts.set_level_zero_stop_writes_trigger(12);

              ColumnFamilyDescriptor::new(*name, cf_opts)
            })
            .collect::<Vec<_>>(),
        )?;

        // Initialize statistics
        let mut batch = WriteBatch::default();

        batch.put_cf(
          database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
          &Statistic::IndexAddresses.key().to_be_bytes(),
          &u64::from(settings.index_addresses_raw()).to_be_bytes(),
        );

        batch.put_cf(
          database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
          &Statistic::IndexInscriptions.key().to_be_bytes(),
          &u64::from(settings.index_inscriptions_raw()).to_be_bytes(),
        );

        batch.put_cf(
          database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
          &Statistic::IndexRunes.key().to_be_bytes(),
          &u64::from(settings.index_runes_raw()).to_be_bytes(),
        );

        batch.put_cf(
          database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
          &Statistic::IndexSats.key().to_be_bytes(),
          &u64::from(settings.index_sats_raw()).to_be_bytes(),
        );

        batch.put_cf(
          database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
          &Statistic::IndexTransactions.key().to_be_bytes(),
          &u64::from(settings.index_transactions_raw()).to_be_bytes(),
        );

        batch.put_cf(
          database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
          &Statistic::Schema.key().to_be_bytes(),
          &SCHEMA_VERSION.to_be_bytes(),
        );

        if settings.index_runes_raw() && settings.chain() == Chain::Mainnet
          || settings.chain() == Chain::FractalMainnet
          || settings.chain() == Chain::FractalTestnet
        {
          let rune = Rune(2055900680524219742);
          let id = RuneId { block: 1, tx: 0 };
          let etching = Txid::all_zeros();

          batch.put_cf(
            database.cf_handle(CF_RUNE_TO_RUNE_ID).unwrap(),
            &rune.store(),
            &id.store(),
          );

          batch.put_cf(
            database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap(),
            &Statistic::Runes.key().to_be_bytes(),
            &1u64.to_be_bytes(),
          );

          batch.put_cf(
            database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap(),
            &id.store(),
            &RuneEntry {
              block: id.block,
              burned: 0,
              divisibility: 0,
              etching,
              terms: Some(Terms {
                amount: Some(1),
                cap: Some(u128::MAX),
                height: (
                  Some((Rune::FRACTAL_START_INTERVAL * 4).into()),
                  Some(
                    (Rune::FRACTAL_START_INTERVAL * 4 + Rune::FRACTAL_SUBSIDY_HALVING_INTERVAL)
                      .into(),
                  ),
                ),
                offset: (None, None),
              }),
              mints: 0,
              number: 0,
              premine: 0,
              spaced_rune: SpacedRune { rune, spacers: 128 },
              symbol: Some('\u{29C9}'),
              timestamp: 0,
              turbo: true,
            }
            .store(),
          );

          batch.put_cf(
            database.cf_handle(CF_TRANSACTION_ID_TO_RUNE).unwrap(),
            &etching.store(),
            &rune.store(),
          );
        }

        database.write_opt(batch, &write_options)?;
        database
      }
    };

    let index_addresses;
    let index_runes;
    let index_sats;
    let index_transactions;
    let index_inscriptions;

    {
      let statistics_cf = database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap();
      index_addresses =
        Self::is_statistic_set_rocksdb(&database, statistics_cf, Statistic::IndexAddresses)?;
      index_inscriptions =
        Self::is_statistic_set_rocksdb(&database, statistics_cf, Statistic::IndexInscriptions)?;
      index_runes =
        Self::is_statistic_set_rocksdb(&database, statistics_cf, Statistic::IndexRunes)?;
      index_sats = Self::is_statistic_set_rocksdb(&database, statistics_cf, Statistic::IndexSats)?;
      index_transactions =
        Self::is_statistic_set_rocksdb(&database, statistics_cf, Statistic::IndexTransactions)?;
    }

    let genesis_block_coinbase_transaction =
      settings.chain().genesis_block().coinbase().unwrap().clone();

    Ok(Self {
      genesis_block_coinbase_txid: genesis_block_coinbase_transaction.txid(),
      client,
      database,
      write_options,
      event_sender,
      first_inscription_height: settings.first_inscription_height(),
      genesis_block_coinbase_transaction,
      height_limit: settings.height_limit(),
      index_addresses,
      index_runes,
      index_sats,
      index_transactions,
      index_inscriptions,
      settings: settings.clone(),
      path,
      started: Utc::now(),
      unrecoverably_reorged: AtomicBool::new(false),
    })
  }

  /// Unlike normal outpoints, which are added to index on creation and removed
  /// when spent, the UTXO entry for special outpoints may be updated.
  ///
  /// The special outpoints are the null outpoint, which receives lost sats,
  /// and the unbound outpoint, which receives unbound inscriptions.
  pub fn is_special_outpoint(outpoint: OutPoint) -> bool {
    outpoint == OutPoint::null() || outpoint == unbound_outpoint()
  }

  pub fn contains_output(&self, output: &OutPoint) -> Result<bool> {
    Ok(
      self
        .database
        .get_cf(
          self
            .database
            .cf_handle(CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY)
            .unwrap(),
          &output.store(),
        )?
        .is_some()
        || self
          .database
          .get_cf(
            self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap(),
            &output.store(),
          )?
          .is_some(),
    )
  }

  pub fn has_address_index(&self) -> bool {
    self.index_addresses
  }

  pub fn has_rune_index(&self) -> bool {
    self.index_runes
  }

  pub fn has_sat_index(&self) -> bool {
    self.index_sats
  }

  pub fn status(&self) -> Result<StatusHtml> {
    let statistic_cf = self.database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap();
    let height_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();

    let statistic = |statistic: Statistic| -> Result<u64> {
      Ok(
        self
          .database
          .get_cf(statistic_cf, &statistic.key().to_be_bytes())?
          .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
          .unwrap_or_default(),
      )
    };

    let height = {
      let mut iter = self.database.iterator_cf(height_cf, IteratorMode::End);
      if let Some(Ok((height_bytes, _))) = iter.next() {
        Some(u32::from_be_bytes(
          height_bytes.as_ref().try_into().unwrap(),
        ))
      } else {
        None
      }
    };

    let next_height = height.map(|height| height + 1).unwrap_or(0);

    let blessed_inscriptions = statistic(Statistic::BlessedInscriptions)?;
    let cursed_inscriptions = statistic(Statistic::CursedInscriptions)?;
    let initial_sync_time = statistic(Statistic::InitialSyncTime)?;

    Ok(StatusHtml {
      address_index: self.has_address_index(),
      blessed_inscriptions,
      chain: self.settings.chain(),
      cursed_inscriptions,
      height,
      initial_sync_time: Duration::from_micros(initial_sync_time),
      inscriptions: blessed_inscriptions + cursed_inscriptions,
      lost_sats: statistic(Statistic::LostSats)?,
      minimum_rune_for_next_block: Rune::minimum_at_height(
        self.settings.chain().network(),
        Height(next_height),
      ),
      rune_index: self.has_rune_index(),
      runes: statistic(Statistic::Runes)?,
      sat_index: self.has_sat_index(),
      started: self.started,
      transaction_index: statistic(Statistic::IndexTransactions)? != 0,
      unrecoverably_reorged: self.unrecoverably_reorged.load(atomic::Ordering::Relaxed),
      uptime: (Utc::now() - self.started).to_std()?,
    })
  }

  pub fn info(&self) -> Result<Info> {
    // For RocksDB, we'll create a simplified info structure
    let mut tables: BTreeMap<String, TableInfo> = BTreeMap::new();

    // Add basic table info for RocksDB
    let column_families = vec![
      CF_SAT_TO_SEQUENCE_NUMBER,
      CF_SEQUENCE_NUMBER_TO_CHILDREN,
      CF_SCRIPT_PUBKEY_TO_OUTPOINT,
      CF_HEIGHT_TO_BLOCK_HEADER,
      CF_HEIGHT_TO_LAST_SEQUENCE_NUMBER,
      CF_HOME_INSCRIPTIONS,
      CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER,
      CF_INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER,
      CF_OUTPOINT_TO_RUNE_BALANCES,
      CF_OUTPOINT_TO_UTXO_ENTRY,
      CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY,
      CF_RUNE_ID_TO_RUNE_ENTRY,
      CF_RUNE_TO_RUNE_ID,
      CF_SAT_TO_SATPOINT,
      CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY,
      CF_SEQUENCE_NUMBER_TO_RUNE_ID,
      CF_SEQUENCE_NUMBER_TO_SATPOINT,
      CF_STATISTIC_TO_COUNT,
      CF_TRANSACTION_ID_TO_RUNE,
      CF_TRANSACTION_ID_TO_TRANSACTION,
      CF_WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP,
    ];

    for cf_name in column_families {
      let cf = self.database.cf_handle(cf_name).unwrap();
      let mut count = 0;
      let iter = self.database.iterator_cf(cf, IteratorMode::Start);
      for _ in iter {
        count += 1;
      }

      tables.insert(
        cf_name.to_string(),
        TableInfo {
          branch_pages: 0,
          fragmented_bytes: 0,
          leaf_pages: count,
          metadata_bytes: 0,
          proportion: 0.0,
          stored_bytes: count as u64 * 100, // Estimate
          total_bytes: count as u64 * 100,
          tree_height: 1,
        },
      );
    }

    let total_bytes = tables
      .values()
      .map(|table_info| table_info.total_bytes)
      .sum();

    tables.values_mut().for_each(|table_info| {
      table_info.proportion = table_info.total_bytes as f64 / total_bytes as f64
    });

    let info = {
      let statistic_cf = self.database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap();
      let sat_ranges = self
        .database
        .get_cf(statistic_cf, &Statistic::SatRanges.key().to_be_bytes())?
        .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
        .unwrap_or(0);
      let outputs_traversed = self
        .database
        .get_cf(
          statistic_cf,
          &Statistic::OutputsTraversed.key().to_be_bytes(),
        )?
        .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
        .unwrap_or(0);
      Info {
        index_path: self.path.clone(),
        blocks_indexed: {
          let height_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
          let mut iter = self.database.iterator_cf(height_cf, IteratorMode::End);
          if let Some(Ok((height_bytes, _))) = iter.next() {
            u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap()) + 1
          } else {
            0
          }
        },
        branch_pages: 0,
        fragmented_bytes: 0,
        index_file_size: fs::metadata(&self.path)?.len(),
        leaf_pages: 0,
        metadata_bytes: 0,
        sat_ranges,
        outputs_traversed,
        page_size: 4096,
        stored_bytes: 0,
        total_bytes,
        tables,
        transactions: {
          let cf = self
            .database
            .cf_handle(CF_WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)
            .unwrap();
          let mut transactions = Vec::new();
          let iter = self.database.iterator_cf(cf, IteratorMode::Start);
          for result in iter {
            let (block_count_bytes, timestamp_bytes) = result?;
            let starting_block_count =
              u32::from_be_bytes(block_count_bytes.as_ref().try_into().unwrap());
            let starting_timestamp =
              u128::from_be_bytes(timestamp_bytes.as_ref().try_into().unwrap());
            transactions.push(TransactionInfo {
              starting_block_count,
              starting_timestamp,
            });
          }
          transactions
        },
        tree_height: 1,
        utxos_indexed: {
          let cf = self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
          let mut count = 0;
          let iter = self.database.iterator_cf(cf, IteratorMode::Start);
          for _ in iter {
            count += 1;
          }

          let cf = self
            .database
            .cf_handle(CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY)
            .unwrap();
          let iter = self.database.iterator_cf(cf, IteratorMode::Start);
          for _ in iter {
            count += 1;
          }

          count
        },
      }
    };

    Ok(info)
  }

  pub fn update(&self) -> Result {
    loop {
      // For RocksDB, we don't need explicit transactions like redb
      // The Updater will handle database operations directly

      // Acquire cache lock and initialize
      let transaction_cache = std::sync::Mutex::new(transaction_cache::TransactionCache::new());
      let mut cache = transaction_cache.lock().unwrap();
      cache.initialize(&self.database)?;

      let mut updater = Updater {
        height: {
          let height_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
          let mut iter = self.database.iterator_cf(height_cf, IteratorMode::End);
          if let Some(Ok((height_bytes, _))) = iter.next() {
            u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap()) + 1
          } else {
            0
          }
        },
        index: self,
        outputs_cached: 0,
        outputs_cached0: 0,
        outputs_cached1: 0,
        outputs_cached2: 0,
        outputs_cached3: 0,
        outputs_dust_count: 0,
        outputs_count: 0,
        outputs_traversed: 0,
        sat_ranges_since_flush: 0,
        cache: &mut cache,
      };

      match updater.update_index() {
        Ok(ok) => return Ok(ok),
        Err(err) => {
          log::info!("{}", err.to_string());

          match err.downcast_ref() {
            Some(&reorg::Error::Recoverable { height, depth }) => {
              Reorg::handle_reorg(self, height, depth)?;
              exit(0);
            }
            Some(&reorg::Error::Unrecoverable) => {
              self
                .unrecoverably_reorged
                .store(true, atomic::Ordering::Relaxed);
              return Err(anyhow!(reorg::Error::Unrecoverable));
            }
            _ => return Err(err),
          };
        }
      }
    }
  }

  pub fn export(&self, filename: &String, include_addresses: bool) -> Result {
    let mut writer = BufWriter::new(fs::File::create(filename)?);

    let blocks_indexed = {
      let height_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
      let mut iter = self.database.iterator_cf(height_cf, IteratorMode::End);
      if let Some(Ok((height_bytes, _))) = iter.next() {
        u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap()) + 1
      } else {
        0
      }
    };

    writeln!(writer, "# export at block height {}", blocks_indexed)?;

    log::info!("exporting database tables to {filename}");

    let sequence_number_to_satpoint_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_SATPOINT)
      .unwrap();
    // fixme: dust utxo
    let outpoint_to_utxo_entry_cf = self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
    let inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();

    for result in self
      .database
      .iterator_cf(inscription_entry_cf, IteratorMode::Start)
    {
      let (sequence_number_bytes, entry_bytes) = result?;
      let sequence_number = u64::from_be_bytes(sequence_number_bytes.as_ref().try_into().unwrap());
      let entry = InscriptionEntry::load(entry_bytes.to_vec());
      let satpoint = SatPoint::load(
        self
          .database
          .get_cf(
            sequence_number_to_satpoint_cf,
            &sequence_number.to_be_bytes(),
          )?
          .unwrap(),
      );

      write!(
        writer,
        "{}\t{}\t{}",
        entry.inscription_number, entry.id, satpoint
      )?;

      if include_addresses {
        let address = if satpoint.outpoint == unbound_outpoint() {
          "unbound".to_string()
        } else {
          let script_pubkey = if self.index_addresses {
            ScriptBuf::from_bytes(
              UtxoEntry::ref_cast(
                &self
                  .database
                  .get_cf(outpoint_to_utxo_entry_cf, &satpoint.outpoint.store())?
                  .unwrap(),
              )
              .parse(self)
              .script_pubkey()
              .to_vec(),
            )
          } else {
            self
              .get_transaction(satpoint.outpoint.txid)?
              .unwrap()
              .output
              .into_iter()
              .nth(satpoint.outpoint.vout.try_into().unwrap())
              .unwrap()
              .script_pubkey
          };

          self
            .settings
            .chain()
            .address_from_script(&script_pubkey)
            .map(|address| address.to_string())
            .unwrap_or_else(|e| e.to_string())
        };
        write!(writer, "\t{}", address)?;
      }
      writeln!(writer)?;

      if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
        break;
      }
    }
    writer.flush()?;
    Ok(())
  }

  pub(crate) fn is_statistic_set_rocksdb(
    database: &DB,
    statistics_cf: &ColumnFamily,
    statistic: Statistic,
  ) -> Result<bool> {
    Ok(
      database
        .get_cf(statistics_cf, &statistic.key().to_be_bytes())?
        .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
        .unwrap_or_default()
        != 0,
    )
  }

  #[cfg(test)]
  pub(crate) fn statistic(&self, statistic: Statistic) -> u64 {
    let statistic_to_count_cf = self.database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap();
    let value = self
      .database
      .get_cf(statistic_to_count_cf, &statistic.key().to_be_bytes())
      .map(|bytes| u64::from_be_bytes(bytes.unwrap().try_into().unwrap()))
      .unwrap_or(0);

    value
  }

  #[cfg(test)]
  pub(crate) fn inscription_number(&self, inscription_id: InscriptionId) -> i64 {
    self
      .get_inscription_entry(inscription_id)
      .unwrap()
      .unwrap()
      .inscription_number
  }

  pub fn block_count(&self) -> Result<u32> {
    let height_to_block_header_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
    let mut iter = self
      .database
      .iterator_cf(height_to_block_header_cf, IteratorMode::End);
    let Some(Ok((height_bytes, _))) = iter.next() else {
      return Ok(0);
    };
    Ok(u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap()) + 1)
  }

  pub fn block_height(&self) -> Result<Option<Height>> {
    let height_to_block_header_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
    let mut iter = self
      .database
      .iterator_cf(height_to_block_header_cf, IteratorMode::End);
    let Some(Ok((height_bytes, _))) = iter.next() else {
      return Ok(None);
    };
    Ok(Some(Height(u32::from_be_bytes(
      height_bytes.as_ref().try_into().unwrap(),
    ))))
  }

  pub fn block_hash(&self, height: Option<u32>) -> Result<Option<BlockHash>> {
    if let Some(height) = height {
      let height_to_block_header_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
      if let Some(header_bytes) = self
        .database
        .get_cf(height_to_block_header_cf, &height.to_be_bytes())?
      {
        Ok(Some(Header::load(header_bytes).block_hash()))
      } else {
        Ok(None)
      }
    } else {
      // Get block hash at highest height
      if let Some(height) = self.block_height()? {
        let height_to_block_header_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
        if let Some(header) = self
          .database
          .get_cf(height_to_block_header_cf, &height.n().to_be_bytes())?
        {
          Ok(Some(Header::load(header).block_hash()))
        } else {
          Ok(None)
        }
      } else {
        Ok(None)
      }
    }
  }

  pub fn blocks(&self, take: usize) -> Result<Vec<(u32, BlockHash)>> {
    let cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
    let mut blocks = Vec::new();
    let mut iter = self.database.iterator_cf(cf, IteratorMode::End);

    for _ in 0..take {
      if let Some(Ok((height_bytes, header_bytes))) = iter.next() {
        let height = u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap());
        let header = Header::load(header_bytes.to_vec());
        blocks.push((height, header.block_hash()));
      } else {
        break;
      }
    }

    Ok(blocks)
  }

  pub fn rare_sat_satpoints(&self) -> Result<Vec<(Sat, SatPoint)>> {
    let cf = self.database.cf_handle(CF_SAT_TO_SATPOINT).unwrap();
    let mut result = Vec::new();
    let iter = self.database.iterator_cf(cf, IteratorMode::Start);

    for item in iter {
      let (sat_bytes, satpoint_bytes) = item?;
      let sat = u64::from_be_bytes(sat_bytes.as_ref().try_into().unwrap());
      let satpoint = SatPoint::load(satpoint_bytes.as_ref().try_into().unwrap());
      result.push((Sat(sat), satpoint));
    }

    Ok(result)
  }

  pub fn rare_sat_satpoint(&self, sat: Sat) -> Result<Option<SatPoint>> {
    let sat_to_satpoint_cf = self.database.cf_handle(CF_SAT_TO_SATPOINT).unwrap();
    if let Some(satpoint_bytes) = self
      .database
      .get_cf(sat_to_satpoint_cf, &sat.n().to_be_bytes())?
    {
      Ok(Some(SatPoint::load(satpoint_bytes)))
    } else {
      Ok(None)
    }
  }

  pub fn get_rune_by_id(&self, id: RuneId) -> Result<Option<Rune>> {
    let rune_id_to_rune_entry_cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    if let Some(entry_bytes) = self
      .database
      .get_cf(rune_id_to_rune_entry_cf, &id.store())?
    {
      let entry = RuneEntry::load(entry_bytes);
      Ok(Some(entry.spaced_rune.rune))
    } else {
      Ok(None)
    }
  }

  pub fn get_rune_by_number(&self, number: usize) -> Result<Option<Rune>> {
    let cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    let mut iter = self.database.iterator_cf(cf, IteratorMode::Start);

    for _ in 0..number {
      if iter.next().is_none() {
        return Ok(None);
      }
    }

    if let Some(Ok((_id, entry_bytes))) = iter.next() {
      let rune = RuneEntry::load_from_bytes(&entry_bytes)
        .unwrap()
        .spaced_rune
        .rune;
      Ok(Some(rune))
    } else {
      Ok(None)
    }
  }

  pub fn rune(&self, rune: Rune) -> Result<Option<(RuneId, RuneEntry, Option<InscriptionId>)>> {
    let rune_to_id_cf = self.database.cf_handle(CF_RUNE_TO_RUNE_ID).unwrap();
    let rune_id = if let Some(id_bytes) = self.database.get_cf(rune_to_id_cf, &rune.store())? {
      RuneId::load(id_bytes)
    } else {
      return Ok(None);
    };

    let rune_id_to_rune_entry_cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    let entry = if let Some(entry_bytes) = self
      .database
      .get_cf(rune_id_to_rune_entry_cf, &rune_id.store())?
    {
      RuneEntry::load(entry_bytes)
    } else {
      return Ok(None);
    };

    let parent = InscriptionId {
      txid: entry.etching,
      index: 0,
    };

    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let parent = if self
      .database
      .get_cf(inscription_id_to_sequence_number_cf, &parent.store())?
      .is_some()
    {
      Some(parent)
    } else {
      None
    };

    Ok(Some((rune_id, entry, parent)))
  }

  pub fn runes(&self) -> Result<Vec<(RuneId, RuneEntry)>> {
    let mut entries = Vec::new();
    let cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();

    for result in self.database.iterator_cf(cf, IteratorMode::Start) {
      let (id_bytes, entry_bytes) = result?;
      entries.push((
        RuneId::load(id_bytes.to_vec()),
        RuneEntry::load_from_bytes(&entry_bytes).unwrap(),
      ));
    }

    Ok(entries)
  }

  pub fn runes_paginated(
    &self,
    page_size: usize,
    page_index: usize,
  ) -> Result<(Vec<(RuneId, RuneEntry)>, bool)> {
    let mut entries = Vec::new();
    let cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    let mut iter = self.database.iterator_cf(cf, IteratorMode::End);

    // Skip to the start position
    for _ in 0..page_index.saturating_mul(page_size) {
      if iter.next().is_none() {
        break;
      }
    }

    // Take the requested items
    for _ in 0..page_size.saturating_add(1) {
      if let Some(Ok((id_bytes, entry_bytes))) = iter.next() {
        entries.push((
          RuneId::load(id_bytes.to_vec()),
          RuneEntry::load_from_bytes(&entry_bytes).unwrap(),
        ));
      } else {
        break;
      }
    }

    let more = entries.len() > page_size;

    Ok((entries, more))
  }

  pub fn encode_rune_balance(id: RuneId, balance: u128, buffer: &mut Vec<u8>) {
    varint::encode_to_vec(id.block.into(), buffer);
    varint::encode_to_vec(id.tx.into(), buffer);
    varint::encode_to_vec(balance, buffer);
  }

  pub fn decode_rune_balance(buffer: &[u8]) -> Result<((RuneId, u128), usize)> {
    let mut len = 0;
    let (block, block_len) = varint::decode(&buffer[len..])?;
    len += block_len;
    let (tx, tx_len) = varint::decode(&buffer[len..])?;
    len += tx_len;
    let id = RuneId {
      block: block.try_into()?,
      tx: tx.try_into()?,
    };
    let (balance, balance_len) = varint::decode(&buffer[len..])?;
    len += balance_len;
    Ok(((id, balance), len))
  }

  pub fn get_rune_balances_for_output(
    &self,
    outpoint: OutPoint,
  ) -> Result<BTreeMap<SpacedRune, Pile>> {
    let outpoint_to_balances_cf = self
      .database
      .cf_handle(CF_OUTPOINT_TO_RUNE_BALANCES)
      .unwrap();

    let Some(balances_buffer) = self
      .database
      .get_cf(outpoint_to_balances_cf, &outpoint.store())?
    else {
      return Ok(BTreeMap::new());
    };

    let mut balances = BTreeMap::new();
    let mut i = 0;
    while i < balances_buffer.len() {
      let ((id, amount), length) = Index::decode_rune_balance(&balances_buffer[i..]).unwrap();
      i += length;

      let rune_id_to_rune_entry_cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
      let entry_bytes = self
        .database
        .get_cf(rune_id_to_rune_entry_cf, &id.store())?
        .unwrap();
      let entry = RuneEntry::load(entry_bytes);

      balances.insert(
        entry.spaced_rune,
        Pile {
          amount,
          divisibility: entry.divisibility,
          symbol: entry.symbol,
        },
      );
    }

    Ok(balances)
  }

  pub fn get_rune_balance_map(&self) -> Result<BTreeMap<SpacedRune, BTreeMap<OutPoint, Pile>>> {
    let outpoint_balances = self.get_rune_balances()?;

    let rune_id_to_rune_entry_cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();

    let mut rune_balances_by_id: BTreeMap<RuneId, BTreeMap<OutPoint, u128>> = BTreeMap::new();

    for (outpoint, balances) in outpoint_balances {
      for (rune_id, amount) in balances {
        *rune_balances_by_id
          .entry(rune_id)
          .or_default()
          .entry(outpoint)
          .or_default() += amount;
      }
    }

    let mut rune_balances = BTreeMap::new();

    for (rune_id, balances) in rune_balances_by_id {
      let RuneEntry {
        divisibility,
        spaced_rune,
        symbol,
        ..
      } = RuneEntry::load(
        self
          .database
          .get_cf(rune_id_to_rune_entry_cf, &rune_id.store())?
          .unwrap(),
      );

      rune_balances.insert(
        spaced_rune,
        balances
          .into_iter()
          .map(|(outpoint, amount)| {
            (
              outpoint,
              Pile {
                amount,
                divisibility,
                symbol,
              },
            )
          })
          .collect(),
      );
    }

    Ok(rune_balances)
  }

  pub fn get_rune_balances(&self) -> Result<Vec<(OutPoint, Vec<(RuneId, u128)>)>> {
    let mut result = Vec::new();
    let cf = self
      .database
      .cf_handle(CF_OUTPOINT_TO_RUNE_BALANCES)
      .unwrap();

    for entry in self.database.iterator_cf(cf, IteratorMode::Start) {
      let (outpoint_bytes, balances_buffer) = entry?;
      let outpoint = OutPoint::load(outpoint_bytes.to_vec());

      let mut balances = Vec::new();
      let mut i = 0;
      while i < balances_buffer.len() {
        let ((id, balance), length) = Index::decode_rune_balance(&balances_buffer[i..]).unwrap();
        i += length;
        balances.push((id, balance));
      }

      result.push((outpoint, balances));
    }

    Ok(result)
  }

  pub fn block_header(&self, hash: BlockHash) -> Result<Option<Header>> {
    self.client.get_block_header(&hash).into_option()
  }

  pub fn block_header_info(&self, hash: BlockHash) -> Result<Option<GetBlockHeaderResult>> {
    self.client.get_block_header_info(&hash).into_option()
  }

  pub fn block_stats(&self, height: u64) -> Result<Option<GetBlockStatsResult>> {
    self.client.get_block_stats(height).into_option()
  }

  pub fn get_block_by_height(&self, height: u32) -> Result<Option<Block>> {
    Ok(
      self
        .client
        .get_block_hash(height.into())
        .into_option()?
        .map(|hash| self.client.get_block(&hash))
        .transpose()?,
    )
  }

  pub fn get_block_by_hash(&self, hash: BlockHash) -> Result<Option<Block>> {
    self.client.get_block(&hash).into_option()
  }

  pub fn get_collections_paginated(
    &self,
    page_size: usize,
    page_index: usize,
  ) -> Result<(Vec<InscriptionId>, bool)> {
    let sequence_number_to_children_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_CHILDREN)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_children' not found"))?;

    let mut collections = Vec::new();
    let mut seen_parents = HashSet::new();
    let mut iter = self
      .database
      .iterator_cf(sequence_number_to_children_cf, IteratorMode::Start);

    while let Some(Ok((key_bytes, _))) = iter.next() {
      if key_bytes.len() >= 8 {
        let parent = u64::from_be_bytes(
          key_bytes[0..8]
            .try_into()
            .map_err(|_| anyhow!("Invalid key format"))?,
        );
        seen_parents.insert(parent);
      }
    }

    let mut unique_parents: Vec<u64> = seen_parents.into_iter().collect();
    unique_parents.sort();

    let start_idx = page_index.saturating_mul(page_size);
    let end_idx = (start_idx + page_size + 1).min(unique_parents.len());

    for &parent in &unique_parents[start_idx..end_idx] {
      let sequence_number_to_inscription_entry_cf = self
        .database
        .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
        .unwrap();
      if let Some(entry_bytes) = self.database.get_cf(
        sequence_number_to_inscription_entry_cf,
        &parent.to_be_bytes(),
      )? {
        let entry = InscriptionEntry::load(entry_bytes);
        collections.push(entry.id);
      }
    }

    let more = collections.len() > page_size;

    if more {
      collections.pop();
    }

    Ok((collections, more))
  }

  #[cfg(test)]
  pub(crate) fn get_children_by_inscription_id(
    &self,
    inscription_id: InscriptionId,
  ) -> Result<Vec<InscriptionId>> {
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let Some(sequence_number_bytes) = self.database.get_cf(
      inscription_id_to_sequence_number_cf,
      &inscription_id.store(),
    )?
    else {
      return Ok(Vec::new());
    };
    let sequence_number = u64::from_be_bytes(sequence_number_bytes.try_into().unwrap());

    self
      .get_children_by_sequence_number_paginated(sequence_number, usize::MAX, 0)
      .map(|(children, _more)| children)
  }

  #[cfg(test)]
  pub(crate) fn get_parents_by_inscription_id(
    &self,
    inscription_id: InscriptionId,
  ) -> Vec<InscriptionId> {
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'inscription_id_to_sequence_number' not found"))
      .unwrap();
    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_inscription_entry' not found"))
      .unwrap();

    let sequence_number_bytes = self
      .database
      .get_cf(
        inscription_id_to_sequence_number_cf,
        &inscription_id.store(),
      )
      .unwrap()
      .unwrap();
    let sequence_number = u64::from_be_bytes(sequence_number_bytes.try_into().unwrap());

    let entry_bytes = self
      .database
      .get_cf(
        sequence_number_to_inscription_entry_cf,
        &sequence_number.to_be_bytes(),
      )
      .unwrap()
      .unwrap();
    let parent_sequences = InscriptionEntry::load(entry_bytes).parents;

    parent_sequences
      .into_iter()
      .map(|parent_sequence_number| {
        let parent_entry_bytes = self
          .database
          .get_cf(
            sequence_number_to_inscription_entry_cf,
            &parent_sequence_number.to_be_bytes(),
          )
          .unwrap()
          .unwrap();
        InscriptionEntry::load(parent_entry_bytes).id
      })
      .collect()
  }

  pub fn get_children_by_sequence_number_paginated(
    &self,
    sequence_number: u64,
    page_size: usize,
    page_index: usize,
  ) -> Result<(Vec<InscriptionId>, bool)> {
    let sequence_number_to_children_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_CHILDREN)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_children' not found"))?;

    let mut children = Vec::new();

    let prefix = sequence_number.to_be_bytes();
    let mut iter = self.database.iterator_cf(
      sequence_number_to_children_cf,
      IteratorMode::From(&prefix, Direction::Forward),
    );
    let mut found_children = 0;
    let mut skipped = 0;

    while let Some(Ok((key_bytes, _))) = iter.next() {
      if key_bytes.len() < 8 || key_bytes[0..8] != prefix {
        break;
      }

      if key_bytes.len() < 16 {
        continue;
      }
      let child_sequence_number = u64::from_be_bytes(
        key_bytes[8..16]
          .try_into()
          .map_err(|_| anyhow!("Invalid key format"))?,
      );

      if skipped >= page_index * page_size {
        if found_children < page_size.saturating_add(1) {
          let sequence_number_to_inscription_entry_cf = self
            .database
            .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
            .unwrap();
          if let Some(entry_bytes) = self.database.get_cf(
            sequence_number_to_inscription_entry_cf,
            &child_sequence_number.to_be_bytes(),
          )? {
            let entry = InscriptionEntry::load(entry_bytes);
            children.push(entry.id);
            found_children += 1;
          }
        } else {
          break;
        }
      } else {
        skipped += 1;
      }
    }

    let more = children.len() > page_size;

    if more {
      children.pop();
    }

    Ok((children, more))
  }

  pub fn get_parents_by_sequence_number_paginated(
    &self,
    parent_sequence_numbers: Vec<u64>,
    page_index: usize,
  ) -> Result<(Vec<InscriptionId>, bool)> {
    const PAGE_SIZE: usize = 100;
    let sequence_number_to_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_inscription_entry' not found"))?;

    let mut parents = parent_sequence_numbers
      .iter()
      .skip(page_index * PAGE_SIZE)
      .take(PAGE_SIZE.saturating_add(1))
      .map(|sequence_number| {
        self
          .database
          .get_cf(sequence_number_to_entry_cf, &sequence_number.to_be_bytes())
          .map(|entry_bytes| InscriptionEntry::load(entry_bytes.unwrap()).id)
          .map_err(|err| err.into())
      })
      .collect::<Result<Vec<InscriptionId>>>()?;

    let more_parents = parents.len() > PAGE_SIZE;

    if more_parents {
      parents.pop();
    }

    Ok((parents, more_parents))
  }

  pub fn get_etching(&self, txid: Txid) -> Result<Option<SpacedRune>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let transaction_id_to_rune_cf = self
      .database
      .cf_handle(CF_TRANSACTION_ID_TO_RUNE)
      .ok_or_else(|| anyhow!("Column family 'transaction_id_to_rune' not found"))?;
    let rune_to_rune_id_cf = self
      .database
      .cf_handle(CF_RUNE_TO_RUNE_ID)
      .ok_or_else(|| anyhow!("Column family 'rune_to_rune_id' not found"))?;
    let rune_id_to_rune_entry_cf = self
      .database
      .cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'rune_id_to_rune_entry' not found"))?;

    let Some(rune_bytes) = self
      .database
      .get_cf(transaction_id_to_rune_cf, &txid.store())?
    else {
      return Ok(None);
    };
    let rune = u128::from_be_bytes(rune_bytes.try_into().unwrap());

    let Some(id_bytes) = self
      .database
      .get_cf(rune_to_rune_id_cf, &rune.to_be_bytes())?
    else {
      return Ok(None);
    };
    let id = RuneId::load(id_bytes);

    let Some(entry_bytes) = self
      .database
      .get_cf(rune_id_to_rune_entry_cf, &id.store())?
    else {
      return Ok(None);
    };
    let entry = RuneEntry::load(entry_bytes);

    Ok(Some(entry.spaced_rune))
  }

  pub fn get_inscription_ids_by_sat(&self, sat: Sat) -> Result<Vec<InscriptionId>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let sat_to_sequence_number_cf = self
      .database
      .cf_handle(CF_SAT_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'sat_to_sequence_number' not found"))?;

    let mut ids = Vec::new();

    // For RocksDB, we need to iterate through the sat_to_sequence_number manually since we don't have multimap tables
    let mut iter = self
      .database
      .iterator_cf(sat_to_sequence_number_cf, IteratorMode::Start);

    while let Some(Ok((sat_bytes, sequence_number_bytes))) = iter.next() {
      let sat_value = u64::from_be_bytes(sat_bytes.as_ref().try_into().unwrap());
      if sat_value == sat.n() {
        let sequence_number =
          u64::from_be_bytes(sequence_number_bytes.as_ref().try_into().unwrap());

        let sequence_number_to_inscription_entry_cf = self
          .database
          .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
          .unwrap();
        if let Some(entry_bytes) = self.database.get_cf(
          sequence_number_to_inscription_entry_cf,
          &sequence_number.to_be_bytes(),
        )? {
          let entry = InscriptionEntry::load(entry_bytes);
          ids.push(entry.id);
        }
      }
    }

    Ok(ids)
  }

  pub fn get_inscription_ids_by_sat_paginated(
    &self,
    sat: Sat,
    page_size: u64,
    page_index: u64,
  ) -> Result<(Vec<InscriptionId>, bool)> {
    // For RocksDB, we'll use direct database access instead of transactions
    let sat_to_sequence_number_cf = self
      .database
      .cf_handle(CF_SAT_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'sat_to_sequence_number' not found"))?;

    let mut ids = Vec::new();
    let mut found_count = 0;
    let mut skipped = 0;

    // For RocksDB, we need to iterate through the sat_to_sequence_number manually since we don't have multimap tables
    let mut iter = self
      .database
      .iterator_cf(sat_to_sequence_number_cf, IteratorMode::Start);

    while let Some(Ok((sat_bytes, sequence_number_bytes))) = iter.next() {
      let sat_value = u64::from_be_bytes(sat_bytes.as_ref().try_into().unwrap());
      if sat_value == sat.n() {
        if skipped >= page_index.saturating_mul(page_size) {
          if found_count < page_size.saturating_add(1) {
            let sequence_number =
              u64::from_be_bytes(sequence_number_bytes.as_ref().try_into().unwrap());

            let sequence_number_to_inscription_entry_cf = self
              .database
              .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
              .unwrap();
            if let Some(entry_bytes) = self.database.get_cf(
              sequence_number_to_inscription_entry_cf,
              &sequence_number.to_be_bytes(),
            )? {
              let entry = InscriptionEntry::load(entry_bytes);
              ids.push(entry.id);
              found_count += 1;
            }
          } else {
            break;
          }
        } else {
          skipped += 1;
        }
      }
    }

    let more = ids.len() > page_size.try_into().unwrap();

    if more {
      ids.pop();
    }

    Ok((ids, more))
  }

  pub fn get_inscription_id_by_sat_indexed(
    &self,
    sat: Sat,
    inscription_index: isize,
  ) -> Result<Option<InscriptionId>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let sat_to_sequence_number_cf = self
      .database
      .cf_handle(CF_SAT_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'sat_to_sequence_number' not found"))?;

    // Collect all sequence numbers for this sat
    let mut sequence_numbers = Vec::new();
    let mut iter = self
      .database
      .iterator_cf(sat_to_sequence_number_cf, IteratorMode::Start);

    while let Some(Ok((sat_bytes, sequence_number_bytes))) = iter.next() {
      let sat_value = u64::from_be_bytes(sat_bytes.as_ref().try_into().unwrap());
      if sat_value == sat.n() {
        let sequence_number =
          u64::from_be_bytes(sequence_number_bytes.as_ref().try_into().unwrap());
        sequence_numbers.push(sequence_number);
      }
    }

    // Sort sequence numbers to maintain order
    sequence_numbers.sort();

    // Get the inscription ID at the specified index
    let sequence_number = if inscription_index < 0 {
      let abs_index = (inscription_index + 1).abs_diff(0);
      sequence_numbers.get(sequence_numbers.len().saturating_sub(abs_index + 1))
    } else {
      sequence_numbers.get(inscription_index.abs_diff(0))
    };

    match sequence_number {
      Some(&seq_num) => {
        let sequence_number_to_inscription_entry_cf = self
          .database
          .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
          .unwrap();
        if let Some(entry_bytes) = self.database.get_cf(
          sequence_number_to_inscription_entry_cf,
          &seq_num.to_be_bytes(),
        )? {
          let entry = InscriptionEntry::load(entry_bytes);
          Ok(Some(entry.id))
        } else {
          Ok(None)
        }
      }
      None => Ok(None),
    }
  }

  #[cfg(test)]
  pub(crate) fn get_inscription_id_by_inscription_number(
    &self,
    inscription_number: i64,
  ) -> Result<Option<InscriptionId>> {
    let inscription_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER)
      .unwrap();

    let Some(sequence_number_bytes) = self
      .database
      .get_cf(inscription_number_cf, &inscription_number.to_be_bytes())?
    else {
      return Ok(None);
    };

    let sequence_number = u64::from_be_bytes(sequence_number_bytes.try_into().unwrap());

    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    let inscription_id = if let Some(entry_bytes) = self.database.get_cf(
      sequence_number_to_inscription_entry_cf,
      &sequence_number.to_be_bytes(),
    )? {
      let entry = InscriptionEntry::load(entry_bytes);
      Some(entry.id)
    } else {
      None
    };

    Ok(inscription_id)
  }

  pub fn get_inscription_satpoint_by_id(
    &self,
    inscription_id: InscriptionId,
  ) -> Result<Option<SatPoint>> {
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let Some(sequence_number_bytes) = self.database.get_cf(
      inscription_id_to_sequence_number_cf,
      &inscription_id.store(),
    )?
    else {
      return Ok(None);
    };
    let sequence_number = u64::from_be_bytes(sequence_number_bytes.try_into().unwrap());

    let sequence_number_to_satpoint_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_SATPOINT)
      .unwrap();
    let satpoint = if let Some(satpoint_bytes) = self.database.get_cf(
      sequence_number_to_satpoint_cf,
      &sequence_number.to_be_bytes(),
    )? {
      Some(SatPoint::load(satpoint_bytes))
    } else {
      None
    };

    Ok(satpoint)
  }

  pub fn get_inscription_by_id(
    &self,
    inscription_id: InscriptionId,
  ) -> Result<Option<Inscription>> {
    if !self.inscription_exists(inscription_id)? {
      return Ok(None);
    }

    Ok(self.get_transaction(inscription_id.txid)?.and_then(|tx| {
      ParsedEnvelope::from_transaction(&tx)
        .into_iter()
        .nth(inscription_id.index as usize)
        .map(|envelope| envelope.payload)
    }))
  }

  pub fn inscription_count(&self, txid: Txid) -> Result<u32> {
    // let start = InscriptionId { index: 0, txid };
    // let end = InscriptionId {
    //   index: u32::MAX,
    //   txid,
    // };

    let cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let mut count = 0;
    let iter = self.database.iterator_cf(cf, IteratorMode::Start);

    for result in iter {
      let (inscription_id_bytes, _) = result?;
      let inscription_id = InscriptionId::load(inscription_id_bytes.to_vec());
      if inscription_id.txid == txid {
        count += 1;
      }
    }

    Ok(count)
  }

  pub fn inscription_exists(&self, inscription_id: InscriptionId) -> Result<bool> {
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    Ok(
      self
        .database
        .get_cf(
          inscription_id_to_sequence_number_cf,
          &inscription_id.store(),
        )?
        .is_some(),
    )
  }

  pub fn get_inscriptions_on_output_with_satpoints(
    &self,
    outpoint: OutPoint,
  ) -> Result<Vec<(SatPoint, InscriptionId)>> {
    // fixme: dust utxo
    let outpoint_to_utxo_entry_cf = self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
    if let Some(entry_bytes) = self
      .database
      .get_cf(outpoint_to_utxo_entry_cf, &outpoint.store())?
    {
      let utxo_entry = UtxoEntry::ref_cast(&entry_bytes).to_buf();
      let parsed = utxo_entry.parse(self);

      let inscriptions_bytes = parsed.inscriptions();
      let mut result = Vec::new();
      let mut byte_offset = 0;

        while byte_offset < inscriptions_bytes.len() {
          let sequence_number = u64::from_le_bytes(
            inscriptions_bytes[byte_offset..byte_offset + 8]
              .try_into()
              .unwrap(),
          );
          byte_offset += 8;

          let (satpoint_offset, varint_len) =
            varint::decode(&inscriptions_bytes[byte_offset..]).unwrap();
          let satpoint_offset = u64::try_from(satpoint_offset).unwrap();
          byte_offset += varint_len;

          let sequence_number_to_inscription_entry_cf = self
            .database
            .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
            .unwrap();
          if let Some(entry_bytes) = self.database.get_cf(
            sequence_number_to_inscription_entry_cf,
            &sequence_number.to_be_bytes(),
          )? {
            let inscription_entry = InscriptionEntry::load(entry_bytes);
            let inscription_id = inscription_entry.id;

            // Calculate the actual SatPoint
            let mut offset = 0;
            let sat_ranges = parsed.sat_ranges();
            for chunk in sat_ranges.chunks_exact(14) {
              let (start, end) = SatRange::load(chunk.try_into().unwrap());
              if satpoint_offset < offset + (end - start) {
                let sat_point = SatPoint {
                  outpoint,
                  offset: satpoint_offset - offset,
                };
                result.push((sat_point, inscription_id));
                break;
              }
              offset += end - start;
            }
          }
        }

        Ok(result)
    } else {
      Ok(Vec::new())
    }
  }

  pub fn get_inscriptions_for_output(&self, outpoint: OutPoint) -> Result<Vec<InscriptionId>> {
    Ok(
      self
        .get_inscriptions_on_output_with_satpoints(outpoint)?
        .iter()
        .map(|(_satpoint, inscription_id)| *inscription_id)
        .collect(),
    )
  }

  pub fn get_inscriptions_for_outputs(
    &self,
    outpoints: &Vec<OutPoint>,
  ) -> Result<Vec<InscriptionId>> {
    let mut inscriptions = Vec::new();
    for outpoint in outpoints {
      inscriptions.extend(
        self
          .get_inscriptions_on_output_with_satpoints(*outpoint)?
          .iter()
          .map(|(_satpoint, inscription_id)| *inscription_id),
      );
    }

    Ok(inscriptions)
  }

  pub fn get_transaction(&self, txid: Txid) -> Result<Option<Transaction>> {
    if txid == self.genesis_block_coinbase_txid {
      return Ok(Some(self.genesis_block_coinbase_transaction.clone()));
    }

    if self.index_transactions {
      let transaction_id_to_transaction_cf = self
        .database
        .cf_handle(CF_TRANSACTION_ID_TO_TRANSACTION)
        .unwrap();
      if let Some(transaction_bytes) = self
        .database
        .get_cf(transaction_id_to_transaction_cf, &txid.store())?
      {
        return Ok(Some(consensus::encode::deserialize(&transaction_bytes)?));
      }
    }

    self.client.get_raw_transaction(&txid, None).into_option()
  }

  pub fn find(&self, sat: Sat) -> Result<Option<SatPoint>> {
    let sat = sat.0;

    if self.block_count()? <= Sat(sat).height().n() {
      return Ok(None);
    }

    let sat_to_satpoint_cf = self.database.cf_handle(CF_SAT_TO_SATPOINT).unwrap();
    if let Some(satpoint_bytes) = self
      .database
      .get_cf(sat_to_satpoint_cf, &sat.to_be_bytes())?
    {
      return Ok(Some(SatPoint::load(satpoint_bytes)));
    }

    // If not found in the cache/table above, iterate over every UTXO entry
    let outpoint_to_utxo_entry_cf = self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();

    for entry in self
      .database
      .iterator_cf(outpoint_to_utxo_entry_cf, IteratorMode::Start)
    {
      let (outpoint_bytes, utxo_entry_bytes) = entry?;
      let sat_ranges = UtxoEntry::ref_cast(&utxo_entry_bytes)
        .parse(self)
        .sat_ranges();

      let mut offset = 0;
      for chunk in sat_ranges.chunks_exact(14) {
        let (start, end) = SatRange::load(chunk.try_into().unwrap());
        if start <= sat && sat < end {
          let satpoint = SatPoint {
            outpoint: OutPoint::load(outpoint_bytes.as_ref().try_into().unwrap()),
            offset: offset + sat - start,
          };
          // Return immediately without caching the result
          return Ok(Some(satpoint));
        }
        offset += end - start;
      }
    }

    Ok(None)
  }

  pub fn find_range(
    &self,
    range_start: Sat,
    range_end: Sat,
  ) -> Result<Option<Vec<FindRangeOutput>>> {
    let range_start = range_start.0;
    let range_end = range_end.0;

    if self.block_count()? < Sat(range_end - 1).height().n() + 1 {
      return Ok(None);
    }

    let Some(mut remaining_sats) = range_end.checked_sub(range_start) else {
      return Err(anyhow!("range end is before range start"));
    };

    let outpoint_to_utxo_entry_cf = self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();

    let mut result = Vec::new();
    for entry in self
      .database
      .iterator_cf(outpoint_to_utxo_entry_cf, IteratorMode::Start)
    {
      let (outpoint_bytes, utxo_entry_bytes) = entry?;
      let sat_ranges = UtxoEntry::ref_cast(&utxo_entry_bytes)
        .parse(self)
        .sat_ranges();

      let mut offset = 0;
      for sat_range in sat_ranges.chunks_exact(14) {
        let (start, end) = SatRange::load(sat_range.try_into().unwrap());

        if end > range_start && start < range_end {
          let overlap_start = start.max(range_start);
          let overlap_end = end.min(range_end);

          result.push(FindRangeOutput {
            start: overlap_start,
            size: overlap_end - overlap_start,
            satpoint: SatPoint {
              outpoint: OutPoint::load(outpoint_bytes.as_ref().try_into().unwrap()),
              offset: offset + overlap_start - start,
            },
          });

          remaining_sats -= overlap_end - overlap_start;

          if remaining_sats == 0 {
            break;
          }
        }
        offset += end - start;
      }
    }

    Ok(Some(result))
  }

  pub fn get_inscription_entry(
    &self,
    inscription_id: InscriptionId,
  ) -> Result<Option<InscriptionEntry>> {
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let sequence_number = if let Some(sequence_number_bytes) = self.database.get_cf(
      inscription_id_to_sequence_number_cf,
      &inscription_id.store(),
    )? {
      u64::from_be_bytes(sequence_number_bytes.try_into().unwrap())
    } else {
      return Ok(None);
    };

    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    if let Some(entry_bytes) = self.database.get_cf(
      sequence_number_to_inscription_entry_cf,
      &sequence_number.to_be_bytes(),
    )? {
      Ok(Some(InscriptionEntry::load(entry_bytes)))
    } else {
      Ok(None)
    }
  }

  pub fn list(&self, outpoint: OutPoint) -> Result<Option<Vec<(u64, u64)>>> {
    if !self.index_sats {
      return Ok(None);
    }

    let outpoint_to_utxo_entry_cf = self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
    if let Some(entry_bytes) = self
      .database
      .get_cf(outpoint_to_utxo_entry_cf, &outpoint.store())?
    {
      let utxo_entry = UtxoEntry::ref_cast(entry_bytes.as_ref()).to_buf();
      Ok(Some(
        utxo_entry
          .parse(self)
          .sat_ranges()
          .chunks_exact(14)
          .map(|chunk| SatRange::load(chunk.try_into().unwrap()))
          .collect::<Vec<(u64, u64)>>(),
      ))
    } else {
      Ok(None)
    }
  }

  pub fn is_output_spent(&self, outpoint: OutPoint) -> Result<bool> {
    Ok(
      outpoint != OutPoint::null()
        && outpoint != self.settings.chain().genesis_coinbase_outpoint()
        && if self.index_sats {
          let outpoint_to_utxo_entry_cf =
            self.database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
          let outpoint_to_nondust_utxo_entry_cf = self
            .database
            .cf_handle(CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY)
            .unwrap();

          self
            .database
            .get_cf(outpoint_to_utxo_entry_cf, &outpoint.store())?
            .is_none()
            && self
              .database
              .get_cf(outpoint_to_nondust_utxo_entry_cf, &outpoint.store())?
              .is_none()
        } else {
          self
            .client
            .get_tx_out(&outpoint.txid, outpoint.vout, Some(true))?
            .is_none()
        },
    )
  }

  pub fn is_output_in_active_chain(&self, outpoint: OutPoint) -> Result<bool> {
    if outpoint == OutPoint::null() {
      return Ok(true);
    }

    if outpoint == self.settings.chain().genesis_coinbase_outpoint() {
      return Ok(true);
    }

    let Some(info) = self
      .client
      .get_raw_transaction_info(&outpoint.txid, None)
      .into_option()?
    else {
      return Ok(false);
    };

    if info.blockhash.is_none() {
      return Ok(false);
    }

    if outpoint.vout.into_usize() >= info.vout.len() {
      return Ok(false);
    }

    Ok(true)
  }

  pub fn block_time(&self, height: Height) -> Result<Blocktime> {
    let height = height.n();

    let height_to_block_header_cf = self.database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();

    if let Some(header_bytes) = self
      .database
      .get_cf(height_to_block_header_cf, &height.to_be_bytes())?
    {
      return Ok(Blocktime::confirmed(Header::load(header_bytes).time));
    }

    let current = {
      let mut iter = self
        .database
        .iterator_cf(height_to_block_header_cf, IteratorMode::End);
      if let Some(Ok((height_bytes, _))) = iter.next() {
        u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap())
      } else {
        0
      }
    };

    let expected_blocks = height
      .checked_sub(current)
      .with_context(|| format!("current {current} height is greater than sat height {height}"))?;

    Ok(Blocktime::Expected(
      Utc::now()
        .round_subsecs(0)
        .checked_add_signed(
          chrono::Duration::try_seconds(10 * 60 * i64::from(expected_blocks))
            .context("timestamp out of range")?,
        )
        .context("timestamp out of range")?,
    ))
  }

  pub fn get_inscriptions_paginated(
    &self,
    page_size: u64,
    page_index: u64,
  ) -> Result<(Vec<InscriptionId>, bool)> {
    // For RocksDB, we'll use direct database access instead of transactions
    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_inscription_entry' not found"))?;

    // Get the last sequence number
    let mut iter = self
      .database
      .iterator_cf(sequence_number_to_inscription_entry_cf, IteratorMode::End);
    let last = if let Some(Ok((number_bytes, _))) = iter.next() {
      u64::from_be_bytes(number_bytes.as_ref().try_into().unwrap())
    } else {
      0
    };

    let start = last.saturating_sub(page_size.saturating_mul(page_index));
    let end = start.saturating_sub(page_size);

    let mut inscriptions = Vec::new();

    // Iterate through the range in reverse order
    // let mut iter = self
    //   .database
    //   .iterator_cf(sequence_number_to_inscription_entry_cf, IteratorMode::End);
    let mut current = last;

    while current > end && inscriptions.len() < page_size.saturating_add(1).try_into().unwrap() {
      if current <= start {
        let sequence_number_to_inscription_entry_cf = self
          .database
          .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
          .unwrap();
        if let Some(entry_bytes) = self.database.get_cf(
          sequence_number_to_inscription_entry_cf,
          &current.to_be_bytes(),
        )? {
          let entry = InscriptionEntry::load(entry_bytes);
          inscriptions.push(entry.id);
        }
      }
      current = current.saturating_sub(1);
    }

    let more = u64::try_from(inscriptions.len()).unwrap_or(u64::MAX) > page_size;

    if more {
      inscriptions.pop();
    }

    Ok((inscriptions, more))
  }

  pub fn get_inscriptions_in_block(&self, block_height: u32) -> Result<Vec<InscriptionId>> {
    let height_to_last_sequence_number_cf = self
      .database
      .cf_handle(CF_HEIGHT_TO_LAST_SEQUENCE_NUMBER)
      .unwrap();
    let Some(newest_sequence_number_bytes) = self.database.get_cf(
      height_to_last_sequence_number_cf,
      &block_height.to_be_bytes(),
    )?
    else {
      return Ok(Vec::new());
    };
    let newest_sequence_number =
      u64::from_be_bytes(TryInto::<[u8; 8]>::try_into(newest_sequence_number_bytes).unwrap());

    let oldest_sequence_number = if let Some(oldest_bytes) = self.database.get_cf(
      height_to_last_sequence_number_cf,
      &block_height.saturating_sub(1).to_be_bytes(),
    )? {
      u64::from_be_bytes(TryInto::<[u8; 8]>::try_into(oldest_bytes).unwrap())
    } else {
      0
    };

    let mut inscriptions = Vec::new();
    for num in oldest_sequence_number..newest_sequence_number {
      let sequence_number_to_inscription_entry_cf = self
        .database
        .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
        .unwrap();
      if let Some(entry_bytes) = self
        .database
        .get_cf(sequence_number_to_inscription_entry_cf, &num.to_be_bytes())?
      {
        let entry = InscriptionEntry::load(entry_bytes);
        inscriptions.push(entry.id);
      }
    }

    Ok(inscriptions)
  }

  pub fn get_runes_in_block(&self, block_height: u64) -> Result<Vec<SpacedRune>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let rune_id_to_rune_entry_cf = self
      .database
      .cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'rune_id_to_rune_entry' not found"))?;

    // let min_id = RuneId {
    //   block: block_height,
    //   tx: 0,
    // };

    // let max_id = RuneId {
    //   block: block_height,
    //   tx: u32::MAX,
    // };

    let mut runes = Vec::new();
    let mut iter = self
      .database
      .iterator_cf(rune_id_to_rune_entry_cf, IteratorMode::Start);

    while let Some(Ok((rune_id_bytes, entry_bytes))) = iter.next() {
      let rune_id = RuneId::load(rune_id_bytes.to_vec());
      if rune_id.block == block_height {
        let entry = RuneEntry::load(entry_bytes.to_vec());
        runes.push(entry.spaced_rune);
      }
      // If we've passed the max_id, we can stop
      if rune_id.block > block_height {
        break;
      }
    }

    Ok(runes)
  }

  pub fn get_highest_paying_inscriptions_in_block(
    &self,
    block_height: u32,
    n: usize,
  ) -> Result<(Vec<InscriptionId>, usize)> {
    let inscription_ids = self.get_inscriptions_in_block(block_height)?;

    let mut inscription_to_fee: Vec<(InscriptionId, u64)> = Vec::new();
    for id in &inscription_ids {
      inscription_to_fee.push((
        *id,
        self
          .get_inscription_entry(*id)?
          .ok_or_else(|| anyhow!("could not get entry for inscription {id}"))?
          .fee,
      ));
    }

    inscription_to_fee.sort_by_key(|(_, fee)| *fee);

    Ok((
      inscription_to_fee
        .iter()
        .map(|(id, _)| *id)
        .rev()
        .take(n)
        .collect(),
      inscription_ids.len(),
    ))
  }

  pub fn get_home_inscriptions(&self) -> Result<Vec<InscriptionId>> {
    let (inscriptions, _more) = self.get_inscriptions_paginated(20, 0)?;

    Ok(inscriptions)
  }

  pub fn get_feed_inscriptions(&self, n: usize) -> Result<Vec<(u64, InscriptionId)>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_inscription_entry' not found"))?;

    let mut inscriptions = Vec::new();
    let mut iter = self
      .database
      .iterator_cf(sequence_number_to_inscription_entry_cf, IteratorMode::End);

    for _ in 0..n {
      if let Some(Ok((number_bytes, entry_bytes))) = iter.next() {
        let number = u64::from_be_bytes(number_bytes.as_ref().try_into().unwrap());
        let entry = InscriptionEntry::load(entry_bytes.to_vec());
        inscriptions.push((number, entry.id));
      } else {
        break;
      }
    }

    Ok(inscriptions)
  }

  pub(crate) fn inscription_info(
    &self,
    query: query::Inscription,
    child: Option<usize>,
  ) -> Result<Option<(api::Inscription, Option<TxOut>, Inscription)>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'inscription_id_to_sequence_number' not found"))?;
    let sat_to_sequence_number_cf = self
      .database
      .cf_handle(CF_SAT_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'sat_to_sequence_number' not found"))?;
    let sequence_number_to_children_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_CHILDREN)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_children' not found"))?;

    let sequence_number = match query {
      query::Inscription::Id(id) => {
        if let Some(sequence_number_bytes) = self
          .database
          .get_cf(inscription_id_to_sequence_number_cf, &id.store())?
        {
          Some(u64::from_be_bytes(
            TryInto::<[u8; 8]>::try_into(sequence_number_bytes).unwrap(),
          ))
        } else {
          None
        }
      }
      query::Inscription::Number(inscription_number) => {
        let inscription_number_to_sequence_number_cf = self
          .database
          .cf_handle(CF_INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER)
          .unwrap();
        if let Some(sequence_number_bytes) = self.database.get_cf(
          inscription_number_to_sequence_number_cf,
          &inscription_number.to_be_bytes(),
        )? {
          Some(u64::from_be_bytes(
            TryInto::<[u8; 8]>::try_into(sequence_number_bytes).unwrap(),
          ))
        } else {
          None
        }
      }
      query::Inscription::Sat(sat) => {
        // For RocksDB, we need to iterate through the sat_to_sequence_number manually since we don't have multimap tables
        let mut iter = self
          .database
          .iterator_cf(sat_to_sequence_number_cf, IteratorMode::Start);
        let mut found_sequence_number = None;

        while let Some(Ok((sat_bytes, sequence_number_bytes))) = iter.next() {
          let sat_value = u64::from_be_bytes(sat_bytes.as_ref().try_into().unwrap());
          if sat_value == sat.n() {
            found_sequence_number = Some(u64::from_be_bytes(
              sequence_number_bytes.as_ref().try_into().unwrap(),
            ));
            break;
          }
        }

        found_sequence_number
      }
    };

    let Some(sequence_number) = sequence_number else {
      return Ok(None);
    };

    let sequence_number = if let Some(child) = child {
      // Key format: parent_sequence_number (8 bytes) + child_sequence_number (8 bytes)
      let prefix = sequence_number.to_be_bytes();
      // Use IteratorMode::From to seek directly to the desired prefix and avoid a full scan
      let mut iter = self.database.iterator_cf(
        sequence_number_to_children_cf,
        IteratorMode::From(&prefix, Direction::Forward),
      );
      let mut found_children = 0;
      let mut found_child = None;

      while let Some(Ok((key_bytes, _))) = iter.next() {
        // Check if the key starts with the expected parent sequence number; a mismatch means we're done
        if key_bytes.len() < 8 || key_bytes[0..8] != prefix {
          break;
        }

        // Extract the child sequence number from the last 8 bytes
        if key_bytes.len() < 16 {
          continue;
        }
        let child_sequence_number = u64::from_be_bytes(
          key_bytes[8..16]
            .try_into()
            .map_err(|_| anyhow!("Invalid key format"))?,
        );

        if found_children == child {
          found_child = Some(child_sequence_number);
          break;
        }
        found_children += 1;
      }

      let Some(child) = found_child else {
        return Ok(None);
      };

      child
    } else {
      sequence_number
    };

    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    let Some(entry_bytes) = self.database.get_cf(
      sequence_number_to_inscription_entry_cf,
      &sequence_number.to_be_bytes(),
    )?
    else {
      return Ok(None);
    };
    let entry = InscriptionEntry::load(entry_bytes);

    let Some(transaction) = self.get_transaction(entry.id.txid)? else {
      return Ok(None);
    };

    let Some(inscription) = ParsedEnvelope::from_transaction(&transaction)
      .into_iter()
      .nth(entry.id.index as usize)
      .map(|envelope| envelope.payload)
    else {
      return Ok(None);
    };

    let sequence_number_to_satpoint_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_SATPOINT)
      .unwrap();
    let Some(satpoint_bytes) = self.database.get_cf(
      sequence_number_to_satpoint_cf,
      &sequence_number.to_be_bytes(),
    )?
    else {
      return Ok(None);
    };
    let satpoint = SatPoint::load(satpoint_bytes);

    let output = if satpoint.outpoint == unbound_outpoint() || satpoint.outpoint == OutPoint::null()
    {
      None
    } else {
      let Some(transaction) = self.get_transaction(satpoint.outpoint.txid)? else {
        return Ok(None);
      };

      transaction
        .output
        .into_iter()
        .nth(satpoint.outpoint.vout.try_into().unwrap())
    };

    let previous = if let Some(n) = sequence_number.checked_sub(1) {
      if let Some(prev_entry_bytes) = self
        .database
        .get_cf(sequence_number_to_inscription_entry_cf, &n.to_be_bytes())?
      {
        Some(InscriptionEntry::load(prev_entry_bytes).id)
      } else {
        None
      }
    } else {
      None
    };

    let sequence_number_to_inscription_entry_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    let next = if let Some(entry_bytes) = self.database.get_cf(
      sequence_number_to_inscription_entry_cf,
      &(sequence_number + 1).to_be_bytes(),
    )? {
      let next_entry = InscriptionEntry::load(entry_bytes);
      Some(next_entry.id)
    } else {
      None
    };

    let mut children = Vec::new();
    // Key format: parent_sequence_number (8 bytes) + child_sequence_number (8 bytes)
    let prefix = sequence_number.to_be_bytes();
    // Use IteratorMode::From to iterate from the desired prefix and avoid a full scan
    let mut iter = self.database.iterator_cf(
      sequence_number_to_children_cf,
      IteratorMode::From(&prefix, Direction::Forward),
    );
    let mut found_children = 0;

    while found_children < 4 {
      if let Some(Ok((key_bytes, _))) = iter.next() {
        // Verify the prefix; if it differs we've exhausted relevant entries
        if key_bytes.len() < 8 || key_bytes[0..8] != prefix {
          break;
        }

        // Extract the child sequence number from the last 8 bytes
        if key_bytes.len() < 16 {
          continue;
        }
        let child_sequence_number = u64::from_be_bytes(
          key_bytes[8..16]
            .try_into()
            .map_err(|_| anyhow!("Invalid key format"))?,
        );

        // Look up the child entry using its sequence number
        let sequence_number_to_inscription_entry_cf = self
          .database
          .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
          .unwrap();
        if let Some(entry_bytes) = self.database.get_cf(
          sequence_number_to_inscription_entry_cf,
          &child_sequence_number.to_be_bytes(),
        )? {
          let child_entry = InscriptionEntry::load(entry_bytes);
          children.push(child_entry.id);
          found_children += 1;
        }
      } else {
        break;
      }
    }

    let sequence_number_to_rune_id_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_RUNE_ID)
      .unwrap();
    let rune = if let Some(rune_id_bytes) = self.database.get_cf(
      sequence_number_to_rune_id_cf,
      &sequence_number.to_be_bytes(),
    )? {
      let rune_id = RuneId::load(rune_id_bytes.to_vec());
      let rune_id_to_rune_entry_cf = self.database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
      if let Some(entry_bytes) = self
        .database
        .get_cf(rune_id_to_rune_entry_cf, &rune_id.store())?
      {
        let rune_entry = RuneEntry::load(entry_bytes);
        Some(rune_entry.spaced_rune)
      } else {
        None
      }
    } else {
      None
    };

    let mut parents = Vec::new();
    for parent in entry.parents.iter().take(4) {
      let sequence_number_to_inscription_entry_cf = self
        .database
        .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
        .unwrap();
      if let Some(entry_bytes) = self.database.get_cf(
        sequence_number_to_inscription_entry_cf,
        &parent.to_be_bytes(),
      )? {
        let parent_entry = InscriptionEntry::load(entry_bytes);
        parents.push(parent_entry.id);
      }
    }

    let mut charms = entry.charms;

    if satpoint.outpoint == OutPoint::null() {
      Charm::Lost.set(&mut charms);
    }

    let effective_mime_type = if let Some(delegate_id) = inscription.delegate() {
      let delegate_result = self.get_inscription_by_id(delegate_id);
      if let Ok(Some(delegate)) = delegate_result {
        delegate.content_type().map(str::to_string)
      } else {
        inscription.content_type().map(str::to_string)
      }
    } else {
      inscription.content_type().map(str::to_string)
    };

    Ok(Some((
      api::Inscription {
        address: output
          .as_ref()
          .and_then(|o| {
            self
              .settings
              .chain()
              .address_from_script(&o.script_pubkey)
              .ok()
          })
          .map(|address| address.to_string()),
        charms: Charm::charms(charms),
        children,
        content_length: inscription.content_length(),
        content_type: inscription.content_type().map(|s| s.to_string()),
        effective_content_type: effective_mime_type,
        fee: entry.fee,
        height: entry.height,
        id: entry.id,
        next,
        number: entry.inscription_number,
        parents,
        previous,
        rune,
        sat: entry.sat,
        satpoint,
        timestamp: timestamp(entry.timestamp.into()).timestamp(),
        value: output.as_ref().map(|o| o.value),
      },
      output,
      inscription,
    )))
  }

  #[cfg(test)]
  fn assert_inscription_location(
    &self,
    inscription_id: InscriptionId,
    satpoint: SatPoint,
    sat: Option<u64>,
  ) {
    // For RocksDB, we'll use direct database access instead of transactions
    let outpoint_to_utxo_entry_cf = self
      .database
      .cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'outpoint_to_utxo_entry' not found"))
      .unwrap();
    let sequence_number_to_satpoint_cf = self
      .database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_SATPOINT)
      .ok_or_else(|| anyhow!("Column family 'sequence_number_to_satpoint' not found"))
      .unwrap();
    let inscription_id_to_sequence_number_cf = self
      .database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'inscription_id_to_sequence_number' not found"))
      .unwrap();
    let sat_to_sequence_number_cf = self
      .database
      .cf_handle(CF_SAT_TO_SEQUENCE_NUMBER)
      .ok_or_else(|| anyhow!("Column family 'sat_to_sequence_number' not found"))
      .unwrap();
    let sat_to_satpoint_cf = self
      .database
      .cf_handle(CF_SAT_TO_SATPOINT)
      .ok_or_else(|| anyhow!("Column family 'sat_to_satpoint' not found"))
      .unwrap();

    let sequence_number_bytes = self
      .database
      .get_cf(
        inscription_id_to_sequence_number_cf,
        &inscription_id.store(),
      )
      .unwrap()
      .unwrap();
    let sequence_number = u64::from_be_bytes(sequence_number_bytes.try_into().unwrap());

    let satpoint_bytes = self
      .database
      .get_cf(
        sequence_number_to_satpoint_cf,
        &sequence_number.to_be_bytes(),
      )
      .unwrap()
      .unwrap();
    assert_eq!(SatPoint::load(satpoint_bytes), satpoint,);

    let utxo_entry_bytes = self
      .database
      .get_cf(outpoint_to_utxo_entry_cf, &satpoint.outpoint.store())
      .unwrap()
      .unwrap();
    let utxo_entry = UtxoEntry::ref_cast(&utxo_entry_bytes);
    let parsed_inscriptions = utxo_entry.parse(self).parse_inscriptions();
    let satpoint_offsets: Vec<u64> = parsed_inscriptions
      .iter()
      .copied()
      .filter_map(|(seq, offset)| (seq == sequence_number).then_some(offset))
      .collect();
    assert!(satpoint_offsets == [satpoint.offset]);

    match sat {
      Some(sat) => {
        if self.index_sats {
          // unbound inscriptions should not be assigned to a sat
          assert_ne!(satpoint.outpoint, unbound_outpoint());

          // For RocksDB, we need to iterate through the sat_to_sequence_number manually since we don't have multimap tables
          let mut iter = self
            .database
            .iterator_cf(sat_to_sequence_number_cf, IteratorMode::Start);
          let mut found = false;

          while let Some(Ok((sat_bytes, sequence_number_bytes))) = iter.next() {
            let sat_value = u64::from_be_bytes(sat_bytes.as_ref().try_into().unwrap());
            if sat_value == sat {
              let seq_num = u64::from_be_bytes(sequence_number_bytes.as_ref().try_into().unwrap());
              if seq_num == sequence_number {
                found = true;
                break;
              }
            }
          }

          assert!(found);

          // we do not track common sats (only the sat ranges)
          if !Sat(sat).common() {
            let satpoint_bytes = self
              .database
              .get_cf(sat_to_satpoint_cf, &sat.to_be_bytes())
              .unwrap()
              .unwrap();
            assert_eq!(SatPoint::load(satpoint_bytes), satpoint,);
          }
        }
      }
      None => {
        if self.index_sats {
          assert_eq!(satpoint.outpoint, unbound_outpoint())
        }
      }
    }
  }

  pub fn get_address_info(&self, address: &Address) -> Result<Vec<OutPoint>> {
    // For RocksDB, we'll use direct database access instead of transactions
    let script_pubkey_to_outpoint_cf = self
      .database
      .cf_handle(CF_SCRIPT_PUBKEY_TO_OUTPOINT)
      .ok_or_else(|| anyhow!("Column family 'script_pubkey_to_outpoint' not found"))?;

    let mut outpoints = Vec::new();
    let script_pubkey: Vec<u8> = address.script_pubkey().as_bytes().to_vec();
    // Use IteratorMode::From to start at the script_pubkey prefix and avoid scanning the entire column family
    // Key format: script_pubkey bytes followed by the serialized outpoint
    let mut iter = self.database.iterator_cf(
      script_pubkey_to_outpoint_cf,
      IteratorMode::From(&script_pubkey, Direction::Forward),
    );

    while let Some(Ok((key_bytes, _))) = iter.next() {
      // Verify that the key begins with the target script_pubkey prefix; a mismatch means we're done because keys are ordered
      if key_bytes.len() < script_pubkey.len()
        || &key_bytes[0..script_pubkey.len()] != script_pubkey.as_slice()
      {
        break; // Prefix mismatch indicates we've iterated past all relevant entries
      }

      // Extract the serialized outpoint (fixed 36 bytes: 32-byte txid + 4-byte vout)
      if key_bytes.len() < script_pubkey.len() + 36 {
        continue; // Skip malformed keys
      }
      let outpoint_bytes = key_bytes[script_pubkey.len()..script_pubkey.len() + 36].to_vec();
      let outpoint = OutPoint::load(outpoint_bytes);
      outpoints.push(outpoint);
    }

    Ok(outpoints)
  }

  pub(crate) fn get_aggregated_rune_balances_for_outputs(
    &self,
    outputs: &Vec<OutPoint>,
  ) -> Result<Vec<(SpacedRune, Decimal, Option<char>)>> {
    let mut runes = BTreeMap::new();

    for output in outputs {
      let rune_balances = self.get_rune_balances_for_output(*output)?;

      for (spaced_rune, pile) in rune_balances {
        runes
          .entry(spaced_rune)
          .and_modify(|(decimal, _symbol): &mut (Decimal, Option<char>)| {
            assert_eq!(decimal.scale, pile.divisibility);
            decimal.value += pile.amount;
          })
          .or_insert((
            Decimal {
              value: pile.amount,
              scale: pile.divisibility,
            },
            pile.symbol,
          ));
      }
    }

    Ok(
      runes
        .into_iter()
        .map(|(spaced_rune, (decimal, symbol))| (spaced_rune, decimal, symbol))
        .collect(),
    )
  }

  pub(crate) fn get_sat_balances_for_outputs(&self, outputs: &Vec<OutPoint>) -> Result<u64> {
    // fixme: nondust utxo
    // For RocksDB, we'll use direct database access instead of transactions
    let outpoint_to_utxo_entry_cf = self
      .database
      .cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY)
      .ok_or_else(|| anyhow!("Column family 'outpoint_to_utxo_entry' not found"))?;

    let mut acc = 0;
    for output in outputs {
      if let Some(utxo_entry_bytes) = self
        .database
        .get_cf(outpoint_to_utxo_entry_cf, &output.store())?
      {
        let utxo_entry = UtxoEntry::ref_cast(&utxo_entry_bytes);
        acc += utxo_entry.parse(self).total_value();
      };
    }

    Ok(acc)
  }

  pub(crate) fn get_output_info(&self, outpoint: OutPoint) -> Result<Option<(api::Output, TxOut)>> {
    let sat_ranges = self.list(outpoint)?;

    let indexed;

    let txout = if outpoint == OutPoint::null() || outpoint == unbound_outpoint() {
      let mut value = 0;

      if let Some(ranges) = &sat_ranges {
        for (start, end) in ranges {
          value += end - start;
        }
      }

      indexed = true;

      TxOut {
        value,
        script_pubkey: ScriptBuf::new(),
      }
    } else {
      indexed = self.contains_output(&outpoint)?;

      let Some(tx) = self.get_transaction(outpoint.txid)? else {
        return Ok(None);
      };

      let Some(txout) = tx.output.into_iter().nth(outpoint.vout as usize) else {
        return Ok(None);
      };

      txout
    };

    let inscriptions = self.get_inscriptions_for_output(outpoint)?;

    let runes = self.get_rune_balances_for_output(outpoint)?;

    let spent = self.is_output_spent(outpoint)?;

    Ok(Some((
      api::Output::new(
        self.settings.chain(),
        inscriptions,
        outpoint,
        txout.clone(),
        indexed,
        runes,
        sat_ranges,
        spent,
      ),
      txout,
    )))
  }
}

#[cfg(test)]
mod tests {
  use {super::*, crate::index::testing::Context};

  #[test]
  fn height_limit() {
    {
      let context = Context::builder().args(["--height-limit", "0"]).build();
      context.mine_blocks(1);
      assert_eq!(context.index.block_height().unwrap(), None);
      assert_eq!(context.index.block_count().unwrap(), 0);
    }

    {
      let context = Context::builder().args(["--height-limit", "1"]).build();
      context.mine_blocks(1);
      assert_eq!(context.index.block_height().unwrap(), Some(Height(0)));
      assert_eq!(context.index.block_count().unwrap(), 1);
    }

    {
      let context = Context::builder().args(["--height-limit", "2"]).build();
      context.mine_blocks(2);
      assert_eq!(context.index.block_height().unwrap(), Some(Height(1)));
      assert_eq!(context.index.block_count().unwrap(), 2);
    }
  }

  #[test]
  fn inscriptions_below_first_inscription_height_are_skipped() {
    let inscription = inscription("text/plain;charset=utf-8", "hello");
    let template = TransactionTemplate {
      inputs: &[(1, 0, 0, inscription.to_witness())],
      ..default()
    };

    {
      let context = Context::builder().build();
      context.mine_blocks(1);
      let txid = context.core.broadcast_tx(template.clone());
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks(1);

      assert_eq!(
        context.index.get_inscription_by_id(inscription_id).unwrap(),
        Some(inscription)
      );

      assert_eq!(
        context
          .index
          .get_inscription_satpoint_by_id(inscription_id)
          .unwrap(),
        Some(SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        })
      );
    }

    {
      let context = Context::builder()
        .arg("--first-inscription-height=3")
        .build();
      context.mine_blocks(1);
      let txid = context.core.broadcast_tx(template);
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks(1);

      assert_eq!(
        context
          .index
          .get_inscription_satpoint_by_id(inscription_id)
          .unwrap(),
        None,
      );
    }
  }

  #[test]
  fn inscriptions_are_not_indexed_if_no_index_inscriptions_flag_is_set() {
    let inscription = inscription("text/plain;charset=utf-8", "hello");
    let template = TransactionTemplate {
      inputs: &[(1, 0, 0, inscription.to_witness())],
      ..default()
    };

    {
      let context = Context::builder().build();
      context.mine_blocks(1);
      let txid = context.core.broadcast_tx(template.clone());
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks(1);

      assert_eq!(
        context.index.get_inscription_by_id(inscription_id).unwrap(),
        Some(inscription)
      );

      assert_eq!(
        context
          .index
          .get_inscription_satpoint_by_id(inscription_id)
          .unwrap(),
        Some(SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        })
      );
    }

    {
      let context = Context::builder().arg("--no-index-inscriptions").build();
      context.mine_blocks(1);
      let txid = context.core.broadcast_tx(template);
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks(1);

      assert_eq!(
        context
          .index
          .get_inscription_satpoint_by_id(inscription_id)
          .unwrap(),
        None,
      );
    }
  }

  #[test]
  fn list_first_coinbase_transaction() {
    let context = Context::builder().arg("--index-sats").build();
    assert_eq!(
      context
        .index
        .list(
          "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b:0"
            .parse()
            .unwrap()
        )
        .unwrap()
        .unwrap(),
      &[(0, 50 * COIN_VALUE)],
    )
  }

  #[test]
  fn list_second_coinbase_transaction() {
    let context = Context::builder().arg("--index-sats").build();
    let txid = context.mine_blocks(1)[0].txdata[0].txid();
    assert_eq!(
      context.index.list(OutPoint::new(txid, 0)).unwrap().unwrap(),
      &[(50 * COIN_VALUE, 100 * COIN_VALUE)],
    )
  }

  #[test]
  fn list_split_ranges_are_tracked_correctly() {
    let context = Context::builder().arg("--index-sats").build();

    context.mine_blocks(1);
    let split_coinbase_output = TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      outputs: 2,
      fee: 0,
      ..default()
    };
    let txid = context.core.broadcast_tx(split_coinbase_output);

    context.mine_blocks(1);

    assert_eq!(
      context.index.list(OutPoint::new(txid, 0)).unwrap().unwrap(),
      &[(50 * COIN_VALUE, 75 * COIN_VALUE)],
    );

    assert_eq!(
      context.index.list(OutPoint::new(txid, 1)).unwrap().unwrap(),
      &[(75 * COIN_VALUE, 100 * COIN_VALUE)],
    );
  }

  #[test]
  fn list_merge_ranges_are_tracked_correctly() {
    let context = Context::builder().arg("--index-sats").build();

    context.mine_blocks(2);
    let merge_coinbase_outputs = TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default()), (2, 0, 0, Default::default())],
      fee: 0,
      ..default()
    };

    let txid = context.core.broadcast_tx(merge_coinbase_outputs);
    context.mine_blocks(1);

    assert_eq!(
      context.index.list(OutPoint::new(txid, 0)).unwrap().unwrap(),
      &[
        (50 * COIN_VALUE, 100 * COIN_VALUE),
        (100 * COIN_VALUE, 150 * COIN_VALUE)
      ],
    );
  }

  #[test]
  fn list_fee_paying_transaction_range() {
    let context = Context::builder().arg("--index-sats").build();

    context.mine_blocks(1);
    let fee_paying_tx = TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      outputs: 2,
      fee: 10,
      ..default()
    };
    let txid = context.core.broadcast_tx(fee_paying_tx);
    let coinbase_txid = context.mine_blocks(1)[0].txdata[0].txid();

    assert_eq!(
      context.index.list(OutPoint::new(txid, 0)).unwrap().unwrap(),
      &[(50 * COIN_VALUE, 7499999995)],
    );

    assert_eq!(
      context.index.list(OutPoint::new(txid, 1)).unwrap().unwrap(),
      &[(7499999995, 9999999990)],
    );

    assert_eq!(
      context
        .index
        .list(OutPoint::new(coinbase_txid, 0))
        .unwrap()
        .unwrap(),
      &[(10000000000, 15000000000), (9999999990, 10000000000)],
    );
  }

  #[test]
  fn list_two_fee_paying_transaction_range() {
    let context = Context::builder().arg("--index-sats").build();

    context.mine_blocks(2);
    let first_fee_paying_tx = TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      fee: 10,
      ..default()
    };
    let second_fee_paying_tx = TransactionTemplate {
      inputs: &[(2, 0, 0, Default::default())],
      fee: 10,
      ..default()
    };
    context.core.broadcast_tx(first_fee_paying_tx);
    context.core.broadcast_tx(second_fee_paying_tx);

    let coinbase_txid = context.mine_blocks(1)[0].txdata[0].txid();

    assert_eq!(
      context
        .index
        .list(OutPoint::new(coinbase_txid, 0))
        .unwrap()
        .unwrap(),
      &[
        (15000000000, 20000000000),
        (9999999990, 10000000000),
        (14999999990, 15000000000)
      ],
    );
  }

  #[test]
  fn list_null_output() {
    let context = Context::builder().arg("--index-sats").build();

    context.mine_blocks(1);
    let no_value_output = TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      fee: 50 * COIN_VALUE,
      ..default()
    };
    let txid = context.core.broadcast_tx(no_value_output);
    context.mine_blocks(1);

    assert_eq!(
      context.index.list(OutPoint::new(txid, 0)).unwrap().unwrap(),
      &[],
    );
  }

  #[test]
  fn list_null_input() {
    let context = Context::builder().arg("--index-sats").build();

    context.mine_blocks(1);
    let no_value_output = TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      fee: 50 * COIN_VALUE,
      ..default()
    };
    context.core.broadcast_tx(no_value_output);
    context.mine_blocks(1);

    let no_value_input = TransactionTemplate {
      inputs: &[(2, 1, 0, Default::default())],
      fee: 0,
      ..default()
    };
    let txid = context.core.broadcast_tx(no_value_input);
    context.mine_blocks(1);

    assert_eq!(
      context.index.list(OutPoint::new(txid, 0)).unwrap().unwrap(),
      &[],
    );
  }

  #[test]
  fn list_spent_output() {
    let context = Context::builder().arg("--index-sats").build();
    context.mine_blocks(1);
    context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      fee: 0,
      ..default()
    });
    context.mine_blocks(1);
    let txid = context.core.tx(1, 0).txid();
    assert_matches!(context.index.list(OutPoint::new(txid, 0)).unwrap(), None);
  }

  #[test]
  fn list_unknown_output() {
    let context = Context::builder().arg("--index-sats").build();

    assert_eq!(
      context
        .index
        .list(
          "0000000000000000000000000000000000000000000000000000000000000000:0"
            .parse()
            .unwrap()
        )
        .unwrap(),
      None
    );
  }

  #[test]
  fn find_first_sat() {
    let context = Context::builder().arg("--index-sats").build();
    assert_eq!(
      context.index.find(Sat(0)).unwrap().unwrap(),
      SatPoint {
        outpoint: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b:0"
          .parse()
          .unwrap(),
        offset: 0,
      }
    )
  }

  #[test]
  fn find_second_sat() {
    let context = Context::builder().arg("--index-sats").build();
    assert_eq!(
      context.index.find(Sat(1)).unwrap().unwrap(),
      SatPoint {
        outpoint: "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b:0"
          .parse()
          .unwrap(),
        offset: 1,
      }
    )
  }

  #[test]
  fn find_first_sat_of_second_block() {
    let context = Context::builder().arg("--index-sats").build();
    context.mine_blocks(1);
    let tx = context.core.tx(1, 0);
    assert_eq!(
      context.index.find(Sat(50 * COIN_VALUE)).unwrap().unwrap(),
      SatPoint {
        outpoint: OutPoint {
          txid: tx.txid(),
          vout: 0,
        },
        offset: 0,
      }
    )
  }

  #[test]
  fn find_unmined_sat() {
    let context = Context::builder().arg("--index-sats").build();
    assert_eq!(context.index.find(Sat(50 * COIN_VALUE)).unwrap(), None);
  }

  #[test]
  fn find_first_sat_spent_in_second_block() {
    let context = Context::builder().arg("--index-sats").build();
    context.mine_blocks(1);
    let spend_txid = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      fee: 0,
      ..default()
    });
    context.mine_blocks(1);
    assert_eq!(
      context.index.find(Sat(50 * COIN_VALUE)).unwrap().unwrap(),
      SatPoint {
        outpoint: OutPoint::new(spend_txid, 0),
        offset: 0,
      }
    )
  }

  #[test]
  fn inscriptions_are_tracked_correctly() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscriptions_without_sats_are_unbound() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, Default::default())],
        fee: 50 * 100_000_000,
        ..default()
      });

      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: unbound_outpoint(),
          offset: 0,
        },
        None,
      );

      context.mine_blocks(1);

      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(4, 0, 0, Default::default())],
        fee: 50 * 100_000_000,
        ..default()
      });

      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(5, 1, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: unbound_outpoint(),
          offset: 1,
        },
        None,
      );
    }
  }

  #[test]
  fn unaligned_inscriptions_are_tracked_correctly() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      let send_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, Default::default()), (2, 1, 0, Default::default())],
        ..default()
      });

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: send_txid,
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn merged_inscriptions_are_tracked_correctly() {
    for context in Context::configurations() {
      context.mine_blocks(2);

      let first_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let first_inscription_id = InscriptionId {
        txid: first_txid,
        index: 0,
      };

      let second_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, inscription("text/png", [1; 100]).to_witness())],
        ..default()
      });
      let second_inscription_id = InscriptionId {
        txid: second_txid,
        index: 0,
      };

      context.mine_blocks(1);

      let merged_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 1, 0, Default::default()), (3, 2, 0, Default::default())],
        ..default()
      });

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        first_inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: merged_txid,
            vout: 0,
          },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        second_inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: merged_txid,
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(100 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscriptions_that_are_sent_to_second_output_are_are_tracked_correctly() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      let send_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, Default::default()), (2, 1, 0, Default::default())],
        outputs: 2,
        ..default()
      });

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: send_txid,
            vout: 1,
          },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn missing_inputs_are_fetched_from_bitcoin_core() {
    for args in [
      ["--first-inscription-height", "2"].as_slice(),
      ["--first-inscription-height", "2", "--index-sats"].as_slice(),
    ] {
      let context = Context::builder().args(args).build();
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      let send_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, Default::default()), (2, 1, 0, Default::default())],
        ..default()
      });

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: send_txid,
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn one_input_fee_spent_inscriptions_are_tracked_correctly() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, Default::default())],
        fee: 50 * COIN_VALUE,
        ..default()
      });

      let coinbase_tx = context.mine_blocks(1)[0].txdata[0].txid();

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: coinbase_tx,
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn two_input_fee_spent_inscriptions_are_tracked_correctly() {
    for context in Context::configurations() {
      context.mine_blocks(2);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, Default::default()), (3, 1, 0, Default::default())],
        fee: 50 * COIN_VALUE,
        ..default()
      });

      let coinbase_tx = context.mine_blocks(1)[0].txdata[0].txid();

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: coinbase_tx,
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscription_can_be_fee_spent_in_first_transaction() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      let coinbase_tx = context.mine_blocks(1)[0].txdata[0].txid();

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: coinbase_tx,
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn lost_inscriptions() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks_with_subsidy(1, 0);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint::null(),
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn multiple_inscriptions_can_be_lost() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let first_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });
      let first_inscription_id = InscriptionId {
        txid: first_txid,
        index: 0,
      };

      context.mine_blocks_with_subsidy(1, 0);
      context.mine_blocks(1);

      let second_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 0, 0, inscription("text/plain", "hello").to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });
      let second_inscription_id = InscriptionId {
        txid: second_txid,
        index: 0,
      };

      context.mine_blocks_with_subsidy(1, 0);

      context.index.assert_inscription_location(
        first_inscription_id,
        SatPoint {
          outpoint: OutPoint::null(),
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        second_inscription_id,
        SatPoint {
          outpoint: OutPoint::null(),
          offset: 50 * COIN_VALUE,
        },
        Some(150 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn lost_sats_are_tracked_correctly() {
    let context = Context::builder()
      .args(["--index-sats", "--first-inscription-height", "10"])
      .build();
    assert_eq!(context.index.statistic(Statistic::LostSats), 0);

    context.mine_blocks(1);
    assert_eq!(context.index.statistic(Statistic::LostSats), 0);

    context.mine_blocks_with_subsidy(1, 0);
    assert_eq!(
      context.index.statistic(Statistic::LostSats),
      50 * COIN_VALUE
    );

    context.mine_blocks_with_subsidy(1, 0);
    assert_eq!(
      context.index.statistic(Statistic::LostSats),
      100 * COIN_VALUE
    );

    context.mine_blocks(1);
    assert_eq!(
      context.index.statistic(Statistic::LostSats),
      100 * COIN_VALUE
    );
  }

  #[test]
  fn lost_sat_ranges_are_tracked_correctly() {
    let context = Context::builder()
      .args(["--index-sats", "--first-inscription-height", "10"])
      .build();

    let null_ranges = || {
      context
        .index
        .list(OutPoint::null())
        .unwrap()
        .unwrap_or_default()
    };

    assert!(null_ranges().is_empty());

    context.mine_blocks(1);

    assert!(null_ranges().is_empty());

    context.mine_blocks_with_subsidy(1, 0);

    assert_eq!(null_ranges(), [(100 * COIN_VALUE, 150 * COIN_VALUE)]);

    context.mine_blocks_with_subsidy(1, 0);

    assert_eq!(
      null_ranges(),
      [
        (100 * COIN_VALUE, 150 * COIN_VALUE),
        (150 * COIN_VALUE, 200 * COIN_VALUE)
      ]
    );

    context.mine_blocks(1);

    assert_eq!(
      null_ranges(),
      [
        (100 * COIN_VALUE, 150 * COIN_VALUE),
        (150 * COIN_VALUE, 200 * COIN_VALUE)
      ]
    );

    context.mine_blocks_with_subsidy(1, 0);

    assert_eq!(
      null_ranges(),
      [
        (100 * COIN_VALUE, 150 * COIN_VALUE),
        (150 * COIN_VALUE, 200 * COIN_VALUE),
        (250 * COIN_VALUE, 300 * COIN_VALUE)
      ]
    );
  }

  #[test]
  fn lost_inscriptions_get_lost_satpoints() {
    for context in Context::configurations() {
      context.mine_blocks_with_subsidy(1, 0);
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, inscription("text/plain", "hello").to_witness())],
        outputs: 2,
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks(1);

      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 1, 1, Default::default()), (3, 1, 0, Default::default())],
        fee: 50 * COIN_VALUE,
        ..default()
      });
      context.mine_blocks_with_subsidy(1, 0);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint::null(),
          offset: 75 * COIN_VALUE,
        },
        Some(100 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscription_skips_zero_value_first_output_of_inscribe_transaction() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        outputs: 2,
        output_values: &[0, 50 * COIN_VALUE],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 1 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscription_can_be_lost_in_first_transaction() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };
      context.mine_blocks_with_subsidy(1, 0);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint::null(),
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn lost_rare_sats_are_tracked() {
    let context = Context::builder().arg("--index-sats").build();
    context.mine_blocks_with_subsidy(1, 0);
    context.mine_blocks_with_subsidy(1, 0);

    assert_eq!(
      context
        .index
        .rare_sat_satpoint(Sat(50 * COIN_VALUE))
        .unwrap()
        .unwrap(),
      SatPoint {
        outpoint: OutPoint::null(),
        offset: 0,
      },
    );

    assert_eq!(
      context
        .index
        .rare_sat_satpoint(Sat(100 * COIN_VALUE))
        .unwrap()
        .unwrap(),
      SatPoint {
        outpoint: OutPoint::null(),
        offset: 50 * COIN_VALUE,
      },
    );
  }

  #[test]
  fn inscriptions_on_output() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context
          .index
          .get_inscriptions_for_output(OutPoint { txid, vout: 0 })
          .unwrap(),
        []
      );

      context.mine_blocks(1);

      assert_eq!(
        context
          .index
          .get_inscriptions_for_output(OutPoint { txid, vout: 0 })
          .unwrap(),
        [inscription_id]
      );

      let send_id = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, Default::default())],
        ..default()
      });

      context.mine_blocks(1);

      assert_eq!(
        context
          .index
          .get_inscriptions_for_output(OutPoint { txid, vout: 0 })
          .unwrap(),
        []
      );

      assert_eq!(
        context
          .index
          .get_inscriptions_for_output(OutPoint {
            txid: send_id,
            vout: 0,
          })
          .unwrap(),
        [inscription_id]
      );
    }
  }

  #[test]
  fn inscriptions_on_same_sat_after_the_first_are_not_unbound() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let first = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId {
        txid: first,
        index: 0,
      };

      assert_eq!(
        context
          .index
          .get_inscriptions_for_output(OutPoint {
            txid: first,
            vout: 0
          })
          .unwrap(),
        [inscription_id]
      );

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: first,
            vout: 0,
          },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      let second = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let inscription_id = InscriptionId {
        txid: second,
        index: 0,
      };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: second,
            vout: 0,
          },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      assert!(context
        .index
        .get_inscription_by_id(InscriptionId {
          txid: second,
          index: 0
        })
        .unwrap()
        .is_some());

      assert!(context
        .index
        .get_inscription_by_id(InscriptionId {
          txid: second,
          index: 0
        })
        .unwrap()
        .is_some());
    }
  }

  #[test]
  fn get_latest_inscriptions_with_no_more() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      let (inscriptions, more) = context.index.get_inscriptions_paginated(100, 0).unwrap();
      assert_eq!(inscriptions, &[inscription_id]);
      assert!(!more);
    }
  }

  #[test]
  fn get_latest_inscriptions_with_more() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let mut ids = Vec::new();

      for i in 0..101 {
        let txid = context.core.broadcast_tx(TransactionTemplate {
          inputs: &[(i + 1, 0, 0, inscription("text/plain", "hello").to_witness())],
          ..default()
        });
        context.mine_blocks(1);
        ids.push(InscriptionId { txid, index: 0 });
      }

      ids.reverse();
      ids.pop();

      assert_eq!(ids.len(), 100);

      let (inscriptions, more) = context.index.get_inscriptions_paginated(100, 0).unwrap();
      assert_eq!(inscriptions, ids);
      assert!(more);
    }
  }

  #[test]
  fn unrecognized_even_field_inscriptions_are_cursed_and_unbound() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[2],
        b"bar",
        &[4],
        b"ord",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: unbound_outpoint(),
          offset: 0,
        },
        None,
      );

      assert_eq!(context.index.inscription_number(inscription_id), -1);
    }
  }

  #[test]
  fn unrecognized_even_field_inscriptions_are_unbound_after_jubilee() {
    for context in Context::configurations() {
      context.mine_blocks(109);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[2],
        b"bar",
        &[4],
        b"ord",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: unbound_outpoint(),
          offset: 0,
        },
        None,
      );

      assert_eq!(context.index.inscription_number(inscription_id), 0);
    }
  }

  #[test]
  fn inscriptions_are_uncursed_after_jubilee() {
    for context in Context::configurations() {
      context.mine_blocks(108);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[1],
        b"text/plain;charset=utf-8",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness.clone())],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.core.height(), 109);

      assert_eq!(context.index.inscription_number(inscription_id), -1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.core.height(), 110);

      assert_eq!(context.index.inscription_number(inscription_id), 0);
    }
  }

  #[test]
  fn duplicate_field_inscriptions_are_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[1],
        b"text/plain;charset=utf-8",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.index.inscription_number(inscription_id), -1);
    }
  }

  #[test]
  fn incomplete_field_inscriptions_are_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let witness = envelope(&[b"ord", &[1]]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.index.inscription_number(inscription_id), -1);
    }
  }

  #[test]
  fn inscriptions_with_pushnum_opcodes_are_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([])
        .push_opcode(opcodes::all::OP_PUSHNUM_1)
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.index.inscription_number(inscription_id), -1);
    }
  }

  #[test]
  fn inscriptions_with_stutter_are_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([])
        .push_opcode(opcodes::all::OP_PUSHNUM_1)
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.index.inscription_number(inscription_id), -1);
    }
  }

  // https://github.com/ordinals/ord/issues/2062
  #[test]
  fn zero_value_transaction_inscription_not_cursed_but_unbound() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, Default::default())],
        fee: 50 * 100_000_000,
        ..default()
      });

      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: unbound_outpoint(),
          offset: 0,
        },
        None,
      );

      assert_eq!(context.index.inscription_number(inscription_id), 0);
    }
  }

  #[test]
  fn transaction_with_inscription_inside_zero_value_2nd_input_should_be_unbound_and_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      // create zero value input
      context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, Default::default())],
        fee: 50 * 100_000_000,
        ..default()
      });

      context.mine_blocks(1);

      let witness = inscription("text/plain", "hello").to_witness();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, witness.clone()), (2, 1, 0, witness.clone())],
        ..default()
      });

      let second_inscription_id = InscriptionId { txid, index: 1 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        second_inscription_id,
        SatPoint {
          outpoint: unbound_outpoint(),
          offset: 0,
        },
        None,
      );

      assert_eq!(context.index.inscription_number(second_inscription_id), -1);
    }
  }

  #[test]
  fn multiple_inscriptions_in_same_tx_all_but_first_input_are_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);
      context.mine_blocks(1);
      context.mine_blocks(1);

      let witness = envelope(&[b"ord", &[1], b"text/plain;charset=utf-8", &[], b"bar"]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (1, 0, 0, witness.clone()),
          (2, 0, 0, witness.clone()),
          (3, 0, 0, witness.clone()),
        ],
        ..default()
      });

      let first = InscriptionId { txid, index: 0 };
      let second = InscriptionId { txid, index: 1 };
      let third = InscriptionId { txid, index: 2 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        first,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        second,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 50 * COIN_VALUE,
        },
        Some(100 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        third,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 100 * COIN_VALUE,
        },
        Some(150 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(first), 0);
      assert_eq!(context.index.inscription_number(second), -1);
      assert_eq!(context.index.inscription_number(third), -2);
    }
  }

  #[test]
  fn multiple_inscriptions_same_input_are_cursed_reinscriptions() {
    for context in Context::configurations() {
      context.core.mine_blocks(1);

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"foo")
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"bar")
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"qix")
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let first = InscriptionId { txid, index: 0 };
      let second = InscriptionId { txid, index: 1 };
      let third = InscriptionId { txid, index: 2 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        first,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        second,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        third,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(first), 0);
      assert_eq!(context.index.inscription_number(second), -1);
      assert_eq!(context.index.inscription_number(third), -2);
    }
  }

  #[test]
  fn multiple_inscriptions_different_inputs_and_same_inputs() {
    for context in Context::configurations() {
      context.core.mine_blocks(1);
      context.core.mine_blocks(1);
      context.core.mine_blocks(1);

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"foo")
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"bar")
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"qix")
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (1, 0, 0, witness.clone()),
          (2, 0, 0, witness.clone()),
          (3, 0, 0, witness.clone()),
        ],
        ..default()
      });

      let first = InscriptionId { txid, index: 0 }; // normal
      let second = InscriptionId { txid, index: 1 }; // cursed reinscription
      let fourth = InscriptionId { txid, index: 3 }; // cursed but bound
      let ninth = InscriptionId { txid, index: 8 }; // cursed reinscription

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        first,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        second,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        fourth,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 50 * COIN_VALUE,
        },
        Some(100 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        ninth,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 100 * COIN_VALUE,
        },
        Some(150 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(first), 0);

      assert_eq!(
        context
          .index
          .get_inscription_id_by_inscription_number(-3)
          .unwrap()
          .unwrap(),
        fourth
      );

      assert_eq!(context.index.inscription_number(fourth), -3);

      assert_eq!(context.index.inscription_number(ninth), -8);
    }
  }

  #[test]
  fn inscription_fee_distributed_evenly() {
    for context in Context::configurations() {
      context.core.mine_blocks(1);

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"foo")
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"bar")
        .push_opcode(opcodes::all::OP_ENDIF)
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([1])
        .push_slice(b"text/plain;charset=utf-8")
        .push_slice([])
        .push_slice(b"qix")
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        fee: 33,
        ..default()
      });

      let first = InscriptionId { txid, index: 0 };
      let second = InscriptionId { txid, index: 1 };

      context.mine_blocks(1);

      assert_eq!(
        context
          .index
          .get_inscription_entry(first)
          .unwrap()
          .unwrap()
          .fee,
        11
      );

      assert_eq!(
        context
          .index
          .get_inscription_entry(second)
          .unwrap()
          .unwrap()
          .fee,
        11
      );
    }
  }

  #[test]
  fn reinscription_on_cursed_inscription_is_not_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);
      context.mine_blocks(1);

      let witness = envelope(&[b"ord", &[1], b"text/plain;charset=utf-8", &[], b"bar"]);

      let cursed_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness.clone()), (2, 0, 0, witness.clone())],
        outputs: 2,
        ..default()
      });

      let cursed = InscriptionId {
        txid: cursed_txid,
        index: 1,
      };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        cursed,
        SatPoint {
          outpoint: OutPoint {
            txid: cursed_txid,
            vout: 1,
          },
          offset: 0,
        },
        Some(100 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(cursed), -1);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        b"reinscription on cursed",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 1, 1, witness)],
        ..default()
      });

      let reinscription_on_cursed = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        reinscription_on_cursed,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(100 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(reinscription_on_cursed), 1);
    }
  }

  #[test]
  fn second_reinscription_on_cursed_inscription_is_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);
      context.mine_blocks(1);

      let witness = envelope(&[b"ord", &[1], b"text/plain;charset=utf-8", &[], b"bar"]);

      let cursed_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness.clone()), (2, 0, 0, witness.clone())],
        outputs: 2,
        ..default()
      });

      let cursed = InscriptionId {
        txid: cursed_txid,
        index: 1,
      };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        cursed,
        SatPoint {
          outpoint: OutPoint {
            txid: cursed_txid,
            vout: 1,
          },
          offset: 0,
        },
        Some(100 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(cursed), -1);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        b"reinscription on cursed",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 1, 1, witness)],
        ..default()
      });

      let reinscription_on_cursed = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        reinscription_on_cursed,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(100 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(reinscription_on_cursed), 1);

      let witness = envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        b"second reinscription on cursed",
      ]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(4, 1, 0, witness)],
        ..default()
      });

      let second_reinscription_on_cursed = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      context.index.assert_inscription_location(
        second_reinscription_on_cursed,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(100 * COIN_VALUE),
      );

      assert_eq!(
        context
          .index
          .inscription_number(second_reinscription_on_cursed),
        -2
      );

      assert_eq!(
        vec![
          cursed,
          reinscription_on_cursed,
          second_reinscription_on_cursed
        ],
        context
          .index
          .get_inscriptions_on_output_with_satpoints(OutPoint { txid, vout: 0 })
          .unwrap()
          .iter()
          .map(|(_satpoint, inscription_id)| *inscription_id)
          .collect::<Vec<InscriptionId>>()
      )
    }
  }

  #[test]
  fn reinscriptions_on_output_correctly_ordered_and_transferred() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          1,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });

      let first = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          1,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });

      let second = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);
      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          3,
          1,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });

      let third = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      let location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      assert_eq!(
        vec![(location, first), (location, second), (location, third)],
        context
          .index
          .get_inscriptions_on_output_with_satpoints(OutPoint { txid, vout: 0 })
          .unwrap()
      )
    }
  }

  #[test]
  fn reinscriptions_are_ordered_correctly_for_many_outpoints() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let mut inscription_ids = Vec::new();
      for i in 1..=21 {
        let txid = context.core.broadcast_tx(TransactionTemplate {
          inputs: &[(
            i,
            if i == 1 { 0 } else { 1 },
            0,
            inscription("text/plain;charset=utf-8", format!("hello {i}")).to_witness(),
          )], // for the first inscription use coinbase, otherwise use the previous tx
          ..default()
        });

        inscription_ids.push(InscriptionId { txid, index: 0 });

        context.mine_blocks(1);
      }

      let final_txid = inscription_ids.last().unwrap().txid;
      let location = SatPoint {
        outpoint: OutPoint {
          txid: final_txid,
          vout: 0,
        },
        offset: 0,
      };

      let expected_result = inscription_ids
        .iter()
        .map(|id| (location, *id))
        .collect::<Vec<(SatPoint, InscriptionId)>>();

      assert_eq!(
        expected_result,
        context
          .index
          .get_inscriptions_on_output_with_satpoints(OutPoint {
            txid: final_txid,
            vout: 0
          })
          .unwrap()
      )
    }
  }

  #[test]
  fn recover_from_reorg() {
    for mut context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          1,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });
      let first_id = InscriptionId { txid, index: 0 };
      let first_location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      context.mine_blocks(6);

      context
        .index
        .assert_inscription_location(first_id, first_location, Some(50 * COIN_VALUE));

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });
      let second_id = InscriptionId { txid, index: 0 };
      let second_location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      context.mine_blocks(1);

      context
        .index
        .assert_inscription_location(second_id, second_location, Some(100 * COIN_VALUE));

      context.core.invalidate_tip();
      context.mine_blocks(2);

      context
        .index
        .assert_inscription_location(first_id, first_location, Some(50 * COIN_VALUE));

      assert!(!context.index.inscription_exists(second_id).unwrap());
    }
  }

  #[test]
  fn recover_from_3_block_deep_and_consecutive_reorg() {
    for mut context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          1,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });
      let first_id = InscriptionId { txid, index: 0 };
      let first_location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      context.mine_blocks(10);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });
      let second_id = InscriptionId { txid, index: 0 };
      let second_location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      context.mine_blocks(1);

      context
        .index
        .assert_inscription_location(second_id, second_location, Some(100 * COIN_VALUE));

      context.core.invalidate_tip();
      context.core.invalidate_tip();
      context.core.invalidate_tip();

      context.mine_blocks(4);

      assert!(!context.index.inscription_exists(second_id).unwrap());

      context.core.invalidate_tip();

      context.mine_blocks(2);

      context
        .index
        .assert_inscription_location(first_id, first_location, Some(50 * COIN_VALUE));
    }
  }

  #[test]
  fn recover_from_very_unlikely_7_block_deep_reorg() {
    for mut context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          1,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });

      context.mine_blocks(11);

      let first_id = InscriptionId { txid, index: 0 };
      let first_location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          0,
          0,
          inscription("text/plain;charset=utf-8", "hello").to_witness(),
        )],
        ..default()
      });

      let second_id = InscriptionId { txid, index: 0 };
      let second_location = SatPoint {
        outpoint: OutPoint { txid, vout: 0 },
        offset: 0,
      };

      context.mine_blocks(7);

      context
        .index
        .assert_inscription_location(second_id, second_location, Some(100 * COIN_VALUE));

      for _ in 0..7 {
        context.core.invalidate_tip();
      }

      context.mine_blocks(9);

      assert!(!context.index.inscription_exists(second_id).unwrap());

      context
        .index
        .assert_inscription_location(first_id, first_location, Some(50 * COIN_VALUE));
    }
  }

  #[test]
  fn inscription_without_parent_tag_has_no_parent_entry() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert!(context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap()
        .parents
        .is_empty());
    }
  }

  #[test]
  fn inscription_with_parent_tag_without_parent_has_no_parent_entry() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          0,
          0,
          Inscription {
            content_type: Some("text/plain".into()),
            body: Some("hello".into()),
            parents: vec![parent_inscription_id.value()],
            ..default()
          }
          .to_witness(),
        )],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert!(context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap()
        .parents
        .is_empty());
    }
  }

  #[test]
  fn inscription_with_parent_tag_and_parent_has_parent_entry() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          1,
          0,
          Inscription {
            content_type: Some("text/plain".into()),
            body: Some("hello".into()),
            parents: vec![parent_inscription_id.value()],
            ..default()
          }
          .to_witness(),
        )],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn inscription_with_two_parent_tags_and_parents_has_parent_entries() {
    for context in Context::configurations() {
      context.mine_blocks(2);

      let parent_txid_a = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let parent_txid_b = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, inscription("text/plain", "world").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id_a = InscriptionId {
        txid: parent_txid_a,
        index: 0,
      };
      let parent_inscription_id_b = InscriptionId {
        txid: parent_txid_b,
        index: 0,
      };

      let multi_parent_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello".into()),
        parents: vec![
          parent_inscription_id_a.value(),
          parent_inscription_id_b.value(),
        ],
        ..default()
      };
      let multi_parent_witness = multi_parent_inscription.to_witness();

      let revelation_input = (3, 1, 0, multi_parent_witness);

      let parent_b_input = (3, 2, 0, Witness::new());

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[revelation_input, parent_b_input],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id_a, parent_inscription_id_b]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_a)
          .unwrap(),
        vec![inscription_id]
      );
      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_b)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn inscription_with_repeated_parent_tags_and_parents_has_singular_parent_entry() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          1,
          0,
          Inscription {
            content_type: Some("text/plain".into()),
            body: Some("hello".into()),
            parents: vec![parent_inscription_id.value(), parent_inscription_id.value()],
            ..default()
          }
          .to_witness(),
        )],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn inscription_with_distinct_parent_tag_encodings_for_same_parent_has_singular_parent_entry() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let trailing_zero_inscription_id: Vec<u8> = parent_inscription_id
        .value()
        .into_iter()
        .chain(vec![0, 0, 0, 0])
        .collect();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          1,
          0,
          Inscription {
            content_type: Some("text/plain".into()),
            body: Some("hello".into()),
            parents: vec![parent_inscription_id.value(), trailing_zero_inscription_id],
            ..default()
          }
          .to_witness(),
        )],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn inscription_with_three_parent_tags_and_two_parents_has_two_parent_entries() {
    for context in Context::configurations() {
      context.mine_blocks(3);

      let parent_txid_a = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let parent_txid_b = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, inscription("text/plain", "world").to_witness())],
        ..default()
      });
      let parent_txid_c = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 0, 0, inscription("text/plain", "wazzup").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id_a = InscriptionId {
        txid: parent_txid_a,
        index: 0,
      };
      let parent_inscription_id_b = InscriptionId {
        txid: parent_txid_b,
        index: 0,
      };
      let parent_inscription_id_c = InscriptionId {
        txid: parent_txid_c,
        index: 0,
      };

      let multi_parent_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello".into()),
        parents: vec![
          parent_inscription_id_a.value(),
          parent_inscription_id_b.value(),
          parent_inscription_id_c.value(),
        ],
        ..default()
      };
      let multi_parent_witness = multi_parent_inscription.to_witness();

      let revealing_parent_a_input = (4, 1, 0, multi_parent_witness);

      let parent_c_input = (4, 3, 0, Witness::new());

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[revealing_parent_a_input, parent_c_input],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id_a, parent_inscription_id_c]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_a)
          .unwrap(),
        vec![inscription_id]
      );
      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_b)
          .unwrap(),
        Vec::new()
      );
      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_c)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn inscription_with_valid_and_malformed_parent_tags_only_lists_valid_entries() {
    for context in Context::configurations() {
      context.mine_blocks(3);

      let parent_txid_a = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });
      let parent_txid_b = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, inscription("text/plain", "world").to_witness())],
        ..default()
      });
      let parent_txid_c = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 0, 0, inscription("text/plain", "wazzup").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id_a = InscriptionId {
        txid: parent_txid_a,
        index: 0,
      };
      let parent_inscription_id_b = InscriptionId {
        txid: parent_txid_b,
        index: 0,
      };
      let parent_inscription_id_c = InscriptionId {
        txid: parent_txid_c,
        index: 0,
      };

      let malformed_inscription_id_b = parent_inscription_id_b
        .value()
        .into_iter()
        .chain(iter::once(0))
        .collect();

      let multi_parent_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello".into()),
        parents: vec![
          parent_inscription_id_a.value(),
          malformed_inscription_id_b,
          parent_inscription_id_c.value(),
        ],
        ..default()
      };
      let multi_parent_witness = multi_parent_inscription.to_witness();

      let revealing_parent_a_input = (4, 1, 0, multi_parent_witness);
      let parent_b_input = (4, 2, 0, Witness::new());
      let parent_c_input = (4, 3, 0, Witness::new());

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[revealing_parent_a_input, parent_b_input, parent_c_input],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id_a, parent_inscription_id_c]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_a)
          .unwrap(),
        vec![inscription_id]
      );
      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_b)
          .unwrap(),
        Vec::new()
      );
      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id_c)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn parents_can_be_in_preceding_input() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(2);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (2, 1, 0, Default::default()),
          (
            3,
            0,
            0,
            Inscription {
              content_type: Some("text/plain".into()),
              body: Some("hello".into()),
              parents: vec![parent_inscription_id.value()],
              ..default()
            }
            .to_witness(),
          ),
        ],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn parents_can_be_in_following_input() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(2);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (
            3,
            0,
            0,
            Inscription {
              content_type: Some("text/plain".into()),
              body: Some("hello".into()),
              parents: vec![parent_inscription_id.value()],
              ..default()
            }
            .to_witness(),
          ),
          (2, 1, 0, Default::default()),
        ],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert_eq!(
        context.index.get_parents_by_inscription_id(inscription_id),
        vec![parent_inscription_id]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id)
          .unwrap(),
        vec![inscription_id]
      );
    }
  }

  #[test]
  fn inscription_with_invalid_parent_tag_and_parent_has_no_parent_entry() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(
          2,
          1,
          0,
          Inscription {
            content_type: Some("text/plain".into()),
            body: Some("hello".into()),
            parents: vec![parent_inscription_id
              .value()
              .into_iter()
              .chain(iter::once(0))
              .collect()],
            ..default()
          }
          .to_witness(),
        )],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      assert!(context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap()
        .parents
        .is_empty());
    }
  }

  #[test]
  fn inscription_with_pointer() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello".into()),
        pointer: Some(100u64.to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 100,
        },
        Some(50 * COIN_VALUE + 100),
      );
    }
  }

  #[test]
  fn inscription_with_pointer_greater_than_output_value_assigned_default() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello".into()),
        pointer: Some((50 * COIN_VALUE).to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscription_with_pointer_into_fee_ignored_and_assigned_default_location() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello".into()),
        pointer: Some((25 * COIN_VALUE).to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription.to_witness())],
        fee: 25 * COIN_VALUE,
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscription_with_pointer_is_cursed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("pointer-child".into()),
        pointer: Some(0u64.to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(inscription_id), -1);
    }
  }

  #[test]
  fn inscription_with_pointer_to_parent_is_cursed_reinscription() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let parent_txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "parent").to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let parent_inscription_id = InscriptionId {
        txid: parent_txid,
        index: 0,
      };

      let child_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("pointer-child".into()),
        parents: vec![parent_inscription_id.value()],
        pointer: Some(0u64.to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, child_inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let child_inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        parent_inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        child_inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(child_inscription_id), -1);

      assert_eq!(
        context
          .index
          .get_parents_by_inscription_id(child_inscription_id),
        vec![parent_inscription_id]
      );

      assert_eq!(
        context
          .index
          .get_children_by_inscription_id(parent_inscription_id)
          .unwrap(),
        vec![child_inscription_id]
      );
    }
  }

  #[test]
  fn inscriptions_in_same_input_with_pointers_to_same_output() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let builder = script::Builder::new();

      let builder = Inscription {
        pointer: Some(100u64.to_le_bytes().to_vec()),
        ..default()
      }
      .append_reveal_script_to_builder(builder);

      let builder = Inscription {
        pointer: Some(300_000u64.to_le_bytes().to_vec()),
        ..default()
      }
      .append_reveal_script_to_builder(builder);

      let builder = Inscription {
        pointer: Some(1_000_000u64.to_le_bytes().to_vec()),
        ..default()
      }
      .append_reveal_script_to_builder(builder);

      let witness = Witness::from_slice(&[builder.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      context.mine_blocks(1);

      let first = InscriptionId { txid, index: 0 };
      let second = InscriptionId { txid, index: 1 };
      let third = InscriptionId { txid, index: 2 };

      context.index.assert_inscription_location(
        first,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 100,
        },
        Some(50 * COIN_VALUE + 100),
      );

      context.index.assert_inscription_location(
        second,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 300_000,
        },
        Some(50 * COIN_VALUE + 300_000),
      );

      context.index.assert_inscription_location(
        third,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 1_000_000,
        },
        Some(50 * COIN_VALUE + 1_000_000),
      );
    }
  }

  #[test]
  fn inscriptions_in_same_input_with_pointers_to_different_outputs() {
    for context in Context::configurations() {
      context.mine_blocks_with_subsidy(1, 300_000);

      let builder = script::Builder::new();

      let builder = Inscription {
        pointer: Some(100u64.to_le_bytes().to_vec()),
        ..default()
      }
      .append_reveal_script_to_builder(builder);

      let builder = Inscription {
        pointer: Some(100_111u64.to_le_bytes().to_vec()),
        ..default()
      }
      .append_reveal_script_to_builder(builder);

      let builder = Inscription {
        pointer: Some(299_999u64.to_le_bytes().to_vec()),
        ..default()
      }
      .append_reveal_script_to_builder(builder);

      let witness = Witness::from_slice(&[builder.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        outputs: 3,
        ..default()
      });

      context.mine_blocks(1);

      let first = InscriptionId { txid, index: 0 };
      let second = InscriptionId { txid, index: 1 };
      let third = InscriptionId { txid, index: 2 };

      context.index.assert_inscription_location(
        first,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 100,
        },
        Some(50 * COIN_VALUE + 100),
      );

      context.index.assert_inscription_location(
        second,
        SatPoint {
          outpoint: OutPoint { txid, vout: 1 },
          offset: 111,
        },
        Some(50 * COIN_VALUE + 100_111),
      );

      context.index.assert_inscription_location(
        third,
        SatPoint {
          outpoint: OutPoint { txid, vout: 2 },
          offset: 99_999,
        },
        Some(50 * COIN_VALUE + 299_999),
      );
    }
  }

  #[test]
  fn inscriptions_in_different_inputs_with_pointers_to_different_outputs() {
    for context in Context::configurations() {
      context.mine_blocks(3);

      let inscription_for_second_output = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello jupiter".into()),
        pointer: Some((50 * COIN_VALUE).to_le_bytes().to_vec()),
        ..default()
      };

      let inscription_for_third_output = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello mars".into()),
        pointer: Some((100 * COIN_VALUE).to_le_bytes().to_vec()),
        ..default()
      };

      let inscription_for_first_output = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello world".into()),
        pointer: Some(0u64.to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (1, 0, 0, inscription_for_second_output.to_witness()),
          (2, 0, 0, inscription_for_third_output.to_witness()),
          (3, 0, 0, inscription_for_first_output.to_witness()),
        ],
        outputs: 3,
        ..default()
      });

      context.mine_blocks(1);

      let inscription_for_second_output = InscriptionId { txid, index: 0 };
      let inscription_for_third_output = InscriptionId { txid, index: 1 };
      let inscription_for_first_output = InscriptionId { txid, index: 2 };

      context.index.assert_inscription_location(
        inscription_for_second_output,
        SatPoint {
          outpoint: OutPoint { txid, vout: 1 },
          offset: 0,
        },
        Some(100 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        inscription_for_third_output,
        SatPoint {
          outpoint: OutPoint { txid, vout: 2 },
          offset: 0,
        },
        Some(150 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        inscription_for_first_output,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscriptions_in_different_inputs_with_pointers_to_same_output() {
    for context in Context::configurations() {
      context.mine_blocks(3);

      let first_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello jupiter".into()),
        ..default()
      };

      let second_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello mars".into()),
        pointer: Some(1u64.to_le_bytes().to_vec()),
        ..default()
      };

      let third_inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello world".into()),
        pointer: Some(2u64.to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (1, 0, 0, first_inscription.to_witness()),
          (2, 0, 0, second_inscription.to_witness()),
          (3, 0, 0, third_inscription.to_witness()),
        ],
        outputs: 1,
        ..default()
      });

      context.mine_blocks(1);

      let first_inscription_id = InscriptionId { txid, index: 0 };
      let second_inscription_id = InscriptionId { txid, index: 1 };
      let third_inscription_id = InscriptionId { txid, index: 2 };

      context.index.assert_inscription_location(
        first_inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        second_inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 1,
        },
        Some(50 * COIN_VALUE + 1),
      );

      context.index.assert_inscription_location(
        third_inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 2,
        },
        Some(50 * COIN_VALUE + 2),
      );
    }
  }

  #[test]
  fn inscriptions_with_pointers_to_same_sat_one_becomes_cursed_reinscriptions() {
    for context in Context::configurations() {
      context.mine_blocks(2);

      let inscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello jupiter".into()),
        ..default()
      };

      let cursed_reinscription = Inscription {
        content_type: Some("text/plain".into()),
        body: Some("hello mars".into()),
        pointer: Some(0u64.to_le_bytes().to_vec()),
        ..default()
      };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[
          (1, 0, 0, inscription.to_witness()),
          (2, 0, 0, cursed_reinscription.to_witness()),
        ],
        outputs: 2,
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };
      let cursed_reinscription_id = InscriptionId { txid, index: 1 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      context.index.assert_inscription_location(
        cursed_reinscription_id,
        SatPoint {
          outpoint: OutPoint { txid, vout: 0 },
          offset: 0,
        },
        Some(50 * COIN_VALUE),
      );

      assert_eq!(context.index.inscription_number(inscription_id), 0);

      assert_eq!(
        context.index.inscription_number(cursed_reinscription_id),
        -1
      );
    }
  }

  #[test]
  fn inscribe_into_fee() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let inscription = Inscription::default();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription.to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });

      let blocks = context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: blocks[0].txdata[0].txid(),
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn inscribe_into_fee_with_reduced_subsidy() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      let inscription = Inscription::default();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription.to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });

      let blocks = context.mine_blocks_with_subsidy(1, 25 * COIN_VALUE);

      let inscription_id = InscriptionId { txid, index: 0 };

      context.index.assert_inscription_location(
        inscription_id,
        SatPoint {
          outpoint: OutPoint {
            txid: blocks[0].txdata[0].txid(),
            vout: 0,
          },
          offset: 50 * COIN_VALUE,
        },
        Some(50 * COIN_VALUE),
      );
    }
  }

  #[test]
  fn pre_jubilee_first_reinscription_after_cursed_inscription_is_blessed() {
    for context in Context::configurations() {
      context.mine_blocks(1);

      // Before the jubilee, an inscription on a sat using a pushnum opcode is
      // cursed and not vindicated.

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([])
        .push_opcode(opcodes::all::OP_PUSHNUM_1)
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      let entry = context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap();

      assert!(Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Cursed));

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Vindicated));

      let sat = entry.sat;

      assert_eq!(entry.inscription_number, -1);

      // Before the jubilee, reinscription on the same sat is not cursed and
      // not vindicated.

      let inscription = Inscription::default();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 1, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      let entry = context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap();

      assert_eq!(entry.inscription_number, 0);

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Cursed));

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Vindicated));

      assert_eq!(sat, entry.sat);

      // Before the jubilee, a third reinscription on the same sat is cursed
      // and not vindicated.

      let inscription = Inscription::default();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(3, 1, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      let entry = context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap();

      assert!(Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Cursed));

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Vindicated));

      assert_eq!(entry.inscription_number, -2);

      assert_eq!(sat, entry.sat);
    }
  }

  #[test]
  fn post_jubilee_first_reinscription_after_vindicated_inscription_not_vindicated() {
    for context in Context::configurations() {
      context.mine_blocks(110);
      // After the jubilee, an inscription on a sat using a pushnum opcode is
      // vindicated and not cursed.

      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_slice([])
        .push_opcode(opcodes::all::OP_PUSHNUM_1)
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      let witness = Witness::from_slice(&[script.into_bytes(), Vec::new()]);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, witness)],
        ..default()
      });

      let inscription_id = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      let entry = context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap();

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Cursed));

      assert!(Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Vindicated));

      let sat = entry.sat;

      assert_eq!(entry.inscription_number, 0);

      // After the jubilee, a reinscription on the same is not cursed and not
      // vindicated.

      let inscription = Inscription::default();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(111, 1, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      let entry = context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap();

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Cursed));

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Vindicated));

      assert_eq!(entry.inscription_number, 1);

      assert_eq!(sat, entry.sat);

      // After the jubilee, a third reinscription on the same is vindicated and
      // not cursed.

      let inscription = Inscription::default();

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(112, 1, 0, inscription.to_witness())],
        ..default()
      });

      context.mine_blocks(1);

      let inscription_id = InscriptionId { txid, index: 0 };

      let entry = context
        .index
        .get_inscription_entry(inscription_id)
        .unwrap()
        .unwrap();

      assert!(!Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Cursed));

      assert!(Charm::charms(entry.charms)
        .iter()
        .any(|charm| *charm == Charm::Vindicated));

      assert_eq!(entry.inscription_number, 2);

      assert_eq!(sat, entry.sat);
    }
  }

  #[test]
  fn is_output_spent() {
    let context = Context::builder().build();

    assert!(!context.index.is_output_spent(OutPoint::null()).unwrap());
    assert!(!context
      .index
      .is_output_spent(Chain::Mainnet.genesis_coinbase_outpoint())
      .unwrap());

    context.mine_blocks(1);

    assert!(!context
      .index
      .is_output_spent(OutPoint {
        txid: context.core.tx(1, 0).txid(),
        vout: 0,
      })
      .unwrap());

    context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(1, 0, 0, Default::default())],
      ..default()
    });

    context.mine_blocks(1);

    assert!(context
      .index
      .is_output_spent(OutPoint {
        txid: context.core.tx(1, 0).txid(),
        vout: 0,
      })
      .unwrap());
  }

  #[test]
  fn is_output_in_active_chain() {
    let context = Context::builder().build();

    assert!(context
      .index
      .is_output_in_active_chain(OutPoint::null())
      .unwrap());

    assert!(context
      .index
      .is_output_in_active_chain(Chain::Mainnet.genesis_coinbase_outpoint())
      .unwrap());

    context.mine_blocks(1);

    assert!(context
      .index
      .is_output_in_active_chain(OutPoint {
        txid: context.core.tx(1, 0).txid(),
        vout: 0,
      })
      .unwrap());

    assert!(!context
      .index
      .is_output_in_active_chain(OutPoint {
        txid: context.core.tx(1, 0).txid(),
        vout: 1,
      })
      .unwrap());

    assert!(!context
      .index
      .is_output_in_active_chain(OutPoint {
        txid: Txid::all_zeros(),
        vout: 0,
      })
      .unwrap());
  }

  #[test]
  fn output_addresses_are_updated() {
    let context = Context::builder()
      .arg("--index-addresses")
      .arg("--index-sats")
      .build();

    context.mine_blocks(2);

    let txid = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(1, 0, 0, Witness::new()), (2, 0, 0, Witness::new())],
      outputs: 2,
      ..Default::default()
    });

    context.mine_blocks(1);

    let transaction = context.index.get_transaction(txid).unwrap().unwrap();

    let first_address = context
      .index
      .settings
      .chain()
      .address_from_script(&transaction.output[0].script_pubkey)
      .unwrap();

    let first_address_second_output = OutPoint {
      txid: transaction.txid(),
      vout: 1,
    };

    assert_eq!(
      context.index.get_address_info(&first_address).unwrap(),
      [
        OutPoint {
          txid: transaction.txid(),
          vout: 0
        },
        first_address_second_output
      ]
    );

    let txid = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(3, 1, 0, Witness::new())],
      p2tr: true,
      ..Default::default()
    });

    context.mine_blocks(1);

    let transaction = context.index.get_transaction(txid).unwrap().unwrap();

    let second_address = context
      .index
      .settings
      .chain()
      .address_from_script(&transaction.output[0].script_pubkey)
      .unwrap();

    assert_eq!(
      context.index.get_address_info(&first_address).unwrap(),
      [first_address_second_output]
    );

    assert_eq!(
      context.index.get_address_info(&second_address).unwrap(),
      [OutPoint {
        txid: transaction.txid(),
        vout: 0
      }]
    );
  }

  #[test]
  fn fee_spent_inscriptions_are_numbered_last_in_block() {
    for context in Context::configurations() {
      context.mine_blocks(2);

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(1, 0, 0, inscription("text/plain", "hello").to_witness())],
        fee: 50 * COIN_VALUE,
        ..default()
      });

      let a = InscriptionId { txid, index: 0 };

      let txid = context.core.broadcast_tx(TransactionTemplate {
        inputs: &[(2, 0, 0, inscription("text/plain", "hello").to_witness())],
        ..default()
      });

      let b = InscriptionId { txid, index: 0 };

      context.mine_blocks(1);

      assert_eq!(context.index.inscription_number(a), 1);
      assert_eq!(context.index.inscription_number(b), 0);
    }
  }

  #[test]
  fn inscription_event_sender_channel() {
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(1024);
    let context = Context::builder().event_sender(event_sender).build();

    context.mine_blocks(1);

    let inscription = Inscription::default();
    let create_txid = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(1, 0, 0, inscription.to_witness())],
      fee: 0,
      outputs: 1,
      ..default()
    });

    context.mine_blocks(1);

    let inscription_id = InscriptionId {
      txid: create_txid,
      index: 0,
    };
    let create_event = event_receiver.blocking_recv().unwrap();
    let expected_charms = if context.index.index_sats { 513 } else { 0 };
    assert_eq!(
      create_event,
      Event::InscriptionCreated {
        inscription_id,
        location: Some(SatPoint {
          outpoint: OutPoint {
            txid: create_txid,
            vout: 0
          },
          offset: 0
        }),
        sequence_number: 0,
        block_height: 2,
        charms: expected_charms,
        parent_inscription_ids: Vec::new(),
      }
    );

    // Transfer inscription
    let transfer_txid = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(2, 1, 0, Default::default())],
      fee: 0,
      outputs: 1,
      ..default()
    });

    context.mine_blocks(1);

    let transfer_event = event_receiver.blocking_recv().unwrap();
    assert_eq!(
      transfer_event,
      Event::InscriptionTransferred {
        block_height: 3,
        inscription_id,
        new_location: SatPoint {
          outpoint: OutPoint {
            txid: transfer_txid,
            vout: 0
          },
          offset: 0
        },
        old_location: SatPoint {
          outpoint: OutPoint {
            txid: create_txid,
            vout: 0
          },
          offset: 0
        },
        sequence_number: 0,
      }
    );
  }

  #[test]
  fn rune_event_sender_channel() {
    const RUNE: u128 = 99246114928149462;

    let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(1024);
    let context = Context::builder()
      .arg("--index-runes")
      .event_sender(event_sender)
      .build();

    let (txid0, id) = context.etch(
      Runestone {
        etching: Some(Etching {
          rune: Some(Rune(RUNE)),
          terms: Some(Terms {
            amount: Some(1000),
            cap: Some(100),
            ..default()
          }),
          ..default()
        }),
        ..default()
      },
      1,
    );

    context.assert_runes(
      [(
        id,
        RuneEntry {
          block: id.block,
          etching: txid0,
          spaced_rune: SpacedRune {
            rune: Rune(RUNE),
            spacers: 0,
          },
          timestamp: id.block,
          mints: 0,
          terms: Some(Terms {
            amount: Some(1000),
            cap: Some(100),
            ..default()
          }),
          ..default()
        },
      )],
      [],
    );

    assert_eq!(
      event_receiver.blocking_recv().unwrap(),
      Event::RuneEtched {
        block_height: 8,
        txid: txid0,
        rune_id: id,
      }
    );

    let txid1 = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(2, 0, 0, Witness::new())],
      op_return: Some(
        Runestone {
          mint: Some(id),
          ..default()
        }
        .encipher(),
      ),
      ..default()
    });

    context.mine_blocks(1);

    context.assert_runes(
      [(
        id,
        RuneEntry {
          block: id.block,
          etching: txid0,
          terms: Some(Terms {
            amount: Some(1000),
            cap: Some(100),
            ..default()
          }),
          mints: 1,
          spaced_rune: SpacedRune {
            rune: Rune(RUNE),
            spacers: 0,
          },
          premine: 0,
          timestamp: id.block,
          ..default()
        },
      )],
      [(
        OutPoint {
          txid: txid1,
          vout: 0,
        },
        vec![(id, 1000)],
      )],
    );

    assert_eq!(
      event_receiver.blocking_recv().unwrap(),
      Event::RuneMinted {
        block_height: 9,
        txid: txid1,
        rune_id: id,
        amount: 1000,
      }
    );

    let txid2 = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(9, 1, 0, Witness::new())],
      op_return: Some(
        Runestone {
          edicts: vec![Edict {
            id,
            amount: 1000,
            output: 0,
          }],
          ..Default::default()
        }
        .encipher(),
      ),
      ..Default::default()
    });

    context.mine_blocks(1);

    context.assert_runes(
      [(
        id,
        RuneEntry {
          block: 8,
          etching: txid0,
          spaced_rune: SpacedRune {
            rune: Rune(RUNE),
            ..default()
          },
          terms: Some(Terms {
            amount: Some(1000),
            cap: Some(100),
            ..Default::default()
          }),
          timestamp: 8,
          mints: 1,
          ..Default::default()
        },
      )],
      [(
        OutPoint {
          txid: txid2,
          vout: 0,
        },
        vec![(id, 1000)],
      )],
    );

    event_receiver.blocking_recv().unwrap();

    pretty_assert_eq!(
      event_receiver.blocking_recv().unwrap(),
      Event::RuneTransferred {
        block_height: 10,
        txid: txid2,
        rune_id: id,
        amount: 1000,
        outpoint: OutPoint {
          txid: txid2,
          vout: 0,
        },
      }
    );

    let txid3 = context.core.broadcast_tx(TransactionTemplate {
      inputs: &[(10, 1, 0, Witness::new())],
      op_return: Some(
        Runestone {
          edicts: vec![Edict {
            id,
            amount: 111,
            output: 0,
          }],
          ..Default::default()
        }
        .encipher(),
      ),
      op_return_index: Some(0),
      ..Default::default()
    });

    context.mine_blocks(1);

    context.assert_runes(
      [(
        id,
        RuneEntry {
          block: 8,
          etching: txid0,
          spaced_rune: SpacedRune {
            rune: Rune(RUNE),
            ..default()
          },
          terms: Some(Terms {
            amount: Some(1000),
            cap: Some(100),
            ..Default::default()
          }),
          timestamp: 8,
          mints: 1,
          burned: 111,
          ..Default::default()
        },
      )],
      [(
        OutPoint {
          txid: txid3,
          vout: 1,
        },
        vec![(id, 889)],
      )],
    );

    event_receiver.blocking_recv().unwrap();

    pretty_assert_eq!(
      event_receiver.blocking_recv().unwrap(),
      Event::RuneBurned {
        block_height: 11,
        txid: txid3,
        amount: 111,
        rune_id: id,
      }
    );
  }

  #[test]
  fn assert_schema_statistic_key_is_zero() {
    // other schema statistic keys may chenge when the schema changes, but for
    // good error messages in older versions, the schema statistic key must be
    // zero
    assert_eq!(Statistic::Schema.key(), 0);
  }

  #[test]
  fn reminder_to_update_utxo_entry_type_name() {
    // This test will break when the schema version is updated, and is a
    // reminder to fix the type name in RocksDB implementation.
    //
    // The type name should be changed from `ord::index::utxo_entry::UtxoValue`
    // to `ord::UtxoEntry`. I think it's probably best if we just name types
    // `ord::NAME`, instead of including the full path, since the full path
    // will change if we reorganize the code.
    assert_eq!(SCHEMA_VERSION, 28);
  }
}

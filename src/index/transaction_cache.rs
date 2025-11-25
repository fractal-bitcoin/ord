use {
  super::*,
  rocksdb::{IteratorMode, WriteBatch, WriteOptions, DB},
  std::collections::{HashMap, HashSet},
};

pub struct TransactionCache {
  statistics_cache: HashMap<Statistic, u64>,

  utxo_new: HashMap<OutPoint, UtxoEntryBuf>,

  pub long_utxo_cache: HashMap<OutPoint, UtxoEntryBuf>,

  inscription_cache: InscriptionCache,

  rune_cache: RuneCache,

  block_header_cache: HashMap<u32, Header>,

  sat_cache: HashMap<u64, SatPoint>,

  timestamp_cache: HashMap<u32, u128>,

  transaction_cache: HashMap<Txid, Vec<u8>>,

  height_to_last_sequence_number_cache: HashMap<u32, u64>,

  sat_to_sequence_number_cache: HashMap<u64, u64>,

  sequence_number_to_rune_id_cache: HashMap<u64, RuneId>,

  pub sequence_number_cache: SequenceNumberCache,

  utxos_to_delete: HashSet<OutPoint>,
  nondust_utxos_to_delete: HashSet<OutPoint>,
  address_index_to_delete: HashMap<Vec<u8>, HashSet<OutPoint>>, // script_pubkey -> outpoints
}

pub struct SequenceNumberCache {
  pub next_inscription_sequence: u64,
  pub next_rune_id: u64,
  pub allocated_sequences: Vec<u64>,
}

pub struct InscriptionCache {
  id_to_sequence: HashMap<InscriptionId, u64>,
  sequence_to_entry: HashMap<u64, InscriptionEntry>,
  number_to_sequence: HashMap<i64, u64>,
  pending_inscriptions: Vec<PendingInscription>,
}

pub struct RuneCache {
  id_to_entry: HashMap<RuneId, RuneEntry>,
  rune_to_id: HashMap<Rune, RuneId>,
  outpoint_to_balances: HashMap<OutPoint, Vec<(RuneId, u128)>>,
  pending_runes: Vec<PendingRune>,
}

pub struct PendingInscription {
  pub inscription_id: InscriptionId,
  pub sequence_number: u64,
  pub entry: InscriptionEntry,
}

pub struct PendingRune {
  pub rune_id: RuneId,
  pub rune: Rune,
  pub entry: RuneEntry,
}

impl SequenceNumberCache {
  pub fn new() -> Self {
    Self {
      next_inscription_sequence: 0,
      next_rune_id: 0,
      allocated_sequences: Vec::new(),
    }
  }

  pub fn initialize_from_database(&mut self, database: &DB) -> Result<()> {
    let sequence_number_to_inscription_entry_cf = database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    let mut iter = database.iterator_cf(sequence_number_to_inscription_entry_cf, IteratorMode::End);
    if let Some(Ok((number_bytes, _))) = iter.next() {
      self.next_inscription_sequence =
        u64::from_be_bytes(number_bytes.as_ref().try_into().unwrap()) + 1;
    }

    let rune_id_to_rune_entry_cf = database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    let mut iter = database.iterator_cf(rune_id_to_rune_entry_cf, IteratorMode::End);
    if let Some(Ok((id_bytes, _))) = iter.next() {
      let rune_id = RuneId::load(id_bytes.to_vec());
      self.next_rune_id = rune_id.tx as u64 + 1;
    }

    Ok(())
  }

  pub fn allocate_sequence_number(&mut self) -> u64 {
    let sequence = self.next_inscription_sequence;
    self.next_inscription_sequence += 1;
    self.allocated_sequences.push(sequence);
    sequence
  }
}

impl InscriptionCache {
  pub fn new() -> Self {
    Self {
      id_to_sequence: HashMap::new(),
      sequence_to_entry: HashMap::new(),
      number_to_sequence: HashMap::new(),
      pending_inscriptions: Vec::new(),
    }
  }
}

impl RuneCache {
  pub fn new() -> Self {
    Self {
      id_to_entry: HashMap::new(),
      rune_to_id: HashMap::new(),
      outpoint_to_balances: HashMap::new(),
      pending_runes: Vec::new(),
    }
  }
}

impl TransactionCache {
  pub fn new() -> Self {
    Self {
      statistics_cache: HashMap::new(),
      utxo_new: HashMap::new(),
      long_utxo_cache: HashMap::new(),
      inscription_cache: InscriptionCache::new(),
      rune_cache: RuneCache::new(),
      block_header_cache: HashMap::new(),
      sat_cache: HashMap::new(),
      timestamp_cache: HashMap::new(),
      transaction_cache: HashMap::new(),
      height_to_last_sequence_number_cache: HashMap::new(),
      sat_to_sequence_number_cache: HashMap::new(),
      sequence_number_to_rune_id_cache: HashMap::new(),
      sequence_number_cache: SequenceNumberCache::new(),
      utxos_to_delete: HashSet::new(),
      nondust_utxos_to_delete: HashSet::new(),
      address_index_to_delete: HashMap::new(),
    }
  }

  pub fn initialize(&mut self, database: &DB) -> Result<()> {
    self
      .sequence_number_cache
      .initialize_from_database(database)?;
    Ok(())
  }

  #[inline(never)]
  pub fn get_utxo_entry(&self, database: &DB, outpoint: &OutPoint) -> Result<Option<UtxoEntryBuf>> {
    let outpoint_to_utxo_entry_cf = database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
    if let Some(entry_bytes) = database.get_cf(outpoint_to_utxo_entry_cf, &outpoint.store())? {
      let entry = UtxoEntry::ref_cast(entry_bytes.as_ref()).to_buf();
      return Ok(Some(entry));
    }

    Ok(None)
  }

  #[inline(never)]
  pub fn get_nondust_utxo_entry(
    &self,
    database: &DB,
    outpoint: &OutPoint,
  ) -> Result<Option<UtxoEntryBuf>> {
    let outpoint_to_nondust_utxo_entry_cf = database
      .cf_handle(CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY)
      .unwrap();
    if let Some(entry_bytes) =
      database.get_cf(outpoint_to_nondust_utxo_entry_cf, &outpoint.store())?
    {
      let entry = UtxoEntry::ref_cast(entry_bytes.as_ref()).to_buf();
      return Ok(Some(entry));
    }

    Ok(None)
  }

  pub fn add_utxo_entry(&mut self, outpoint: OutPoint, utxo_entry: UtxoEntryBuf) {
    if !utxo_entry.dust() {
      self.long_utxo_cache.insert(outpoint, utxo_entry);
    } else {
      self.utxo_new.insert(outpoint, utxo_entry);
    }
  }

  pub fn mark_utxo_for_deletion(&mut self, outpoint: OutPoint) {
    self.utxos_to_delete.insert(outpoint);
  }

  pub fn mark_nondust_utxo_for_deletion(&mut self, outpoint: OutPoint) {
    self.nondust_utxos_to_delete.insert(outpoint);
  }

  pub fn mark_address_index_for_deletion(&mut self, script_pubkey: Vec<u8>, outpoint: OutPoint) {
    self
      .address_index_to_delete
      .entry(script_pubkey)
      .or_insert_with(HashSet::new)
      .insert(outpoint);
  }

  #[inline(never)]
  pub fn get_inscription_sequence(
    &self,
    database: &DB,
    inscription_id: &InscriptionId,
  ) -> Result<Option<u64>> {
    if let Some(&sequence_number) = self.inscription_cache.id_to_sequence.get(inscription_id) {
      return Ok(Some(sequence_number));
    }

    let inscription_id_to_sequence_number_cf = database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    if let Some(sequence_bytes) = database.get_cf(
      inscription_id_to_sequence_number_cf,
      &inscription_id.store(),
    )? {
      let sequence_number = u64::from_be_bytes(sequence_bytes.try_into().unwrap());
      return Ok(Some(sequence_number));
    }

    Ok(None)
  }

  #[inline(never)]
  pub fn get_inscription_entry(
    &self,
    database: &DB,
    sequence_number: &u64,
  ) -> Result<Option<InscriptionEntry>> {
    if let Some(entry) = self
      .inscription_cache
      .sequence_to_entry
      .get(&sequence_number)
    {
      return Ok(Some(entry.clone()));
    }

    let sequence_number_to_inscription_entry_cf = database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    if let Some(entry_bytes) = database.get_cf(
      sequence_number_to_inscription_entry_cf,
      &sequence_number.to_be_bytes(),
    )? {
      let entry = InscriptionEntry::load(entry_bytes);
      return Ok(Some(entry));
    }

    Ok(None)
  }

  pub fn create_inscription(
    &mut self,
    inscription_id: InscriptionId,
    entry: InscriptionEntry,
    sequence_number: u64,
  ) -> u64 {
    self
      .inscription_cache
      .id_to_sequence
      .insert(inscription_id, sequence_number);
    self
      .inscription_cache
      .sequence_to_entry
      .insert(sequence_number, entry.clone());
    self
      .inscription_cache
      .number_to_sequence
      .insert(entry.inscription_number, sequence_number);

    self
      .inscription_cache
      .pending_inscriptions
      .push(PendingInscription {
        inscription_id,
        sequence_number,
        entry,
      });

    sequence_number
  }

  pub fn update_inscription_entry(&mut self, sequence_number: u64, entry: InscriptionEntry) {
    self
      .inscription_cache
      .sequence_to_entry
      .insert(sequence_number, entry);
  }

  pub fn get_statistic(&self, database: &DB, statistic: Statistic) -> Result<u64> {
    if let Some(&value) = self.statistics_cache.get(&statistic) {
      return Ok(value);
    }

    let statistic_to_count_cf = database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap();
    let value = database
      .get_cf(statistic_to_count_cf, &statistic.key().to_be_bytes())?
      .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
      .unwrap_or(0);

    Ok(value)
  }

  pub fn update_statistic(&mut self, statistic: Statistic, value: u64) {
    self.statistics_cache.insert(statistic, value);
  }

  #[inline(never)]
  pub fn get_rune_entry(&self, database: &DB, rune_id: &RuneId) -> Result<Option<RuneEntry>> {
    if let Some(entry) = self.rune_cache.id_to_entry.get(rune_id) {
      return Ok(Some(*entry));
    }

    let rune_id_to_rune_entry_cf = database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    if let Some(entry_bytes) = database.get_cf(rune_id_to_rune_entry_cf, &rune_id.store())? {
      let entry = RuneEntry::load(entry_bytes);
      return Ok(Some(entry));
    }

    Ok(None)
  }

  pub fn create_rune(&mut self, rune_id: RuneId, rune: Rune, entry: RuneEntry) {
    self.rune_cache.id_to_entry.insert(rune_id, entry);
    self.rune_cache.rune_to_id.insert(rune, rune_id);

    self.rune_cache.pending_runes.push(PendingRune {
      rune_id,
      rune,
      entry,
    });
  }

  pub fn update_rune_entry(&mut self, rune_id: RuneId, entry: RuneEntry) {
    self.rune_cache.id_to_entry.insert(rune_id, entry);
  }

  pub fn add_rune_balances(&mut self, outpoint: OutPoint, balances: Vec<(RuneId, u128)>) {
    self
      .rune_cache
      .outpoint_to_balances
      .insert(outpoint, balances);
  }

  #[inline(never)]
  pub fn get_rune_balances(
    &self,
    database: &DB,
    outpoint: &OutPoint,
  ) -> Result<Option<Vec<(RuneId, u128)>>> {
    if let Some(balances) = self.rune_cache.outpoint_to_balances.get(outpoint) {
      return Ok(Some(balances.clone()));
    }

    let outpoint_to_rune_balances_cf = database.cf_handle(CF_OUTPOINT_TO_RUNE_BALANCES).unwrap();
    if let Some(guard) = database.get_cf(outpoint_to_rune_balances_cf, &outpoint.store())? {
      let buffer = guard;
      let mut balances = Vec::new();
      let mut i = 0;
      while i < buffer.len() {
        let ((id, balance), len) = Index::decode_rune_balance(&buffer[i..]).unwrap();
        i += len;
        balances.push((id, balance));
      }
      return Ok(Some(balances));
    }

    Ok(None)
  }

  pub fn allocate_sequence_number(&mut self) -> u64 {
    self.sequence_number_cache.allocate_sequence_number()
  }

  pub fn commit_with_extra_data(
    &mut self,
    database: &DB,
    write_options: &WriteOptions,
    address_index_data: &[(Vec<u8>, OutPoint)], // (script_pubkey, outpoint)
    inscription_index_data: &[(u64, SatPoint)], // (sequence_number, satpoint)
    last_commit: bool,
  ) -> Result<()> {
    self.commit_basic_data(database, write_options, last_commit)?;

    if !address_index_data.is_empty() {
      self.commit_address_index_data(database, write_options, address_index_data)?;
    }

    if !inscription_index_data.is_empty() {
      self.commit_inscription_index_data(database, write_options, inscription_index_data)?;
    }

    self.commit_inscription_data(database, write_options)?;

    self.commit_rune_data(database, write_options)?;

    self.clear(last_commit);

    Ok(())
  }

  fn commit_address_index_data(
    &self,
    database: &DB,
    write_options: &WriteOptions,
    address_index_data: &[(Vec<u8>, OutPoint)],
  ) -> Result<()> {
    let mut batch = WriteBatch::default();
    let script_pubkey_to_outpoint_cf = database.cf_handle(CF_SCRIPT_PUBKEY_TO_OUTPOINT).unwrap();

    for (script_pubkey, outpoint) in address_index_data {
      let mut key = Vec::with_capacity(script_pubkey.len() + 36);
      key.extend_from_slice(script_pubkey);
      let outpoint_bytes = outpoint.store();
      key.extend_from_slice(&outpoint_bytes);
      batch.put_cf(script_pubkey_to_outpoint_cf, &key, &[]);
    }

    for (script_pubkey, outpoints) in &self.address_index_to_delete {
      for outpoint in outpoints {
        let mut key = Vec::with_capacity(script_pubkey.len() + 36);
        key.extend_from_slice(script_pubkey);
        let outpoint_bytes = outpoint.store();
        key.extend_from_slice(&outpoint_bytes);
        batch.delete_cf(script_pubkey_to_outpoint_cf, &key);
      }
    }

    database.write_opt(batch, write_options)?;
    Ok(())
  }

  fn commit_inscription_index_data(
    &self,
    database: &DB,
    write_options: &WriteOptions,
    inscription_index_data: &[(u64, SatPoint)],
  ) -> Result<()> {
    if inscription_index_data.is_empty() {
      return Ok(());
    }

    let mut batch = WriteBatch::default();
    let sequence_number_to_satpoint_cf =
      database.cf_handle(CF_SEQUENCE_NUMBER_TO_SATPOINT).unwrap();

    for (sequence_number, satpoint) in inscription_index_data {
      batch.put_cf(
        sequence_number_to_satpoint_cf,
        &sequence_number.to_be_bytes(),
        &satpoint.store(),
      );
    }

    database.write_opt(batch, write_options)?;
    Ok(())
  }

  fn commit_basic_data(
    &mut self,
    database: &DB,
    write_options: &WriteOptions,
    last_commit: bool,
  ) -> Result<()> {
    let mut batch = WriteBatch::default();

    let outpoint_to_utxo_entry_cf = database.cf_handle(CF_OUTPOINT_TO_UTXO_ENTRY).unwrap();
    for (outpoint, utxo_entry) in &self.utxo_new {
      batch.put_cf(
        outpoint_to_utxo_entry_cf,
        &outpoint.store(),
        utxo_entry.as_bytes(),
      );
    }

    let outpoint_to_nondust_utxo_entry_cf = database
      .cf_handle(CF_OUTPOINT_TO_NONDUST_UTXO_ENTRY)
      .unwrap();
    if last_commit {
      for (outpoint, utxo_entry) in &self.long_utxo_cache {
        batch.put_cf(
          outpoint_to_nondust_utxo_entry_cf,
          &outpoint.store(),
          utxo_entry.as_bytes(),
        );
      }
    }

    for outpoint in &self.utxos_to_delete {
      batch.delete_cf(outpoint_to_utxo_entry_cf, &outpoint.store());
    }

    for outpoint in &self.nondust_utxos_to_delete {
      batch.delete_cf(outpoint_to_nondust_utxo_entry_cf, &outpoint.store());
    }

    let statistic_to_count_cf = database.cf_handle(CF_STATISTIC_TO_COUNT).unwrap();
    for (statistic, value) in &self.statistics_cache {
      batch.put_cf(
        statistic_to_count_cf,
        &statistic.key().to_be_bytes(),
        &value.to_be_bytes(),
      );
    }

    let height_to_block_header_cf = database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
    for (height, header) in &self.block_header_cache {
      batch.put_cf(
        height_to_block_header_cf,
        &height.to_be_bytes(),
        &header.store(),
      );
    }

    let sat_to_satpoint_cf = database.cf_handle(CF_SAT_TO_SATPOINT).unwrap();
    for (sat, satpoint) in &self.sat_cache {
      batch.put_cf(sat_to_satpoint_cf, &sat.to_be_bytes(), &satpoint.store());
    }

    let write_transaction_starting_block_count_to_timestamp_cf = database
      .cf_handle(CF_WRITE_TRANSACTION_STARTING_BLOCK_COUNT_TO_TIMESTAMP)
      .unwrap();
    for (height, timestamp) in &self.timestamp_cache {
      batch.put_cf(
        write_transaction_starting_block_count_to_timestamp_cf,
        &height.to_be_bytes(),
        &timestamp.to_be_bytes(),
      );
    }

    let transaction_id_to_transaction_cf = database
      .cf_handle(CF_TRANSACTION_ID_TO_TRANSACTION)
      .unwrap();
    for (txid, data) in &self.transaction_cache {
      batch.put_cf(transaction_id_to_transaction_cf, &txid.store(), data);
    }

    let height_to_last_sequence_number_cf = database
      .cf_handle(CF_HEIGHT_TO_LAST_SEQUENCE_NUMBER)
      .unwrap();
    for (height, sequence_number) in &self.height_to_last_sequence_number_cache {
      batch.put_cf(
        height_to_last_sequence_number_cf,
        &height.to_be_bytes(),
        &sequence_number.to_be_bytes(),
      );
    }

    let sat_to_sequence_number_cf = database.cf_handle(CF_SAT_TO_SEQUENCE_NUMBER).unwrap();
    for (sat, sequence_number) in &self.sat_to_sequence_number_cache {
      batch.put_cf(
        sat_to_sequence_number_cf,
        &sat.to_be_bytes(),
        &sequence_number.to_be_bytes(),
      );
    }

    let sequence_number_to_rune_id_cf = database.cf_handle(CF_SEQUENCE_NUMBER_TO_RUNE_ID).unwrap();
    for (sequence_number, rune_id) in &self.sequence_number_to_rune_id_cache {
      batch.put_cf(
        sequence_number_to_rune_id_cf,
        &sequence_number.to_be_bytes(),
        &rune_id.store(),
      );
    }

    if !batch.is_empty() {
      database.write_opt(batch, write_options)?;
    }

    Ok(())
  }

  fn commit_inscription_data(&mut self, database: &DB, write_options: &WriteOptions) -> Result<()> {
    let mut batch = WriteBatch::default();

    let inscription_id_to_sequence_number_cf = database
      .cf_handle(CF_INSCRIPTION_ID_TO_SEQUENCE_NUMBER)
      .unwrap();
    let sequence_number_to_inscription_entry_cf = database
      .cf_handle(CF_SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)
      .unwrap();
    let inscription_number_to_sequence_number_cf = database
      .cf_handle(CF_INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER)
      .unwrap();
    // let home_inscriptions_cf = database.cf_handle(CF_HOME_INSCRIPTIONS).unwrap();
    let sequence_number_to_children_cf =
      database.cf_handle(CF_SEQUENCE_NUMBER_TO_CHILDREN).unwrap();
    let sat_to_sequence_number_cf = database.cf_handle(CF_SAT_TO_SEQUENCE_NUMBER).unwrap();

    for pending in &self.inscription_cache.pending_inscriptions {
      batch.put_cf(
        inscription_id_to_sequence_number_cf,
        &pending.inscription_id.store(),
        &pending.sequence_number.to_be_bytes(),
      );
      let entry = pending.entry.clone();
      batch.put_cf(
        sequence_number_to_inscription_entry_cf,
        &pending.sequence_number.to_be_bytes(),
        &entry.store(),
      );
      batch.put_cf(
        inscription_number_to_sequence_number_cf,
        &pending.entry.inscription_number.to_be_bytes(),
        &pending.sequence_number.to_be_bytes(),
      );

      // batch.put_cf(
      //   home_inscriptions_cf,
      //   &pending.sequence_number.to_be_bytes(),
      //   &pending.inscription_id.store(),
      // );

      if let Some(sat) = pending.entry.sat {
        batch.put_cf(
          sat_to_sequence_number_cf,
          &sat.n().to_be_bytes(),
          &pending.sequence_number.to_be_bytes(),
        );
      }

      for parent_sequence_number in &pending.entry.parents {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&parent_sequence_number.to_be_bytes());
        key.extend_from_slice(&pending.sequence_number.to_be_bytes());
        batch.put_cf(sequence_number_to_children_cf, &key, &[]);
      }
    }

    // for (sequence_number, entry) in &self.inscription_cache.sequence_to_entry {
    //   if !self
    //     .inscription_cache
    //     .pending_inscriptions
    //     .iter()
    //     .any(|p| p.sequence_number == *sequence_number)
    //   {
    //     batch.put_cf(
    //       sequence_number_to_inscription_entry_cf,
    //       &sequence_number.to_be_bytes(),
    //       &entry.clone().store(),
    //     );
    //   }
    // }

    if !batch.is_empty() {
      database.write_opt(batch, write_options)?;
    }

    Ok(())
  }

  fn commit_rune_data(&mut self, database: &DB, write_options: &WriteOptions) -> Result<()> {
    let mut batch = WriteBatch::default();

    let rune_id_to_rune_entry_cf = database.cf_handle(CF_RUNE_ID_TO_RUNE_ENTRY).unwrap();
    let rune_to_rune_id_cf = database.cf_handle(CF_RUNE_TO_RUNE_ID).unwrap();
    let outpoint_to_rune_balances_cf = database.cf_handle(CF_OUTPOINT_TO_RUNE_BALANCES).unwrap();
    let transaction_id_to_rune_cf = database.cf_handle(CF_TRANSACTION_ID_TO_RUNE).unwrap();

    for pending in &self.rune_cache.pending_runes {
      batch.put_cf(
        rune_id_to_rune_entry_cf,
        &pending.rune_id.store(),
        &pending.entry.store(),
      );
      batch.put_cf(
        rune_to_rune_id_cf,
        &pending.rune.store(),
        &pending.rune_id.store(),
      );
      batch.put_cf(
        transaction_id_to_rune_cf,
        &pending.entry.etching.store(),
        &pending.rune.store(),
      );
    }

    for (rune_id, entry) in &self.rune_cache.id_to_entry {
      if !self
        .rune_cache
        .pending_runes
        .iter()
        .any(|p| p.rune_id == *rune_id)
      {
        batch.put_cf(rune_id_to_rune_entry_cf, &rune_id.store(), &entry.store());
      }
    }

    for (outpoint, balances) in &self.rune_cache.outpoint_to_balances {
      let mut buffer = Vec::new();
      for (rune_id, balance) in balances {
        Index::encode_rune_balance(*rune_id, *balance, &mut buffer);
      }
      if !buffer.is_empty() {
        batch.put_cf(outpoint_to_rune_balances_cf, &outpoint.store(), &buffer);
      }
    }

    if !batch.is_empty() {
      database.write_opt(batch, write_options)?;
    }

    Ok(())
  }

  fn clear(&mut self, last_commit: bool) {
    self.statistics_cache.clear();
    self.utxo_new.clear();
    if last_commit {
      self.long_utxo_cache.clear();
    }
    self.inscription_cache.pending_inscriptions.clear();
    self.inscription_cache.sequence_to_entry.clear();
    self.inscription_cache.number_to_sequence.clear();
    self.inscription_cache.id_to_sequence.clear();
    self.rune_cache.pending_runes.clear();
    self.rune_cache.id_to_entry.clear();
    self.rune_cache.rune_to_id.clear();
    self.rune_cache.outpoint_to_balances.clear();
    self.block_header_cache.clear();
    self.sat_cache.clear();
    self.timestamp_cache.clear();
    self.transaction_cache.clear();
    self.height_to_last_sequence_number_cache.clear();
    self.sat_to_sequence_number_cache.clear();
    self.sequence_number_to_rune_id_cache.clear();
    self.sequence_number_cache.allocated_sequences.clear();
    // self.sequence_number_cache.next_inscription_sequence = 0;
    // self.sequence_number_cache.next_rune_id = 0;
    self.utxos_to_delete.clear();
    self.nondust_utxos_to_delete.clear();
    self.address_index_to_delete.clear();
  }

  pub fn get_next_sequence_number(&self) -> u64 {
    self.sequence_number_cache.next_inscription_sequence
  }

  pub fn add_block_header(&mut self, height: u32, header: Header) {
    self.block_header_cache.insert(height, header);
  }

  pub fn add_write_transaction_timestamp(&mut self, height: u32, timestamp: u128) {
    self.timestamp_cache.insert(height, timestamp);
  }

  pub fn add_transaction_data(&mut self, txid: Txid, data: &[u8]) {
    self.transaction_cache.insert(txid, data.to_vec());
  }

  pub fn get_block_height(&self, database: &DB) -> Result<Option<u32>> {
    let height_to_block_header_cf = database.cf_handle(CF_HEIGHT_TO_BLOCK_HEADER).unwrap();
    let mut iter = database.iterator_cf(height_to_block_header_cf, IteratorMode::End);
    let Some(Ok((height_bytes, _))) = iter.next() else {
      return Ok(None);
    };
    Ok(Some(
      u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap())
        + self.block_header_cache.len() as u32,
    ))
  }

  pub fn get_home_inscription_count(&self, database: &DB) -> Result<u64> {
    let home_inscriptions_cf = database.cf_handle(CF_HOME_INSCRIPTIONS).unwrap();
    let mut count = 0;
    let mut iter = database.iterator_cf(home_inscriptions_cf, IteratorMode::Start);
    while let Some(Ok(_)) = iter.next() {
      count += 1;
    }
    Ok(count)
  }

  pub fn add_sat_satpoint_mapping(&mut self, sat: u64, satpoint: SatPoint) {
    self.sat_cache.insert(sat, satpoint);
  }

  #[inline(never)]
  pub fn get_rune_id_by_rune(&self, database: &DB, rune: &Rune) -> Result<Option<RuneId>> {
    if let Some(rune_id) = self.rune_cache.rune_to_id.get(rune) {
      return Ok(Some(*rune_id));
    }

    let rune_to_id_cf = database.cf_handle(CF_RUNE_TO_RUNE_ID).unwrap();
    if let Some(id_bytes) = database.get_cf(rune_to_id_cf, &rune.store())? {
      Ok(Some(RuneId::load(id_bytes)))
    } else {
      Ok(None)
    }
  }

  pub fn set_height_to_last_sequence_number(&mut self, height: u32, sequence_number: u64) {
    self
      .height_to_last_sequence_number_cache
      .insert(height, sequence_number);
  }

  pub fn set_sat_to_sequence_number(&mut self, sat: u64, sequence_number: u64) {
    self
      .sat_to_sequence_number_cache
      .insert(sat, sequence_number);
  }

  pub fn set_sequence_number_to_rune_id(&mut self, sequence_number: u64, rune_id: RuneId) {
    self
      .sequence_number_to_rune_id_cache
      .insert(sequence_number, rune_id);
  }
}

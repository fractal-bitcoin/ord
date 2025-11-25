use super::*;

pub(crate) trait Entry: Sized {
  type Value;

  fn load(value: Self::Value) -> Self;

  fn store(self) -> Self::Value;

  // Helper method to load from bytes for fixed-size array types
  #[allow(dead_code)]
  fn load_from_fixed_bytes<const N: usize>(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>>
  where
    Self::Value: From<[u8; N]>,
  {
    if bytes.len() != N {
      return Err(format!("Expected {} bytes, got {}", N, bytes.len()).into());
    }
    let mut array = [0u8; N];
    array.copy_from_slice(bytes);
    Ok(Self::load(array.into()))
  }
}

pub(super) type HeaderValue = Vec<u8>;

impl Entry for Header {
  type Value = HeaderValue;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

impl Entry for Rune {
  type Value = Vec<u8>;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct RuneEntry {
  pub block: u64,
  pub burned: u128,
  pub divisibility: u8,
  pub etching: Txid,
  pub mints: u128,
  pub number: u64,
  pub premine: u128,
  pub spaced_rune: SpacedRune,
  pub symbol: Option<char>,
  pub terms: Option<Terms>,
  pub timestamp: u64,
  pub turbo: bool,
}

impl RuneEntry {
  pub fn mintable(&self, height: u64) -> Result<u128, MintError> {
    let Some(terms) = self.terms else {
      return Err(MintError::Unmintable);
    };

    if let Some(start) = self.start() {
      if height < start {
        return Err(MintError::Start(start));
      }
    }

    if let Some(end) = self.end() {
      if height >= end {
        return Err(MintError::End(end));
      }
    }

    let cap = terms.cap.unwrap_or_default();

    if self.mints >= cap {
      return Err(MintError::Cap(cap));
    }

    Ok(terms.amount.unwrap_or_default())
  }

  pub fn supply(&self) -> u128 {
    self.premine
      + self.mints
        * self
          .terms
          .and_then(|terms| terms.amount)
          .unwrap_or_default()
  }

  pub fn max_supply(&self) -> u128 {
    self.premine
      + self.terms.and_then(|terms| terms.cap).unwrap_or_default()
        * self
          .terms
          .and_then(|terms| terms.amount)
          .unwrap_or_default()
  }

  pub fn pile(&self, amount: u128) -> Pile {
    Pile {
      amount,
      divisibility: self.divisibility,
      symbol: self.symbol,
    }
  }

  pub fn start(&self) -> Option<u64> {
    let terms = self.terms?;

    let relative = terms
      .offset
      .0
      .map(|offset| self.block.saturating_add(offset));

    let absolute = terms.height.0;

    relative
      .zip(absolute)
      .map(|(relative, absolute)| relative.max(absolute))
      .or(relative)
      .or(absolute)
  }

  pub fn end(&self) -> Option<u64> {
    let terms = self.terms?;

    let relative = terms
      .offset
      .1
      .map(|offset| self.block.saturating_add(offset));

    let absolute = terms.height.1;

    relative
      .zip(absolute)
      .map(|(relative, absolute)| relative.min(absolute))
      .or(relative)
      .or(absolute)
  }
}

impl Default for RuneEntry {
  fn default() -> Self {
    Self {
      block: 0,
      burned: 0,
      divisibility: 0,
      etching: Txid::all_zeros(),
      mints: 0,
      number: 0,
      premine: 0,
      spaced_rune: SpacedRune::default(),
      symbol: None,
      terms: None,
      timestamp: 0,
      turbo: false,
    }
  }
}

impl Entry for RuneEntry {
  type Value = Vec<u8>;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

impl RuneEntry {
  pub fn load_from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
    Ok(ciborium::from_reader(bytes)?)
  }
}

pub(super) type RuneIdValue = Vec<u8>;

impl Entry for RuneId {
  type Value = RuneIdValue;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct InscriptionEntry {
  pub charms: u16,
  pub fee: u64,
  pub height: u32,
  pub id: InscriptionId,
  pub inscription_number: i64,
  pub parents: Vec<u64>,
  pub sat: Option<Sat>,
  pub sequence_number: u64,
  pub timestamp: u32,
}

pub(crate) type InscriptionEntryValue = Vec<u8>;

impl Entry for InscriptionEntry {
  type Value = InscriptionEntryValue;

  #[rustfmt::skip]
  fn load(data: InscriptionEntryValue) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

#[allow(dead_code)]
pub(crate) type InscriptionIdValue = Vec<u8>;

impl Entry for InscriptionId {
  type Value = Vec<u8>;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

impl InscriptionId {
  pub fn load_from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
    Ok(ciborium::from_reader(bytes)?)
  }
}

pub(super) type OutPointValue = Vec<u8>;

impl Entry for OutPoint {
  type Value = OutPointValue;

  fn load(data: Self::Value) -> Self {
    // 固定大小：txid (32字节) + vout (4字节) = 36字节
    if data.len() != 36 {
      panic!(
        "Invalid OutPoint data length: expected 36 bytes, got {}",
        data.len()
      );
    }
    let txid_bytes: [u8; 32] = data[0..32].try_into().unwrap();
    let vout_bytes: [u8; 4] = data[32..36].try_into().unwrap();
    OutPoint {
      txid: Txid::from_byte_array(txid_bytes),
      vout: u32::from_be_bytes(vout_bytes),
    }
  }

  fn store(self) -> Self::Value {
    // 固定大小：txid (32字节) + vout (4字节) = 36字节
    let mut writer = Vec::with_capacity(36);
    writer.extend_from_slice(&self.txid.to_byte_array());
    writer.extend_from_slice(&self.vout.to_be_bytes());
    writer
  }
}

pub(super) type SatPointValue = Vec<u8>;

impl Entry for SatPoint {
  type Value = SatPointValue;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

pub(super) type SatRange = (u64, u64);

impl Entry for SatRange {
  type Value = Vec<u8>;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

pub(super) type TxidValue = Vec<u8>;

impl Entry for Txid {
  type Value = TxidValue;

  fn load(data: Self::Value) -> Self {
    ciborium::from_reader(&data[..]).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut writer = Vec::new();
    ciborium::into_writer(&self, &mut writer).unwrap();
    writer
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bitcoin::{OutPoint, Txid};

  #[test]
  fn test_outpoint_store_size() {
    // 测试 OutPoint 序列化大小
    let txid_hex = "c7ee58a761f23d68b4a35c16f25e96fe9317a63d8a951eff9773099ba08cb6ad";
    let txid = Txid::from_str(txid_hex).unwrap();

    // 测试不同的 vout 值
    for vout in [0u32, 1, 255, 256, 65535, 65536, u32::MAX] {
      let outpoint = OutPoint::new(txid, vout);
      let serialized = outpoint.store();
      println!("OutPoint with vout={}: {} bytes", vout, serialized.len());
    }
  }

  #[test]
  fn test_sat_range_load_store() {
    let sat_range = (50 * 100_000_000 as u64, 105_000_000 * 100_000_000 as u64) as SatRange;
    let stored_bytes = sat_range.store();
    let loaded_sat_range = SatRange::load(stored_bytes);
    assert_eq!(sat_range, loaded_sat_range);
  }

  #[test]
  fn inscription_entry() {
    let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefi0"
      .parse::<InscriptionId>()
      .unwrap();

    let entry = InscriptionEntry {
      charms: 0,
      fee: 1,
      height: 2,
      id,
      inscription_number: 3,
      parents: vec![4, 5, 6],
      sat: Some(Sat(7)),
      sequence_number: 8,
      timestamp: 9,
    };

    // Test serialization round-trip
    let serialized = entry.clone().store();
    let deserialized = InscriptionEntry::load(serialized);
    assert_eq!(entry, deserialized);
  }

  #[test]
  fn inscription_id_entry() {
    let inscription_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdefi0"
      .parse::<InscriptionId>()
      .unwrap();

    // Test serialization round-trip
    let serialized = inscription_id.store();
    let deserialized = InscriptionId::load(serialized);
    assert_eq!(inscription_id, deserialized);
  }

  #[test]
  fn parent_entry_index() {
    let inscription_id = "0000000000000000000000000000000000000000000000000000000000000000i1"
      .parse::<InscriptionId>()
      .unwrap();

    // Test serialization round-trip
    let serialized = inscription_id.store();
    let deserialized = InscriptionId::load(serialized);
    assert_eq!(inscription_id, deserialized);

    let inscription_id = "0000000000000000000000000000000000000000000000000000000000000000i256"
      .parse::<InscriptionId>()
      .unwrap();

    // Test serialization round-trip
    let serialized = inscription_id.store();
    let deserialized = InscriptionId::load(serialized);
    assert_eq!(inscription_id, deserialized);
  }

  #[test]
  fn rune_entry() {
    let entry = RuneEntry {
      block: 12,
      burned: 1,
      divisibility: 3,
      etching: Txid::from_byte_array([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
        0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
        0x1E, 0x1F,
      ]),
      terms: Some(Terms {
        cap: Some(1),
        height: (Some(2), Some(3)),
        amount: Some(4),
        offset: (Some(5), Some(6)),
      }),
      mints: 11,
      number: 6,
      premine: 12,
      spaced_rune: SpacedRune {
        rune: Rune(7),
        spacers: 8,
      },
      symbol: Some('a'),
      timestamp: 10,
      turbo: true,
    };

    // Test serialization round-trip
    let serialized = entry.clone().store();
    let deserialized = RuneEntry::load(serialized);
    assert_eq!(entry, deserialized);
  }

  #[test]
  fn rune_id_entry() {
    let rune_id = RuneId { block: 1, tx: 2 };

    // Test serialization round-trip
    let serialized = rune_id.store();
    let deserialized = RuneId::load(serialized);
    assert_eq!(rune_id, deserialized);
  }

  #[test]
  fn header() {
    // Create a test header with some data
    let mut header_data = [0u8; 80];
    for i in 0..80 {
      header_data[i] = i as u8;
    }

    // 使用 consensus_decode 来解析 header
    use bitcoin::consensus::Decodable;
    let header = Header::consensus_decode(&mut &header_data[..]).unwrap();

    // Test serialization round-trip
    let serialized = header.store();
    let deserialized = Header::load(serialized);
    assert_eq!(header, deserialized);
  }

  #[test]
  fn mintable_default() {
    assert_eq!(RuneEntry::default().mintable(0), Err(MintError::Unmintable));
  }

  #[test]
  fn mintable_cap() {
    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(0),
      Ok(1000),
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          ..default()
        }),
        mints: 1,
        ..default()
      }
      .mintable(0),
      Err(MintError::Cap(1)),
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: None,
          amount: Some(1000),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(0),
      Err(MintError::Cap(0)),
    );
  }

  #[test]
  fn mintable_offset_start() {
    assert_eq!(
      RuneEntry {
        block: 1,
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          offset: (Some(1), None),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(1),
      Err(MintError::Start(2)),
    );

    assert_eq!(
      RuneEntry {
        block: 1,
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          offset: (Some(1), None),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(2),
      Ok(1000),
    );
  }

  #[test]
  fn mintable_offset_end() {
    assert_eq!(
      RuneEntry {
        block: 1,
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          offset: (None, Some(1)),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(1),
      Ok(1000),
    );

    assert_eq!(
      RuneEntry {
        block: 1,
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          offset: (None, Some(1)),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(2),
      Err(MintError::End(2)),
    );
  }

  #[test]
  fn mintable_height_start() {
    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          height: (Some(1), None),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(0),
      Err(MintError::Start(1)),
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          height: (Some(1), None),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(1),
      Ok(1000),
    );
  }

  #[test]
  fn mintable_height_end() {
    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          height: (None, Some(1)),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(0),
      Ok(1000),
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          cap: Some(1),
          amount: Some(1000),
          height: (None, Some(1)),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .mintable(1),
      Err(MintError::End(1)),
    );
  }

  #[test]
  fn mintable_multiple_terms() {
    let entry = RuneEntry {
      terms: Some(Terms {
        cap: Some(1),
        amount: Some(1000),
        height: (Some(10), Some(20)),
        offset: (Some(0), Some(10)),
      }),
      block: 10,
      mints: 0,
      ..default()
    };

    assert_eq!(entry.mintable(10), Ok(1000));

    {
      let mut entry = entry;
      entry.terms.as_mut().unwrap().cap = None;
      assert_eq!(entry.mintable(10), Err(MintError::Cap(0)));
    }

    {
      let mut entry = entry;
      entry.terms.as_mut().unwrap().height.0 = Some(11);
      assert_eq!(entry.mintable(10), Err(MintError::Start(11)));
    }

    {
      let mut entry = entry;
      entry.terms.as_mut().unwrap().height.1 = Some(10);
      assert_eq!(entry.mintable(10), Err(MintError::End(10)));
    }

    {
      let mut entry = entry;
      entry.terms.as_mut().unwrap().offset.0 = Some(1);
      assert_eq!(entry.mintable(10), Err(MintError::Start(11)));
    }

    {
      let mut entry = entry;
      entry.terms.as_mut().unwrap().offset.1 = Some(0);
      assert_eq!(entry.mintable(10), Err(MintError::End(10)));
    }
  }

  #[test]
  fn supply() {
    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          amount: Some(1000),
          ..default()
        }),
        mints: 0,
        ..default()
      }
      .supply(),
      0
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          amount: Some(1000),
          ..default()
        }),
        mints: 1,
        ..default()
      }
      .supply(),
      1000
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          amount: Some(1000),
          ..default()
        }),
        mints: 0,
        premine: 1,
        ..default()
      }
      .supply(),
      1
    );

    assert_eq!(
      RuneEntry {
        terms: Some(Terms {
          amount: Some(1000),
          ..default()
        }),
        mints: 1,
        premine: 1,
        ..default()
      }
      .supply(),
      1001
    );
  }
}

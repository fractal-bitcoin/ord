use super::*;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct EtchingEntry {
  pub commit: Transaction,
  pub reveal: Transaction,
  pub output: batch::Output,
}

pub(super) type EtchingEntryValue = Vec<u8>;

impl Entry for EtchingEntry {
  type Value = EtchingEntryValue;

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

  #[test]
  fn etching_entry() {
    let commit = Transaction {
      version: 2,
      lock_time: LockTime::ZERO,
      input: vec![TxIn {
        previous_output: OutPoint::null(),
        script_sig: ScriptBuf::new(),
        sequence: Sequence::MAX,
        witness: Witness::new(),
      }],
      output: Vec::new(),
    };

    let reveal = Transaction {
      version: 2,
      lock_time: LockTime::ZERO,
      input: vec![TxIn {
        previous_output: OutPoint::null(),
        script_sig: ScriptBuf::new(),
        sequence: Sequence::default(),
        witness: Witness::new(),
      }],
      output: Vec::new(),
    };

    let txid = Txid::from_byte_array([
      0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
      0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
      0x1E, 0x1F,
    ]);

    let output = batch::Output {
      commit: txid,
      commit_psbt: None,
      inscriptions: Vec::new(),
      parent: None,
      reveal: txid,
      reveal_broadcast: true,
      reveal_psbt: None,
      rune: None,
      total_fees: 0,
    };

    let entry = EtchingEntry {
      commit,
      reveal,
      output,
    };

    // Test serialization round-trip
    let serialized = entry.clone().store();
    let deserialized = EtchingEntry::load(serialized);
    assert_eq!(entry, deserialized);
  }
}

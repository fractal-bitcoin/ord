use super::*;
use rocksdb::{ColumnFamily, IteratorMode};

#[allow(dead_code)]
pub(crate) struct Rtx {
  database: rocksdb::DB,
}

impl Rtx {
  #[allow(dead_code)]
  pub(crate) fn new(database: rocksdb::DB) -> Result<Rtx> {
    Ok(Self { database })
  }

  #[allow(dead_code)]
  pub(crate) fn block_height(&self) -> Result<Option<Height>> {
    let height_to_block_header_cf = self
      .database
      .cf_handle(CF_HEIGHT_TO_BLOCK_HEADER)
      .ok_or_else(|| anyhow!("Column family 'height_to_block_header' not found"))?;
    let mut iter = self
      .database
      .iterator_cf(height_to_block_header_cf, IteratorMode::End);
    if let Some(Ok((height_bytes, _))) = iter.next() {
      let height = u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap());
      Ok(Some(Height(height)))
    } else {
      Ok(None)
    }
  }

  #[allow(dead_code)]
  pub(crate) fn block_count(&self) -> Result<u32> {
    let height_to_block_header_cf = self
      .database
      .cf_handle(CF_HEIGHT_TO_BLOCK_HEADER)
      .ok_or_else(|| anyhow!("Column family 'height_to_block_header' not found"))?;
    let mut iter = self
      .database
      .iterator_cf(height_to_block_header_cf, IteratorMode::End);
    if let Some(Ok((height_bytes, _))) = iter.next() {
      let height = u32::from_be_bytes(height_bytes.as_ref().try_into().unwrap());
      Ok(height + 1)
    } else {
      Ok(0)
    }
  }

  #[allow(dead_code)]
  pub(crate) fn block_hash(&self, height: Option<u32>) -> Result<Option<BlockHash>> {
    let height_to_block_header_cf = self
      .database
      .cf_handle(CF_HEIGHT_TO_BLOCK_HEADER)
      .ok_or_else(|| anyhow!("Column family 'height_to_block_header' not found"))?;
    match height {
      Some(height) => {
        // Get block hash for specific height
        let height_bytes = height.to_be_bytes();
        if let Some(header_bytes) = self
          .database
          .get_cf(height_to_block_header_cf, &height_bytes)?
        {
          let header = Header::load(header_bytes.to_vec());
          Ok(Some(header.block_hash()))
        } else {
          Ok(None)
        }
      }
      None => {
        // Get block hash for the latest height
        let mut iter = self
          .database
          .iterator_cf(height_to_block_header_cf, IteratorMode::End);
        if let Some(Ok((_height_bytes, header_bytes))) = iter.next() {
          let header = Header::load(header_bytes.to_vec());
          Ok(Some(header.block_hash()))
        } else {
          Ok(None)
        }
      }
    }
  }

  /// Get a column family handle by name
  #[allow(dead_code)]
  pub(crate) fn get_cf(&self, name: &str) -> Result<&ColumnFamily> {
    self
      .database
      .cf_handle(name)
      .ok_or_else(|| anyhow!("Column family '{}' not found", name))
  }

  /// Get a value from a specific column family
  #[allow(dead_code)]
  pub(crate) fn get_cf_value<K>(&self, cf: &ColumnFamily, key: &K) -> Result<Option<Vec<u8>>>
  where
    K: AsRef<[u8]>,
  {
    Ok(self.database.get_cf(cf, key.as_ref())?)
  }

  /// Iterate over a column family
  #[allow(dead_code)]
  pub(crate) fn iter_cf(&self, cf: &ColumnFamily, mode: IteratorMode) -> rocksdb::DBIterator {
    self.database.iterator_cf(cf, mode)
  }
}

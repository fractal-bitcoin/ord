use rocksdb::checkpoint::Checkpoint;
use std::fs;
use std::path::Path;
use {super::*, updater::BlockData};

#[derive(Debug, PartialEq)]
pub(crate) enum Error {
  Recoverable { height: u32, depth: u32 },
  Unrecoverable,
}

impl Display for Error {
  fn fmt(&self, f: &mut Formatter) -> fmt::Result {
    match self {
      Self::Recoverable { height, depth } => {
        write!(f, "{depth} block deep reorg detected at height {height}")
      }
      Self::Unrecoverable => write!(f, "unrecoverable reorg detected"),
    }
  }
}

impl std::error::Error for Error {}

const MAX_SAVEPOINTS: u64 = 2;
const SAVEPOINT_INTERVAL: u64 = 10;
const CHAIN_TIP_DISTANCE: u64 = 21;

pub(crate) struct Reorg {}

impl Reorg {
  pub(crate) fn detect_reorg(block: &BlockData, height: u32, index: &Index) -> Result {
    log::info!("detect_reorg enter, height: {:?}", height);
    let bitcoind_prev_blockhash = block.header.prev_blockhash;

    match index.block_hash(height.checked_sub(1))? {
      Some(index_prev_blockhash) if index_prev_blockhash == bitcoind_prev_blockhash => Ok(()),
      Some(index_prev_blockhash) if index_prev_blockhash != bitcoind_prev_blockhash => {
        let max_recoverable_reorg_depth =
          (MAX_SAVEPOINTS - 1) * SAVEPOINT_INTERVAL + (height % SAVEPOINT_INTERVAL as u32) as u64;

        for depth in 1..=max_recoverable_reorg_depth as u32 {
          let depth_u32 = depth;
          let index_block_hash = index.block_hash(height.checked_sub(depth_u32))?;
          let bitcoind_block_hash = index
            .client
            .get_block_hash(u64::from(height.saturating_sub(depth_u32)))
            .into_option()?;

          if index_block_hash == bitcoind_block_hash {
            return Err(anyhow!(reorg::Error::Recoverable {
              height,
              depth: depth_u32
            }));
          }
        }

        Err(anyhow!(reorg::Error::Unrecoverable))
      }
      _ => Ok(()),
    }
  }

  pub(crate) fn is_savepoint_required(index: &Index, height: u32) -> Result<bool> {
    let height = u64::from(height);

    let statistic_to_count = index
      .database
      .cf_handle(CF_STATISTIC_TO_COUNT)
      .ok_or_else(|| anyhow!("Failed to open column family 'statistic_to_count'"))?;

    let last_savepoint_height = index
      .database
      .get_cf(
        statistic_to_count,
        &Statistic::LastSavepointHeight.key().to_be_bytes(),
      )?
      .map(|last_savepoint_height| {
        u64::from(u32::from_be_bytes(
          last_savepoint_height.try_into().unwrap(),
        ))
      })
      .unwrap_or(0);

    let blocks = index.client.get_blockchain_info()?.headers;

    let result = (height < SAVEPOINT_INTERVAL
      || height.saturating_sub(last_savepoint_height) >= SAVEPOINT_INTERVAL)
      && blocks.saturating_sub(height) <= CHAIN_TIP_DISTANCE;

    log::trace!(
      "is_savepoint_required={}: height={}, last_savepoint_height={}, blocks={}",
      result,
      height,
      last_savepoint_height,
      blocks
    );

    Ok(result)
  }

  pub(crate) fn handle_reorg(index: &Index, height: u32, depth: u32) -> Result {
    log::info!("rolling back database after reorg of depth {depth} at height {height}");

    // Place checkpoint directory alongside index.rocksdb
    let checkpoints_dir = index
      .path
      .parent()
      .ok_or_else(|| anyhow!("Cannot get parent directory of index path"))?
      .join("checkpoints");
    if !checkpoints_dir.exists() {
      return Err(anyhow!("No checkpoints found"));
    }

    // Get all checkpoint directories sorted by height
    let mut checkpoint_dirs: Vec<_> = fs::read_dir(&checkpoints_dir)?
      .filter_map(|entry| {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
          let name = path.file_name()?.to_str()?;
          if name.starts_with("checkpoint_") {
            let height_str = &name[11..]; // Skip "checkpoint_" prefix
            height_str.parse::<u32>().ok().map(|height| (height, path))
          } else {
            None
          }
        } else {
          None
        }
      })
      .collect();

    checkpoint_dirs.sort_by_key(|(height, _)| *height);

    // Find the best checkpoint (largest height less than current height - depth)
    let target_checkpoint = checkpoint_dirs
      .iter()
      .rev()
      .find(|(checkpoint_height, _)| *checkpoint_height <= height - depth)
      .ok_or_else(|| anyhow!("No suitable checkpoint found for rollback"))?;

    let (checkpoint_height, checkpoint_path) = target_checkpoint;
    log::info!(
      "restoring checkpoint at height {}, path: {}",
      checkpoint_height,
      checkpoint_path.display()
    );

    let db_dir = index.path.clone();
    let temp_dir = index
      .path
      .parent()
      .ok_or_else(|| anyhow!("Cannot get parent directory of index path"))?
      .join("backup");

    if db_dir.exists() {
      if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
      }
      // Move current database to a temporary location
      fs::rename(&db_dir, &temp_dir)?;
    }

    // Rename checkpoint to database directory (atomic operation)
    fs::rename(checkpoint_path, &db_dir)?;

    // Remove checkpoints beyond the current height
    for (_height, path) in checkpoint_dirs
      .iter()
      .skip_while(|(h, _)| *h <= height - depth)
    {
      log::info!("removing checkpoint path: {}", path.display());
      fs::remove_dir_all(path)?;
    }

    log::info!(
      "successfully prepared database rollback to height {}",
      checkpoint_height
    );

    Ok(())
  }

  pub(crate) fn update_savepoints(index: &Index, height: u32) -> Result {
    if Self::is_savepoint_required(index, height)? {
      // Place checkpoint directory alongside index.rocksdb
      let checkpoints_dir = index
        .path
        .parent()
        .ok_or_else(|| anyhow!("Cannot get parent directory of index path"))?
        .join("checkpoints");
      fs::create_dir_all(&checkpoints_dir)?;

      log::debug!("Creating checkpoint at height {}", height);

      // Create checkpoint
      let checkpoint_path = checkpoints_dir.join(format!("checkpoint_{}", height));
      let checkpoint = Checkpoint::new(&index.database)?;
      checkpoint.create_checkpoint(&checkpoint_path)?;

      log::debug!(
        "Checkpoint created successfully at {}",
        checkpoint_path.display()
      );

      // Update last savepoint height
      let statistic_to_count = index
        .database
        .cf_handle(CF_STATISTIC_TO_COUNT)
        .ok_or_else(|| anyhow!("Failed to open column family 'statistic_to_count'"))?;

      index.database.put_cf_opt(
        statistic_to_count,
        &Statistic::LastSavepointHeight.key().to_be_bytes(),
        &height.to_be_bytes(),
        &index.write_options,
      )?;

      let mut flush_opts = FlushOptions::default();
      flush_opts.set_wait(true);

      index
        .database
        .flush_cf_opt(statistic_to_count, &flush_opts)?;

      // Clean up old checkpoints
      Self::cleanup_old_checkpoints(&checkpoints_dir)?;
    }

    Ok(())
  }

  /// Clean up old checkpoints, keeping only the newest MAX_SAVEPOINTS
  fn cleanup_old_checkpoints(checkpoints_dir: &Path) -> Result<()> {
    let mut checkpoint_dirs: Vec<_> = fs::read_dir(checkpoints_dir)?
      .filter_map(|entry| {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
          let name = path.file_name()?.to_str()?;
          if name.starts_with("checkpoint_") {
            let height_str = &name[11..]; // Skip "checkpoint_" prefix
            height_str.parse::<u32>().ok().map(|height| (height, path))
          } else {
            None
          }
        } else {
          None
        }
      })
      .collect();

    checkpoint_dirs.sort_by_key(|(height, _)| *height);

    // Delete excess checkpoints
    if checkpoint_dirs.len() > MAX_SAVEPOINTS as usize {
      let to_remove = checkpoint_dirs.len() - MAX_SAVEPOINTS as usize;
      for (_, path) in checkpoint_dirs.iter().take(to_remove) {
        log::debug!("Removing old checkpoint: {}", path.display());
        fs::remove_dir_all(path)?;
      }
    }

    Ok(())
  }
}

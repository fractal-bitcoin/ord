// To the extent possible under law, the author(s) have dedicated all
// copyright and related and neighboring rights to this software to the
// public domain worldwide. This software is distributed without
// any warranty.
//
// You should have received a copy of the CC0 Public Domain Dedication
// along with this software.
// If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
//

use crate::json::bitcoin::Amount;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Network information result with updated warnings field as Vec<String>
/// This reuses the existing structures from bitcoincore_rpc_json but overrides the warnings field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetNetworkInfoResult {
  /// The server version
  pub version: usize,
  /// The server subversion string
  pub subversion: String,
  /// The protocol version
  #[serde(rename = "protocolversion")]
  pub protocol_version: usize,
  /// The services we offer to the network
  #[serde(rename = "localservices")]
  pub local_services: String,
  /// True if transaction relay is requested from peers
  #[serde(rename = "localrelay")]
  pub local_relay: bool,
  /// Time offset
  #[serde(rename = "timeoffset")]
  pub time_offset: isize,
  /// The number of connections
  pub connections: usize,
  /// The number of connections in
  pub connections_in: Option<usize>,
  /// The number of connections out
  pub connections_out: Option<usize>,
  /// Whether p2p networking is enabled
  #[serde(rename = "networkactive")]
  pub network_active: bool,
  /// Information per network
  pub networks: Vec<crate::json::GetNetworkInfoResultNetwork>,
  /// Minimum relay fee for transactions in BTC/kB
  #[serde(
    rename = "relayfee",
    with = "crate::json::bitcoin::amount::serde::as_btc"
  )]
  pub relay_fee: Amount,
  /// Minimum fee increment for mempool limiting or BIP 125 replacement in BTC/kB
  #[serde(
    rename = "incrementalfee",
    with = "crate::json::bitcoin::amount::serde::as_btc"
  )]
  pub incremental_fee: Amount,
  /// List of local addresses
  #[serde(rename = "localaddresses")]
  pub local_addresses: Vec<crate::json::GetNetworkInfoResultAddress>,
}

/// Blockchain information result with updated warnings field as Vec<String>
/// This reuses the existing structures from bitcoincore_rpc_json but overrides the warnings field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetBlockchainInfoResult {
  /// Current network name as defined in BIP70 (main, test, regtest)
  pub chain: String,
  /// The current number of blocks processed in the server
  pub blocks: u64,
  /// The current number of headers we have validated
  pub headers: u64,
  /// The hash of the currently best block
  #[serde(rename = "bestblockhash")]
  pub best_block_hash: crate::json::bitcoin::BlockHash,
  /// The current difficulty
  pub difficulty: f64,
  /// Median time for the current best block
  #[serde(rename = "mediantime")]
  pub median_time: u64,
  /// Estimate of verification progress [0..1]
  #[serde(rename = "verificationprogress")]
  pub verification_progress: f64,
  /// Estimate of whether this node is in Initial Block Download mode
  #[serde(rename = "initialblockdownload")]
  pub initial_block_download: bool,
  /// Total amount of work in active chain, in hexadecimal
  #[serde(rename = "chainwork", with = "crate::json::serde_hex")]
  pub chain_work: Vec<u8>,
  /// The estimated size of the block and undo files on disk
  pub size_on_disk: u64,
  /// If the blocks are subject to pruning
  pub pruned: bool,
  /// Lowest-height complete block stored (only present if pruning is enabled)
  #[serde(rename = "pruneheight")]
  pub prune_height: Option<u64>,
  /// Whether automatic pruning is enabled (only present if pruning is enabled)
  pub automatic_pruning: Option<bool>,
  /// The target size used by pruning (only present if automatic pruning is enabled)
  pub prune_target_size: Option<u64>,
  /// Status of softforks in progress
  #[serde(default)]
  pub softforks: HashMap<String, crate::json::Softfork>,
}

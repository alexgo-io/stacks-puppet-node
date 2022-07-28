use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::ops::Add;
use std::sync::{Mutex, Arc};
use std::time::{Instant, SystemTime, Duration};
use async_std::stream::StreamExt;
use async_std::task::block_on;
use http_types::{Method, Response, StatusCode};

use stacks::burnchains::bitcoin::BitcoinBlock;
use stacks::burnchains::{
    Burnchain, BurnchainBlock, BurnchainBlockHeader, BurnchainStateTransitionOps, Txid,
};
use stacks::chainstate::burn::db::sortdb::{SortitionDB, SortitionHandleTx};
use stacks::chainstate::burn::operations::{
    leader_block_commit::BURN_BLOCK_MINED_AT_MODULUS, BlockstackOperationType, LeaderBlockCommitOp,
    LeaderKeyRegisterOp, PreStxOp, StackStxOp, TransferStxOp, UserBurnSupportOp,
};
use stacks::chainstate::burn::BlockSnapshot;
use stacks::core::{StacksEpoch, StacksEpochId, PEER_VERSION_EPOCH_2_0, STACKS_EPOCH_MAX};
use stacks::types::chainstate::{BurnchainHeaderHash, PoxId};
use stacks::util::get_epoch_time_secs;
use stacks::util::hash::Sha256Sum;

use super::super::operations::BurnchainOpSigner;
use super::super::Config;
use super::{BurnchainController, BurnchainTip, Error as BurnchainControllerError};
use stacks::vm::costs::ExecutionCost;

struct PuppetControl {
    pub kicked: bool,
    pub next_block_time: SystemTime,
    pub block_interval: Duration,
}

/// MocknetController is simulating a simplistic burnchain.
pub struct MocknetController {
    config: Config,
    burnchain: Burnchain,
    db: Option<SortitionDB>,
    chain_tip: Option<BurnchainTip>,
    queued_operations: VecDeque<BlockstackOperationType>,
    puppet_control: Arc<Mutex<PuppetControl>>,
    control_server: Option<std::thread::JoinHandle<Result<(), io::Error>>>,
}

impl MocknetController {
    pub fn generic(config: Config) -> Box<dyn BurnchainController> {
        Box::new(Self::new(config))
    }

    fn new(config: Config) -> Self {
        debug!("Opening Burnchain at {}", &config.get_burn_db_path());
        let burnchain = Burnchain::regtest(&config.get_burn_db_path());

        Self {
            config: config,
            burnchain: burnchain,
            db: None,
            queued_operations: VecDeque::new(),
            chain_tip: None,
            puppet_control: Arc::new(Mutex::new(PuppetControl {
                kicked: false,
                next_block_time: SystemTime::now(),
                block_interval: Duration::from_secs(600),
            })),
            control_server: None,
        }
    }

    fn build_next_block_header(current_block: &BlockSnapshot) -> BurnchainBlockHeader {
        let curr_hash = &current_block.burn_header_hash.to_bytes()[..];
        let next_hash = Sha256Sum::from_data(&curr_hash);

        let block = BurnchainBlock::Bitcoin(BitcoinBlock::new(
            current_block.block_height + 1,
            &BurnchainHeaderHash::from_bytes(next_hash.as_bytes()).unwrap(),
            &current_block.burn_header_hash,
            &vec![],
            get_epoch_time_secs(),
        ));
        block.header()
    }
}

impl BurnchainController for MocknetController {
    fn sortdb_ref(&self) -> &SortitionDB {
        self.db.as_ref().expect("BUG: did not instantiate burn DB")
    }

    fn sortdb_mut(&mut self) -> &mut SortitionDB {
        match self.db {
            Some(ref mut sortdb) => sortdb,
            None => {
                unreachable!();
            }
        }
    }

    fn get_chain_tip(&self) -> BurnchainTip {
        match &self.chain_tip {
            Some(chain_tip) => chain_tip.clone(),
            None => {
                unreachable!();
            }
        }
    }

    fn get_headers_height(&self) -> u64 {
        match &self.chain_tip {
            Some(chain_tip) => chain_tip.block_snapshot.block_height,
            None => {
                unreachable!();
            }
        }
    }

    fn get_stacks_epochs(&self) -> Vec<StacksEpoch> {
        match &self.config.burnchain.epochs {
            Some(epochs) => epochs.clone(),
            None => vec![StacksEpoch {
                epoch_id: StacksEpochId::Epoch20,
                start_height: 0,
                end_height: STACKS_EPOCH_MAX,
                block_limit: ExecutionCost::max_value(),
                network_epoch: PEER_VERSION_EPOCH_2_0,
            }],
        }
    }

    fn start(
        &mut self,
        _ignored_target_height_opt: Option<u64>,
    ) -> Result<(BurnchainTip, u64), BurnchainControllerError> {
        // If `config` sets a value, use that. Otherwise, use a default.
        let epoch_vector = self.get_stacks_epochs();
        let db = match SortitionDB::connect(
            &self.config.get_burn_db_file_path(),
            0,
            &BurnchainHeaderHash::zero(),
            get_epoch_time_secs(),
            &epoch_vector,
            true,
        ) {
            Ok(db) => db,
            Err(_) => panic!("Error while connecting to burnchain db"),
        };
        let block_snapshot = SortitionDB::get_canonical_burn_chain_tip(db.conn())
            .expect("FATAL: failed to get canonical chain tip");

        self.db = Some(db);

        let genesis_state = BurnchainTip {
            block_snapshot,
            state_transition: BurnchainStateTransitionOps::noop(),
            received_at: Instant::now(),
        };
        self.chain_tip = Some(genesis_state.clone());
        let block_height = genesis_state.block_snapshot.block_height;

        if std::env::var("STACKS_NODE_PUPPET_MODE").unwrap_or_default() == "true" {
            info!("ENV STACKS_NODE_PUPPET_MODE is set to true, starting burnchain signal server..");
            let puppet_control = Arc::clone(&self.puppet_control);
            self.control_server = Some(std::thread::spawn(move || block_on(async {
                let listener = async_std::net::TcpListener::bind(
                    std::env::var("STACKS_NODE_PUPPET_BIND").unwrap_or(String::from("0.0.0.0:20445"))).await?;
                let addr = format!("http://{}", listener.local_addr()?);
                info!("burnchain signal server listening on {}", addr);

                // For each incoming TCP connection, spawn a task and call `accept`.
                let mut incoming = listener.incoming();
                while let Some(stream) = incoming.next().await {
                    let stream = stream?;
                    async_h1::accept(stream.clone(), |req| async {
                        let mut req = req;
                        match (
                            req.method(),
                            req.url().path(),
                        ) {
                            (Method::Get, "/ping") => Ok(Response::new(StatusCode::Ok)),
                            (Method::Post, "/kick") => {
                                let mut puppet_control = puppet_control.lock().unwrap();
                                puppet_control.kicked = true;
                                Ok(Response::new(StatusCode::Ok))
                            }
                            (Method::Put, "/duration") => {
                                let body = req.body_string().await;
                                match body {
                                    Ok(x) => {
                                        let v = x.parse::<u64>().unwrap_or(0);
                                        if v > 0 {
                                            println!("Setting duration to {}", v);
                                            io::stdout().flush().unwrap();
                                            let mut puppet_control = puppet_control.lock().unwrap();
                                            puppet_control.block_interval = Duration::from_secs(v);
                                            puppet_control.next_block_time = SystemTime::now().add(puppet_control.block_interval);
                                        }
                                    }
                                    _ => ()
                                }
                                Ok(Response::new(StatusCode::Ok))
                            }
                            _ => {
                                let mut rs = Response::new(StatusCode::BadRequest);
                                rs.set_body(format!("[{}] {}", req.method(), req.url().path()));
                                Ok(rs)
                            }
                        }
                    }).await.unwrap_or(())
                }
                Ok(())
            })));
        }

        Ok((genesis_state, block_height))
    }

    fn submit_operation(
        &mut self,
        operation: BlockstackOperationType,
        _op_signer: &mut BurnchainOpSigner,
        _attempt: u64,
    ) -> bool {
        self.queued_operations.push_back(operation);
        true
    }

    fn sync(
        &mut self,
        _ignored_target_height_opt: Option<u64>,
    ) -> Result<(BurnchainTip, u64), BurnchainControllerError> {
        let chain_tip = self.get_chain_tip();
        if chain_tip.block_snapshot.block_height > 3 && self.control_server.is_some() {
            info!("Waiting a signal to proceed at burn block height {}", chain_tip.block_snapshot.block_height);
            loop {
                let puppet_control = self.puppet_control.lock().unwrap();
                if puppet_control.kicked || puppet_control.next_block_time.le(&SystemTime::now()) { break; }
                drop(puppet_control);
                std::thread::sleep(Duration::from_millis(100));
            }
            info!("Signal received, mining new burn block...");
        }

        // Simulating mining
        let next_block_header = Self::build_next_block_header(&chain_tip.block_snapshot);
        let mut vtxindex = 1;
        let mut ops = vec![];

        while let Some(payload) = self.queued_operations.pop_front() {
            let txid = Txid(
                Sha256Sum::from_data(
                    format!("{}::{}", next_block_header.block_height, vtxindex).as_bytes(),
                )
                    .0,
            );
            let op = match payload {
                BlockstackOperationType::LeaderKeyRegister(payload) => {
                    BlockstackOperationType::LeaderKeyRegister(LeaderKeyRegisterOp {
                        consensus_hash: payload.consensus_hash,
                        public_key: payload.public_key,
                        memo: payload.memo,
                        address: payload.address,
                        txid,
                        vtxindex: vtxindex,
                        block_height: next_block_header.block_height,
                        burn_header_hash: next_block_header.block_hash,
                    })
                }
                BlockstackOperationType::LeaderBlockCommit(payload) => {
                    BlockstackOperationType::LeaderBlockCommit(LeaderBlockCommitOp {
                        sunset_burn: 0,
                        block_header_hash: payload.block_header_hash,
                        new_seed: payload.new_seed,
                        parent_block_ptr: payload.parent_block_ptr,
                        parent_vtxindex: payload.parent_vtxindex,
                        key_block_ptr: payload.key_block_ptr,
                        key_vtxindex: payload.key_vtxindex,
                        memo: payload.memo,
                        burn_fee: payload.burn_fee,
                        apparent_sender: payload.apparent_sender,
                        input: payload.input,
                        commit_outs: payload.commit_outs,
                        txid,
                        vtxindex: vtxindex,
                        block_height: next_block_header.block_height,
                        burn_parent_modulus: if next_block_header.block_height > 0 {
                            (next_block_header.block_height - 1) % BURN_BLOCK_MINED_AT_MODULUS
                        } else {
                            BURN_BLOCK_MINED_AT_MODULUS - 1
                        } as u8,
                        burn_header_hash: next_block_header.block_hash,
                    })
                }
                BlockstackOperationType::UserBurnSupport(payload) => {
                    BlockstackOperationType::UserBurnSupport(UserBurnSupportOp {
                        address: payload.address,
                        consensus_hash: payload.consensus_hash,
                        public_key: payload.public_key,
                        key_block_ptr: payload.key_block_ptr,
                        key_vtxindex: payload.key_vtxindex,
                        block_header_hash_160: payload.block_header_hash_160,
                        burn_fee: payload.burn_fee,
                        txid,
                        vtxindex: vtxindex,
                        block_height: next_block_header.block_height,
                        burn_header_hash: next_block_header.block_hash,
                    })
                }
                BlockstackOperationType::PreStx(payload) => {
                    BlockstackOperationType::PreStx(PreStxOp {
                        txid,
                        vtxindex,
                        block_height: next_block_header.block_height,
                        burn_header_hash: next_block_header.block_hash,
                        ..payload
                    })
                }
                BlockstackOperationType::TransferStx(payload) => {
                    BlockstackOperationType::TransferStx(TransferStxOp {
                        txid,
                        vtxindex,
                        block_height: next_block_header.block_height,
                        burn_header_hash: next_block_header.block_hash,
                        ..payload
                    })
                }
                BlockstackOperationType::StackStx(payload) => {
                    BlockstackOperationType::StackStx(StackStxOp {
                        txid,
                        vtxindex,
                        block_height: next_block_header.block_height,
                        burn_header_hash: next_block_header.block_hash,
                        ..payload
                    })
                }
            };
            ops.push(op);
            vtxindex += 1;
        }

        // Include txs in a new block
        let (block_snapshot, state_transition) = {
            match self.db {
                None => {
                    unreachable!();
                }
                Some(ref mut burn_db) => {
                    let mut burn_tx =
                        SortitionHandleTx::begin(burn_db, &chain_tip.block_snapshot.sortition_id)
                            .unwrap();
                    let new_chain_tip = burn_tx
                        .process_block_ops(
                            &self.burnchain,
                            &chain_tip.block_snapshot,
                            &next_block_header,
                            ops,
                            None,
                            PoxId::stubbed(),
                            None,
                            0,
                        )
                        .unwrap();
                    burn_tx.commit().unwrap();
                    new_chain_tip
                }
            }
        };

        let state_transition = BurnchainStateTransitionOps {
            accepted_ops: state_transition.accepted_ops,
            consumed_leader_keys: state_transition.consumed_leader_keys,
        };

        // Transmit the new state
        let new_state = BurnchainTip {
            block_snapshot,
            state_transition,
            received_at: Instant::now(),
        };
        self.chain_tip = Some(new_state.clone());

        let block_height = new_state.block_snapshot.block_height;
        {
            let mut puppet_control = self.puppet_control.lock().unwrap();
            puppet_control.next_block_time = SystemTime::now().add(puppet_control.block_interval);
            puppet_control.kicked = false;
        }
        Ok((new_state, block_height))
    }

    #[cfg(test)]
    fn bootstrap_chain(&mut self, _num_blocks: u64) {}

    fn connect_dbs(&mut self) -> Result<(), BurnchainControllerError> {
        Ok(())
    }
}

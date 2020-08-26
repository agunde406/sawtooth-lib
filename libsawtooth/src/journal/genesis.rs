/*
 * Copyright 2018 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

use std::error::Error;
use std::fs::{remove_file, File};
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};

use cylinder::Signer;
use transact::execution::executor::Executor;
use transact::protocol::receipt::TransactionResult;
use transact::scheduler::{BatchExecutionResult, SchedulerError, SchedulerFactory};
use transact::state::{merkle::MerkleState, StateChange, Write};

use crate::journal::block_manager::BlockManager;
use crate::protocol::block::BlockBuilder;
use crate::protocol::genesis::GenesisData;
use crate::protos::FromBytes;
use crate::state::settings_view::SettingsView;
use crate::state::state_view_factory::StateViewFactory;
use crate::store::receipt_store::TransactionReceiptStore;

use super::{chain::ChainReader, chain_id_manager::ChainIdManager, NULL_BLOCK_IDENTIFIER};

const GENESIS_FILE: &str = "genesis.batch";

/// Used to combine errors and `Batch` results returned from the `Scheduler`
enum SchedulerEvent {
    Result(BatchExecutionResult),
    Error(SchedulerError),
    Complete,
}

/// Builder for creating a `GenesisController`
#[derive(Default)]
pub struct GenesisControllerBuilder {
    transaction_executor: Option<Executor>,
    scheduler_factory: Option<Box<dyn SchedulerFactory>>,
    block_manager: Option<BlockManager>,
    chain_reader: Option<Box<dyn ChainReader>>,
    state_view_factory: Option<StateViewFactory>,
    identity_signer: Option<Box<dyn Signer>>,
    data_dir: Option<String>,
    chain_id_manager: Option<ChainIdManager>,
    receipt_store: Option<TransactionReceiptStore>,
    initial_state_root: Option<String>,
    merkle_state: Option<MerkleState>,
}

impl GenesisControllerBuilder {
    // Creates an empty builder
    pub fn new() -> Self {
        GenesisControllerBuilder::default()
    }

    pub fn with_transaction_executor(mut self, executor: Executor) -> GenesisControllerBuilder {
        self.transaction_executor = Some(executor);
        self
    }

    pub fn with_scheduler_factory(
        mut self,
        scheduler_factory: Box<dyn SchedulerFactory>,
    ) -> GenesisControllerBuilder {
        self.scheduler_factory = Some(scheduler_factory);
        self
    }

    pub fn with_block_manager(mut self, block_manager: BlockManager) -> GenesisControllerBuilder {
        self.block_manager = Some(block_manager);
        self
    }

    pub fn with_chain_reader(
        mut self,
        chain_reader: Box<dyn ChainReader>,
    ) -> GenesisControllerBuilder {
        self.chain_reader = Some(chain_reader);
        self
    }

    pub fn with_state_view_factory(
        mut self,
        state_view_factory: StateViewFactory,
    ) -> GenesisControllerBuilder {
        self.state_view_factory = Some(state_view_factory);
        self
    }

    pub fn with_data_dir(mut self, data_dir: String) -> GenesisControllerBuilder {
        self.data_dir = Some(data_dir);
        self
    }

    pub fn with_chain_id_manager(
        mut self,
        chain_id_manager: ChainIdManager,
    ) -> GenesisControllerBuilder {
        self.chain_id_manager = Some(chain_id_manager);
        self
    }

    pub fn with_receipt_store(
        mut self,
        receipt_store: TransactionReceiptStore,
    ) -> GenesisControllerBuilder {
        self.receipt_store = Some(receipt_store);
        self
    }

    pub fn with_initial_state_root(
        mut self,
        initial_state_root: String,
    ) -> GenesisControllerBuilder {
        self.initial_state_root = Some(initial_state_root);
        self
    }

    pub fn with_merkle_state(mut self, merkle_state: MerkleState) -> GenesisControllerBuilder {
        self.merkle_state = Some(merkle_state);
        self
    }

    pub fn with_identity_signer(
        mut self,
        identity_signer: Box<dyn Signer>,
    ) -> GenesisControllerBuilder {
        self.identity_signer = Some(identity_signer);
        self
    }

    /// Builds the `GenesisJounral`
    ///
    /// Returns an error if one of the required fields are not provided
    pub fn build(self) -> Result<GenesisController, GenesisControllerBuildError> {
        let transaction_executor = self.transaction_executor.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'transaction_executor' field is required".to_string(),
            )
        })?;

        let scheduler_factory = self.scheduler_factory.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'scheduler_factory' field is required".to_string(),
            )
        })?;

        let block_manager = self.block_manager.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'block_manager' field is required".to_string(),
            )
        })?;

        let chain_reader = self.chain_reader.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'chain_reader' field is required".to_string(),
            )
        })?;

        let state_view_factory = self.state_view_factory.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'state_view_factory' field is required".to_string(),
            )
        })?;

        let data_dir = self.data_dir.ok_or_else(|| {
            GenesisControllerBuildError::MissingField("'data_dir' field is required".to_string())
        })?;

        let chain_id_manager = self.chain_id_manager.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'chain_id_manager' field is required".to_string(),
            )
        })?;

        let receipt_store = self.receipt_store.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'receipt_store' field is required".to_string(),
            )
        })?;

        let initial_state_root = self.initial_state_root.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'initial_state_root' field is required".to_string(),
            )
        })?;

        let merkle_state = self.merkle_state.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'merkle_state' field is required".to_string(),
            )
        })?;

        let identity_signer = self.identity_signer.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'identity_signer' field is required".to_string(),
            )
        })?;

        let mut genesis_path_buf = PathBuf::from(data_dir);
        genesis_path_buf.push(GENESIS_FILE);

        Ok(GenesisController {
            transaction_executor,
            scheduler_factory,
            block_manager,
            chain_reader,
            state_view_factory,
            identity_signer,
            chain_id_manager,
            receipt_store,
            genesis_file_path: genesis_path_buf,
            initial_state_root,
            merkle_state,
        })
    }
}

/// The `GenesisController` is in charge of checking if this is the genesis node and creating the
/// the genesis block from the genesis batch file
pub struct GenesisController {
    transaction_executor: Executor,
    scheduler_factory: Box<dyn SchedulerFactory>,
    block_manager: BlockManager,
    chain_reader: Box<dyn ChainReader>,
    state_view_factory: StateViewFactory,
    identity_signer: Box<dyn Signer>,
    chain_id_manager: ChainIdManager,
    receipt_store: TransactionReceiptStore,
    initial_state_root: String,
    merkle_state: MerkleState,
    genesis_file_path: PathBuf,
}

impl GenesisController {
    /// Determines if the system should be put in genesis mode
    ///
    /// Returns:
    ///      bool: return whether or not a genesis block is required to be
    ///
    /// Returns error if there is invalid combination of the following: genesis.batch, existing
    /// chain head, and block chain id.
    pub fn requires_genesis(&self) -> Result<bool, GenesisError> {
        // check if the genesis batch file exists
        let genesis_path = self.genesis_file_path.as_path();
        if genesis_path.is_file() {
            debug!("Genesis batch file: {}", self.genesis_file_path.display());
        } else {
            debug!("Genesis batch file: not found",);
        }

        // check if there is a chain head
        let chain_head = self.chain_reader.chain_head().map_err(|err| {
            GenesisError::InvalidGenesisState(format!("Unable to read chain head: {}", err))
        })?;

        if chain_head.is_some() && genesis_path.is_file() {
            return Err(GenesisError::InvalidGenesisState(format!(
                "Cannot have a genesis batch file and an existing chain (chain head: {})",
                chain_head
                    .expect("Chain head will never be None")
                    .block()
                    .header_signature()
            )));
        }

        // check if there is a block chain id
        let block_chain_id = self.chain_id_manager.get_block_chain_id().map_err(|err| {
            GenesisError::InvalidGenesisState(format!("Unable to read block chain id: {}", err))
        })?;

        if block_chain_id.is_some() && genesis_path.is_file() {
            return Err(GenesisError::InvalidGenesisState(format!(
                "Cannot have a genesis batch file and join an existing network (chain_id: {})",
                block_chain_id.expect("Chain id will never be None")
            )));
        };

        if !genesis_path.is_file() && chain_head.is_none() {
            info!("No chain head and not the genesis node: starting in peering mode")
        }

        Ok(genesis_path.is_file() && chain_head.is_none() && block_chain_id.is_none())
    }

    /// Starts the genesis block creation process.
    ///
    /// GenesisError is returned if a genesis block is unable to be produced, or the resulting
    /// block-chain-id saved.
    pub fn start(&mut self) -> Result<(), GenesisError> {
        let mut batch_file = File::open(self.genesis_file_path.clone()).map_err(|_| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to open genesis batch file: {}",
                self.genesis_file_path.display()
            ))
        })?;

        let mut contents = vec![];
        batch_file.read_to_end(&mut contents).map_err(|_| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to read entire genesis batch file: {}",
                self.genesis_file_path.display()
            ))
        })?;
        let genesis_data = GenesisData::from_bytes(&contents).map_err(|_| {
            GenesisError::InvalidGenesisData(
                "Unable to create GenesisData from bytes in genesis batch file".to_string(),
            )
        })?;

        let mut scheduler = self
            .scheduler_factory
            .create_scheduler(self.initial_state_root.to_string())
            .map_err(|err| {
                GenesisError::BatchValidationError(format!("Unable to create scheduler: {:?}", err))
            })?;

        let (result_tx, result_rx): (Sender<SchedulerEvent>, Receiver<SchedulerEvent>) = channel();
        let error_tx = result_tx.clone();
        // Add callback to convert batch result option to scheduler event
        scheduler
            .set_result_callback(Box::new(move |batch_result| {
                let scheduler_event = match batch_result {
                    Some(result) => SchedulerEvent::Result(result),
                    None => SchedulerEvent::Complete,
                };
                if result_tx.send(scheduler_event).is_err() {
                    error!("Unable to send batch result; receiver must have dropped");
                }
            }))
            .map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "Unable to set result callback on scheduler: {}",
                    err
                ))
            })?;

        // add callback to convert error into scheduler event
        scheduler
            .set_error_callback(Box::new(move |err| {
                if error_tx.send(SchedulerEvent::Error(err)).is_err() {
                    error!("Unable to send scheduler error; receiver must have dropped");
                }
            }))
            .map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "Unable to set error callback on scheduler: {}",
                    err
                ))
            })?;

        debug!("Adding {} genesis batches", genesis_data.batches().len());

        for batch in genesis_data.batches() {
            scheduler.add_batch(batch.clone()).map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "While adding a batch to the schedule: {:?}",
                    err
                ))
            })?;
        }

        scheduler.finalize().map_err(|err| {
            GenesisError::BatchValidationError(format!(
                "During call to scheduler.finalize: {:?}",
                err
            ))
        })?;

        self.transaction_executor
            .execute(
                scheduler.take_task_iterator().map_err(|_| {
                    GenesisError::BatchValidationError(
                        "Unable to take task iterator from scheduler".to_string(),
                    )
                })?,
                scheduler.new_notifier().map_err(|_| {
                    GenesisError::BatchValidationError(
                        "Unable to get new notifier from scheduler".to_string(),
                    )
                })?,
            )
            .map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "During call to Executor.execute: {}",
                    err
                ))
            })?;

        // collection results from the scheduler
        let mut execution_results = vec![];
        loop {
            match result_rx.recv() {
                Ok(SchedulerEvent::Result(result)) => execution_results.push(result),
                Ok(SchedulerEvent::Complete) => break,
                Ok(SchedulerEvent::Error(err)) => {
                    return Err(GenesisError::BatchValidationError(format!(
                        "During execution: {:?}",
                        err
                    )))
                }
                Err(err) => {
                    return Err(GenesisError::BatchValidationError(format!(
                        "Error while trying to receive scheduler event: {:?}",
                        err
                    )))
                }
            }
        }

        // Collect reciepts and changes from execution results
        let mut receipts = vec![];
        let mut changes = vec![];
        for batch_result in execution_results {
            if !batch_result.receipts.is_empty() {
                for receipt in batch_result.receipts {
                    match receipt.transaction_result.clone() {
                        TransactionResult::Invalid { .. } => {
                            return Err(GenesisError::BatchValidationError(format!(
                                "Genesis batch {} was invalid due to transaction {}",
                                &batch_result.batch.batch().header_signature(),
                                &receipt.transaction_id
                            )));
                        }
                        TransactionResult::Valid { state_changes, .. } => {
                            changes.append(
                                &mut state_changes.into_iter().map(StateChange::from).collect(),
                            );
                            receipts.push(receipt);
                        }
                    };
                }
            } else {
                return Err(GenesisError::BatchValidationError(format!(
                    "batch {} did not have transaction results",
                    &batch_result.batch.batch().header_signature(),
                )));
            };
        }

        // commit state and get new state hash
        let new_state_hash = self
            .merkle_state
            .commit(&self.initial_state_root, &changes)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Unable to commit state from genesis batches {}",
                    err
                ))
            })?;

        debug!("Produced state hash {} for genesis block", new_state_hash);

        let new_state_hash_bytes = hex::decode(new_state_hash.clone()).map_err(|_| {
            GenesisError::InvalidGenesisState(format!(
                "Cannot convert resulting state hash to bytes: {}",
                new_state_hash
            ))
        })?;

        // Validate required settings are set
        let settings_view = self
            .state_view_factory
            .create_view::<SettingsView>(&new_state_hash_bytes)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!("Cannot create settings view: {}", err))
            })?;

        let name = settings_view
            .get_setting_str("sawtooth.consensus.algorithm.name", None)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Cannot check for sawtooth.consensus.algorithm.name : {}",
                    err
                ))
            })?;
        let version = settings_view
            .get_setting_str("sawtooth.consensus.algorithm.version", None)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Cannot check for sawtooth.consensus.algorithm.version : {}",
                    err
                ))
            })?;

        if name.is_none() || version.is_none() {
            return Err(GenesisError::LocalConfigurationError(
                "sawtooth.consensus.algorithm.name \
                and sawtooth.consensus.algorithm.version must be set in the \
                genesis block"
                    .to_string(),
            ));
        }

        let block_pair = self
            .generate_genesis_block()
            .with_batches(
                genesis_data
                    .take_batches()
                    .into_iter()
                    .map(|batch| {
                        let (batch, _) = batch.take();
                        batch
                    })
                    .collect(),
            )
            .with_state_root_hash(new_state_hash_bytes)
            .build_pair(&*self.identity_signer)
            .map_err(|err| GenesisError::BlockGenerationError(err.to_string()))?;

        let block_id = block_pair.block().header_signature().to_string();

        info!("Genesis block created");
        self.block_manager.put(vec![block_pair]).map_err(|err| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to put genesis block into block manager: {}",
                err
            ))
        })?;

        self.block_manager
            .persist(&block_id, "commit_store")
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Unable to persist genesis block into block manager: {}",
                    err
                ))
            })?;

        self.receipt_store.append(receipts).map_err(|err| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to append transaction receipts into receipt store: {}",
                err
            ))
        })?;

        self.chain_id_manager
            .save_block_chain_id(&block_id)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Unable to save block chain id in the chain id manager: {}",
                    err
                ))
            })?;

        debug!("Deleting genesis file");
        remove_file(self.genesis_file_path.as_path()).map_err(|err| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to remove genesis batch file: {}",
                err
            ))
        })?;

        Ok(())
    }

    fn generate_genesis_block(&self) -> BlockBuilder {
        BlockBuilder::new()
            .with_block_num(0)
            .with_previous_block_id(NULL_BLOCK_IDENTIFIER.to_string())
            .with_consensus(b"Genesis".to_vec())
    }
}

/// Represents errors raised while building
#[derive(Debug)]
pub enum GenesisError {
    BatchValidationError(String),
    BlockGenerationError(String),
    InvalidGenesisState(String),
    InvalidGenesisData(String),
    LocalConfigurationError(String),
}

impl Error for GenesisError {}

impl std::fmt::Display for GenesisError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            GenesisError::BatchValidationError(ref msg) => {
                write!(f, "Unable to validate batches {}", msg)
            }
            GenesisError::BlockGenerationError(ref msg) => {
                write!(f, "Unable to create genesisi block {}", msg)
            }
            GenesisError::InvalidGenesisState(ref msg) => write!(f, "{}", msg),
            GenesisError::InvalidGenesisData(ref msg) => write!(f, "{}", msg),
            GenesisError::LocalConfigurationError(ref msg) => {
                write!(f, "Unable to start validator: {}", msg)
            }
        }
    }
}

/// Errors that may occur when building the GenesisController
#[derive(Debug)]
pub enum GenesisControllerBuildError {
    MissingField(String),
}

impl std::error::Error for GenesisControllerBuildError {
    fn description(&self) -> &str {
        match *self {
            Self::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for GenesisControllerBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::MissingField(ref s) => write!(f, "missing a required field: {}", s),
        }
    }
}

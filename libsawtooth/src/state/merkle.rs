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

use std::collections::HashMap;
use std::io::Cursor;

use cbor::value::Bytes;
use cbor::value::Value;
use cbor::{decoder::GenericDecoder, encoder::GenericEncoder};

use transact::database::Database;
use transact::state::merkle::MerkleRadixTree;
use transact::state::merkle::StateDatabaseError as TransactStateDatabaseError;
use transact::state::{Read, StateChange, StateReadError, StateWriteError, Write};

use crate::state::error::StateDatabaseError;

use super::{StateIter, StateReader};

pub fn decode_cbor_value(cbor_bytes: &[u8]) -> Result<Vec<u8>, StateDatabaseError> {
    let input = Cursor::new(cbor_bytes);
    let mut decoder = GenericDecoder::new(cbor::Config::default(), input);
    let decoded_value = decoder.value()?;

    match decoded_value {
        Value::Bytes(Bytes::Bytes(bytes)) => Ok(bytes),
        _ => Err(StateDatabaseError::InvalidRecord),
    }
}

pub fn encode_cbor_value(bytes: &[u8]) -> Result<Vec<u8>, StateDatabaseError> {
    let mut encoder = GenericEncoder::new(Cursor::new(Vec::new()));
    encoder
        .value(&Value::Bytes(Bytes::Bytes(bytes.to_vec())))
        .map_err(|_| StateDatabaseError::InvalidRecord)?;
    Ok(encoder.into_inner().into_writer().into_inner())
}

#[derive(Clone)]
pub struct CborMerkleState {
    db: Box<dyn Database>,
}

impl CborMerkleState {
    pub fn new(db: Box<dyn Database>) -> Self {
        Self { db }
    }
}

impl Write for CborMerkleState {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn commit(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        let mut merkle_database = MerkleRadixTree::new(self.db.clone(), Some(state_id))
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))?;

        merkle_database
            .set_merkle_root(state_id.to_string())
            .map_err(|err| match err {
                TransactStateDatabaseError::NotFound(msg) => StateWriteError::InvalidStateId(msg),
                _ => StateWriteError::StorageError(Box::new(err)),
            })?;

        let cbor_state_changes = state_changes
            .iter()
            .map(|state_change| match state_change {
                StateChange::Delete { .. } => Ok(state_change.clone()),
                StateChange::Set { key, value } => {
                    let cbor_value = encode_cbor_value(value)
                        .map_err(|err| StateWriteError::StorageError(Box::new(err)))?;
                    Ok(StateChange::Set {
                        key: key.to_string(),
                        value: cbor_value,
                    })
                }
            })
            .collect::<Result<Vec<StateChange>, StateWriteError>>()?;

        merkle_database
            .update(&cbor_state_changes, false)
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))
    }

    fn compute_state_id(
        &self,
        state_id: &Self::StateId,
        state_changes: &[StateChange],
    ) -> Result<Self::StateId, StateWriteError> {
        let mut merkle_database = MerkleRadixTree::new(self.db.clone(), Some(state_id))
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))?;

        merkle_database
            .set_merkle_root(state_id.to_string())
            .map_err(|err| match err {
                TransactStateDatabaseError::NotFound(msg) => StateWriteError::InvalidStateId(msg),
                _ => StateWriteError::StorageError(Box::new(err)),
            })?;

        let cbor_state_changes = state_changes
            .iter()
            .map(|state_change| match state_change {
                StateChange::Delete { .. } => Ok(state_change.clone()),
                StateChange::Set { key, value } => {
                    let cbor_value = encode_cbor_value(value)
                        .map_err(|err| StateWriteError::StorageError(Box::new(err)))?;
                    Ok(StateChange::Set {
                        key: key.to_string(),
                        value: cbor_value,
                    })
                }
            })
            .collect::<Result<Vec<StateChange>, StateWriteError>>()?;

        merkle_database
            .update(&cbor_state_changes, true)
            .map_err(|err| StateWriteError::StorageError(Box::new(err)))
    }
}

impl Read for CborMerkleState {
    type StateId = String;
    type Key = String;
    type Value = Vec<u8>;

    fn get(
        &self,
        state_id: &Self::StateId,
        keys: &[Self::Key],
    ) -> Result<HashMap<Self::Key, Self::Value>, StateReadError> {
        let mut merkle_database = MerkleRadixTree::new(self.db.clone(), Some(state_id))
            .map_err(|err| StateReadError::StorageError(Box::new(err)))?;

        merkle_database
            .set_merkle_root(state_id.to_string())
            .map_err(|err| match err {
                TransactStateDatabaseError::NotFound(msg) => StateReadError::InvalidStateId(msg),
                _ => StateReadError::StorageError(Box::new(err)),
            })?;
        keys.iter().try_fold(HashMap::new(), |mut result, key| {
            let value = match merkle_database.get_value(key) {
                Ok(value) => Ok(value),
                Err(err) => match err {
                    TransactStateDatabaseError::NotFound(_) => Ok(None),
                    _ => Err(StateReadError::StorageError(Box::new(err))),
                },
            }?;
            if let Some(value) = value {
                result.insert(
                    key.to_string(),
                    decode_cbor_value(&value)
                        .map_err(|err| StateReadError::StorageError(Box::new(err)))?,
                );
            }
            Ok(result)
        })
    }

    fn clone_box(&self) -> Box<dyn Read<StateId = String, Key = String, Value = Vec<u8>>> {
        Box::new(Clone::clone(self))
    }
}

pub struct DecodedMerkleStateReader {
    merkle_database: MerkleRadixTree,
}

impl DecodedMerkleStateReader {
    pub fn new(merkle_database: MerkleRadixTree) -> Self {
        Self { merkle_database }
    }
}

impl StateReader for DecodedMerkleStateReader {
    fn contains(&self, address: &str) -> Result<bool, StateDatabaseError> {
        self.merkle_database
            .contains(address)
            .map_err(StateDatabaseError::from)
    }

    fn get(&self, address: &str) -> Result<Option<Vec<u8>>, StateDatabaseError> {
        Ok(
            match self
                .merkle_database
                .get_value(address)
                .map_err(StateDatabaseError::from)?
            {
                Some(bytes) => Some(decode_cbor_value(&bytes)?),
                None => None,
            },
        )
    }

    fn leaves(&self, prefix: Option<&str>) -> Result<Box<StateIter>, StateDatabaseError> {
        Ok(Box::new(
            self.merkle_database
                .leaves(prefix)
                .map_err(StateDatabaseError::from)?
                .map(|value| {
                    value
                        .map_err(StateDatabaseError::from)
                        .and_then(|(address, bytes)| {
                            decode_cbor_value(&bytes).map(|new_bytes| (address, new_bytes))
                        })
                }),
        ))
    }
}

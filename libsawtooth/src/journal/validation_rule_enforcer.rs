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

/// Enforces block validation rules
use std::convert::TryFrom;
use std::str::FromStr;

use transact::protocol::{batch::Batch, transaction::Transaction};

use crate::state::settings_view::SettingsView;

const BLOCK_VALIDATION_RULES: &str = "sawtooth.validator.block_validation_rules";

/// Reads the block validation rules in state and enforces that blocks conform to those rules
pub struct ValidationRuleEnforcer {
    rules: Vec<Rule>,
    local_signer_key: Vec<u8>,
    txn_info: Vec<TxnInfo>,
}

impl ValidationRuleEnforcer {
    /// Creates a new validation rule enforcer by reading the rules from state
    ///
    /// # Arguments
    ///
    /// * `settings_view` - The view of state used to read the block validation rules setting
    /// * `local_signer_key` - The public key of the node that produced the block being validated;
    ///   it is expected to be the signer of local transactions
    pub fn new(
        settings_view: &SettingsView,
        local_signer_key: Vec<u8>,
    ) -> Result<Self, ValidationRuleEnforcerError> {
        let rules = settings_view
            .get_setting_str(BLOCK_VALIDATION_RULES, None)
            .map_err(|err| ValidationRuleEnforcerError::Internal(err.to_string()))?
            .map(|rules_str| parse_rules(&rules_str))
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            rules,
            local_signer_key,
            txn_info: vec![],
        })
    }

    /// Adds the given batches to a running-list and returns a boolean to indicate if all batches
    /// received so-far follow the rules.
    ///
    /// If not enough batches/transactions have been added to verify rules based on the position of
    /// batches/transactions, those rules are ignored.
    pub fn add_batches<'a, I: IntoIterator<Item = &'a Batch>>(
        &mut self,
        batches: I,
    ) -> Result<bool, ValidationRuleEnforcerError> {
        if self.rules.is_empty() {
            return Ok(true);
        }

        // Store the info from all transactions
        batches
            .into_iter()
            .flat_map(|batch| batch.transactions())
            .cloned()
            .try_for_each(|txn| {
                self.txn_info.push(TxnInfo::try_from(txn)?);
                Ok(())
            })?;

        Ok(self.validate(false))
    }

    /// Returns whether or not the added batches follow the rules. If `final_validation` is true,
    /// all position-based rules will be enforced; if there is no batch/transaction at the position
    /// required by a rule, validation will fail.
    pub fn validate(&self, final_validation: bool) -> bool {
        self.rules
            .iter()
            .all(|rule| rule.validate(&self.txn_info, &self.local_signer_key, final_validation))
    }
}

/// Parses the whole rules string, which is in the form "<rule1>;<rule2>;*"
fn parse_rules(rules_str: &str) -> Result<Vec<Rule>, ValidationRuleEnforcerError> {
    if rules_str.is_empty() {
        Ok(vec![])
    } else {
        rules_str.split(';').map(Rule::from_str).collect()
    }
}

/// Native representation of the validation rules
#[derive(Debug)]
enum Rule {
    /// Only N (`limit`) of transaction type X (`family_name`) may be included in a block
    NofX { family_name: String, limit: usize },
    /// A transaction of type X (`family_name`) must be in the list of transactions at Y
    /// (`position`)
    XatY {
        family_name: String,
        position: usize,
    },
    /// The transactions at the given `indices` must be signed by the same key that signed the block
    Local { indices: Vec<usize> },
}

impl Rule {
    /// Verifies the rule is not violated by the given list of transactions
    ///
    /// # Arguments
    ///
    /// * `txn_info` - The list of transactions (reduced to only relevant info) to validate
    /// * `local_signer_key` - The key used for validating the `Local` rule
    /// * `final_validation` - If `true`, the `XatY` and `Local` rules will fail when there is no
    ///   transactions at the required position
    pub fn validate(
        &self,
        txn_info: &[TxnInfo],
        local_signer_key: &[u8],
        final_validation: bool,
    ) -> bool {
        match self {
            Self::NofX { family_name, limit } => {
                let count = txn_info
                    .iter()
                    .filter(|info| &info.family_name == family_name)
                    .count();
                if count > *limit {
                    debug!(
                        "Found {} transactions of type {}; only {} are allowed",
                        count, family_name, limit
                    );
                    false
                } else {
                    true
                }
            }
            Self::XatY {
                family_name,
                position,
            } => {
                let txn_family_name = match txn_info.get(*position) {
                    Some(info) => &info.family_name,
                    None if final_validation => {
                        debug!(
                            "Transaction at position {} is required by XatY rule",
                            position
                        );
                        return false;
                    }
                    None => return true, // Haven't gotten txn at position Y yet
                };
                if txn_family_name != family_name {
                    debug!(
                        "Transaction at position {} is not of the correct type; expected {}, \
                         found {}",
                        position, family_name, txn_info[*position].family_name
                    );
                    false
                } else {
                    true
                }
            }
            Self::Local { indices } => {
                for index in indices {
                    let signer_key = match txn_info.get(*index) {
                        Some(info) => info.signer_public_key.as_slice(),
                        None if final_validation => {
                            debug!("Transaction at index {} is required by local rule", index);
                            return false;
                        }
                        None => return true, // Haven't gotten txn at this index yet
                    };
                    if signer_key != local_signer_key {
                        debug!(
                            "Transaction at position {} is not signed by the local key {}",
                            index,
                            hex::encode(local_signer_key)
                        );
                        return false;
                    }
                }
                true
            }
        }
    }
}

impl FromStr for Rule {
    type Err = ValidationRuleEnforcerError;

    /// Parses a rule string, which is in the form "<rule_type>:<rule_arg1>,<rule_arg2>,*"
    fn from_str(rule_str: &str) -> Result<Self, Self::Err> {
        let mut rule_parts = rule_str.split(':');

        let rule_type = rule_parts.next().expect("split cannot return empty iter");

        let rule_args = rule_parts
            .next()
            .ok_or_else(|| {
                ValidationRuleEnforcerError::InvalidRule(format!(
                    "empty arguments string for rule: {}",
                    rule_str
                ))
            })?
            .split(',')
            .map(|arg| {
                if arg.is_empty() {
                    Err(ValidationRuleEnforcerError::InvalidRule(format!(
                        "empty argument for rule: {}",
                        rule_str
                    )))
                } else {
                    Ok(arg.to_string())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if rule_args.is_empty() {
            return Err(ValidationRuleEnforcerError::InvalidRule(format!(
                "no arguments provided for rule: {}",
                rule_str
            )));
        }

        match rule_type {
            // The "NofX" rule has two arguments: a limit (the integer that indicates the max
            // number of transactions) and a family name (the name of the transaction family that
            // is being limited).
            //
            // Example: "NofX:2,intkey" means only 2 intkey transactions are allowed per block.
            "NofX" => {
                let limit = rule_args
                    .get(0)
                    .ok_or_else(|| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found NofX rule with no arguments".into(),
                        )
                    })?
                    .trim()
                    .parse()
                    .map_err(|_| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found NofX rule with non-integer limit".into(),
                        )
                    })?;

                let family_name = rule_args
                    .get(1)
                    .ok_or_else(|| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found NofX rule with no family name argument".into(),
                        )
                    })?
                    .trim()
                    .to_string();

                Ok(Rule::NofX { family_name, limit })
            }
            // The "XatY" rule has two arguments: a family name (the name of the transaction family
            // that must be present) and a position (the position in the list of transactions where
            // a transaction of the right type must be present). The first transaction in a block
            // has index 0. If the position is larger than the number of transactions in a block,
            // then there would not be a transaction of type X at Y; this will fail when performing
            // "final validation".
            //
            // Example: "XatY:intkey,0" means the first transaction in a block must be an intkey
            // transaction.
            "XatY" => {
                let family_name = rule_args
                    .get(0)
                    .ok_or_else(|| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found XatY rule with no arguments".into(),
                        )
                    })?
                    .trim()
                    .to_string();

                let position = rule_args
                    .get(1)
                    .ok_or_else(|| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found XatY rule with no position argument".into(),
                        )
                    })?
                    .trim()
                    .parse()
                    .map_err(|_| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found XatY rule with non-integer position".into(),
                        )
                    })?;

                Ok(Rule::XatY {
                    family_name,
                    position,
                })
            }
            // The "Local" rules has a variable number of arguments; each one is an index at which
            // there must be a transaction that is signed by the same key that signed the block.
            // This rule is useful in combination with the other rules to ensure a client is not
            // submitting transactions that should only be injected by the node that produced the
            // block.
            "local" => {
                let indices = rule_args
                    .iter()
                    .map(|s| s.trim().parse())
                    .collect::<Result<_, _>>()
                    .map_err(|_| {
                        ValidationRuleEnforcerError::InvalidRule(
                            "found local rule with non-integer index".into(),
                        )
                    })?;
                Ok(Rule::Local { indices })
            }
            rule_type => Err(ValidationRuleEnforcerError::InvalidRule(format!(
                "unknown rule type: {}",
                rule_type
            ))),
        }
    }
}

/// Minimal set of information needed to verify the transactions in a block
struct TxnInfo {
    family_name: String,
    signer_public_key: Vec<u8>,
}

impl TryFrom<Transaction> for TxnInfo {
    type Error = ValidationRuleEnforcerError;

    fn try_from(txn: Transaction) -> Result<Self, Self::Error> {
        txn.into_pair()
            .map(|txn_pair| Self {
                family_name: txn_pair.header().family_name().into(),
                signer_public_key: txn_pair.header().signer_public_key().into(),
            })
            .map_err(|err| {
                ValidationRuleEnforcerError::InvalidBatches(format!(
                    "failed to deserialize transaction header: {}",
                    err
                ))
            })
    }
}

/// Errors that may occur when validating a block
#[derive(Debug)]
pub enum ValidationRuleEnforcerError {
    Internal(String),
    InvalidBatches(String),
    InvalidRule(String),
}

impl std::error::Error for ValidationRuleEnforcerError {}

impl std::fmt::Display for ValidationRuleEnforcerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Internal(msg) => f.write_str(msg),
            Self::InvalidBatches(msg) => write!(f, "invalid batches were provided: {}", msg),
            Self::InvalidRule(msg) => {
                write!(f, "block validation rule configuration is invalid: {}", msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cylinder::{hash::HashSigner, Signer};
    use transact::protocol::{
        batch::{Batch, BatchBuilder},
        transaction::{HashMethod, TransactionBuilder},
    };

    /// Test that if no validation rules are set, the block is valid.
    #[test]
    fn test_no_setting() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: vec![],
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };

        assert!(enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(enforcer.validate(true));
    }

    /// Test that if NofX Rule is set, the validation rule is checked
    /// correctly. Test:
    ///     1. Valid Block, has one or less intkey transactions.
    ///     2. Invalid Block, to many intkey transactions.
    #[test]
    fn test_n_of_x() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("NofX:1,intkey").expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(enforcer.validate(true));

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("NofX:0,intkey").expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(!enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(!enforcer.validate(true));
    }

    /// Test that if XatY Rule is set, the validation rule is checked
    /// correctly. Test:
    ///     1. Valid Block, has intkey at the 0th position.
    ///     2. Invalid Block, does not have an blockinfo txn at the 0th postion
    #[test]
    fn test_x_at_y() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("XatY:intkey,0").expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(enforcer.validate(true));

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("XatY:blockinfo,0").expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(!enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(!enforcer.validate(true));
    }

    /// Test that if local Rule is set, the validation rule is checked
    /// correctly. Test:
    ///     1. Valid Block, first transaction is signed by the expected signer.
    ///     2. Invalid Block, first transaction is not signed by the expected
    ///        signer.
    #[test]
    fn test_local() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("local:0").expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(enforcer.validate(true));

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("local:0").expect("Failed to parse rules"),
            local_signer_key: b"another_pub_key".to_vec(),
            txn_info: vec![],
        };
        assert!(!enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(!enforcer.validate(true));
    }

    /// Test that if multiple rules are set, they are all checked correctly.
    /// Block should be valid.
    #[test]
    fn test_all_at_once() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("NofX:1,intkey;XatY:intkey,0;local:0")
                .expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(enforcer.validate(true));
    }

    /// Test that if multiple rules are set, they are all checked correctly.
    /// Block is invalid, because there are too many intkey transactions
    #[test]
    fn test_all_at_once_bad_number_of_intkey() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("NofX:0,intkey;XatY:intkey,0;local:0")
                .expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(!enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(!enforcer.validate(true));
    }

    /// Test that if multiple rules are set, they are all checked correctly.
    /// Block is invalid, there is not a blockinfo transactions at the 0th
    /// position.
    #[test]
    fn test_all_at_once_bad_family_at_index() {
        let signer = HashSigner;
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batches = make_batches(&["intkey"], &signer);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("NofX:1,intkey;XatY:blockinfo,0;local:0")
                .expect("Failed to parse rules"),
            local_signer_key: pub_key.as_slice().into(),
            txn_info: vec![],
        };
        assert!(!enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(!enforcer.validate(true));
    }

    /// Test that if multiple rules are set, they are all checked correctly.
    /// Expected signer is invalid, transaction at the 0th position is not
    /// signed by the expected signer.
    #[test]
    fn test_all_at_once_signer_key() {
        let batches = make_batches(&["intkey"], &HashSigner);

        let mut enforcer = ValidationRuleEnforcer {
            rules: parse_rules("NofX:1,intkey;XatY:intkey,0;local:0")
                .expect("Failed to parse rules"),
            local_signer_key: b"not_same_pubkey".to_vec(),
            txn_info: vec![],
        };
        assert!(!enforcer
            .add_batches(&batches)
            .expect("Failed to add batches"));
        assert!(!enforcer.validate(true));
    }

    fn make_batches(families: &[&str], signer: &dyn Signer) -> Vec<Batch> {
        let transactions = families
            .iter()
            .map(|family| {
                TransactionBuilder::new()
                    .with_family_name((*family).into())
                    .with_family_version("0.test".into())
                    .with_inputs(vec![])
                    .with_outputs(vec![])
                    .with_payload_hash_method(HashMethod::SHA512)
                    .with_payload(vec![])
                    .build(signer)
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to build transactions");

        vec![BatchBuilder::new()
            .with_transactions(transactions)
            .build(signer)
            .expect("Failed to build batch")]
    }
}

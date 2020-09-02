// Copyright 2018-2020 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Errors for the block publisher

use std::error::Error;

use crate::state::{error::StateDatabaseError, settings_view::SettingsViewError};

/// Errors that can occur in the block publisher
#[derive(Debug)]
pub enum BlockPublisherError {
    /// An internal error that can't be handled externally
    Internal(String),
}

impl Error for BlockPublisherError {}

impl std::fmt::Display for BlockPublisherError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Internal(msg) => f.write_str(msg),
        }
    }
}

impl From<StateDatabaseError> for BlockPublisherError {
    fn from(err: StateDatabaseError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<SettingsViewError> for BlockPublisherError {
    fn from(err: SettingsViewError) -> Self {
        Self::Internal(err.to_string())
    }
}
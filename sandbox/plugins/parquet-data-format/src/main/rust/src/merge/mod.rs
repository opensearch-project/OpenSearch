/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

mod context;
mod cursor;
pub mod error;
pub mod heap;
pub mod io_task;
pub mod schema;
mod sorted;
mod unsorted;

pub use error::{MergeError, MergeResult};
pub use sorted::merge_sorted;
pub use unsorted::merge_unsorted;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
mod tests;

pub mod writer;
pub mod native_settings;
pub mod field_config;
pub mod writer_properties_builder;
pub mod rate_limited_writer;
pub mod parquet_merge_stream;
mod jni;

pub use native_settings::NativeSettings;
pub use field_config::FieldConfig;
pub use writer_properties_builder::WriterPropertiesBuilder;
pub use writer::SETTINGS_STORE;
pub use native_bridge_spi::{log_info, log_error, log_debug};

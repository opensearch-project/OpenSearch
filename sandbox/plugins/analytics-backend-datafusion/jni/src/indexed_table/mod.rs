/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Index-accelerated parquet queries for DataFusion.

Core modules:
- `index` — traits: ShardSearcher, RowGroupDocsCollector, BitsetMode
- `table_provider` — IndexedTableProvider (DataFusion TableProvider impl)
- `stream` — IndexedExec streaming execution per segment
- `partitioning` — row-group-aligned partition assignment
- `page_pruner` — page-level pruning using parquet statistics
- `parquet_bridge` — DataFusion parquet API isolation layer
- `metrics` — execution plan metrics
- `jni_searcher` — JniShardSearcher (JNI callbacks to Java)
- `jni_helpers` — build_segments utility

Tree query modules:
- `bool_tree` — BoolNode enum, wire-format deserialization, De Morgan's normalization
- `tree_eval` — Two-phase evaluation: prefetch (Roaring bitmaps) + on-batch (Arrow kernels)
- `tree_provider` — TreeIndexedTableProvider (DataFusion TableProvider for tree queries)
- `tree_stream` — TreeIndexedExec / TreeIndexedStream streaming execution
- `jni_tree_searcher` — JniTreeShardSearcher (JNI callbacks via FilterTreeCallbackBridge)
**/

pub mod index;
pub mod metrics;
mod page_pruner;
pub mod parquet_bridge;
pub mod partitioning;
pub mod stream;
pub mod table_provider;
pub mod jni_searcher;
pub mod jni_helpers;

// Tree query modules
pub mod bool_tree;
pub mod tree_eval;
pub mod tree_provider;
pub mod tree_stream;
pub mod jni_tree_searcher;
pub mod substrait_to_tree;

pub use index::{BitsetMode, ShardSearcher};
pub use jni_searcher::JniShardSearcher;
pub use table_provider::{IndexedTableConfig, IndexedTableProvider};
pub use jni_helpers::build_segments;
pub use bool_tree::BoolNode;
pub use tree_provider::{TreeIndexedTableConfig, TreeIndexedTableProvider};
pub use jni_tree_searcher::JniTreeShardSearcher;

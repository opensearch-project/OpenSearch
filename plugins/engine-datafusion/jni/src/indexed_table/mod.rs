/**
Index-accelerated parquet queries for DataFusion.

Copied from the indexed_table POC crate for integration into engine-datafusion.

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

pub use index::{BitsetMode, ShardSearcher};
pub use jni_searcher::JniShardSearcher;
pub use table_provider::{IndexedTableConfig, IndexedTableProvider};
pub use jni_helpers::build_segments;

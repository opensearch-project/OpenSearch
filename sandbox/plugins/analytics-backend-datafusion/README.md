# analytics-backend-datafusion

DataFusion native execution engine plugin for the OpenSearch analytics framework. Implements `SearchBackEndPlugin` (server SPI for shard-level reader management) and `AnalyticsSearchBackendPlugin` (analytics-framework SPI for query execution) to execute query plan fragments via a Rust/DataFusion runtime over JNI.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  analytics-engine (hub)                                         │
│  ExtensiblePlugin — discovers AnalyticsSearchBackendPlugin SPIs │
│  Routes query plan fragments to back-ends via DefaultPlanExecutor│
└──────────────┬──────────────────────────────────────────────────┘
               │ SPI (extendedPlugins = ['analytics-engine'])
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  analytics-backend-datafusion                                   │
│                                                                 │
│  DataFusionPlugin                                               │
│    ├── createComponents() → DataFusionService (node-level)      │
│    ├── searcher(ExecutionContext) → DatafusionSearchExecEngine   │
│    └── createReaderManager(format, shardPath)                   │
│            → DatafusionReaderManager                            │
│                                                                 │
│  Execution flow:                                                │
│    ExecutionContext                                              │
│      → DatafusionSearchExecEngine.prepare()                     │
│          (RelNode → Substrait bytes → DatafusionQuery)          │
│      → DatafusionSearchExecEngine.execute()                     │
│          → DatafusionSearcher.search(DatafusionContext)          │
│              → NativeBridge.executeQuery() [JNI]                │
│          → DatafusionResultStream (Arrow record batches)        │
│                                                                 │
│  Native layer (JNI):                                            │
│    NativeBridge ──→ rust       │
│      createDatafusionReader / closeDatafusionReader              │
│      createGlobalRuntime / closeGlobalRuntime                   │
│      executeQuery / streamNext / streamClose                    │
└─────────────────────────────────────────────────────────────────┘
```

## Key classes

| Class | Role                                                                                                                                                                                                                                   |
|---|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DataFusionPlugin` | Plugin entry point. Implements `SearchBackEndPlugin` (server SPI — provides `createReaderManager` for shard-level data access) and `AnalyticsSearchBackendPlugin` (analytics-framework SPI — provides `searcher` for query execution).
| `DataFusionService` | Node-level lifecycle service. Loads the native JNI library, creates the Tokio runtime , global runtime environment and memory pool. Shared by all per-shard engines.                                                                   |
| `DatafusionSearchExecEngine` | Per-query engine. `prepare()` converts the Calcite `RelNode` to a Substrait plan; `execute()` delegates to `DatafusionSearcher` and returns a `DatafusionResultStream`.                                                                |
| `DatafusionContext` | Execution context carrying the query plan, `DatafusionSearcher`, optional `IndexFilterTree`, native runtime pointer, result `StreamHandle` etc. Implements `SearchExecutionContext<DatafusionSearcher>`.                               |
| `DatafusionSearcher` | Executes the Substrait plan against a native reader via `NativeBridge.executeQuery()`. Owns no resources - reader lifecycle is managed by `DatafusionReaderManager`.                                                                   |
| `DatafusionReader` | Per-shard point-in-time snapshot of data files. Wraps a `ReaderHandle`.                                                                                                                                                                |
| `DatafusionReaderManager` | Manages `DatafusionReader` lifecycle per `CatalogSnapshot`. Handles refresh (swap in new reader) and deletion (close old reader).                                                                                                      |
| `NativeRuntimeHandle` | Thread-safe wrapper around the native runtime pointer with liveness checks.                                                                                                                                                            |

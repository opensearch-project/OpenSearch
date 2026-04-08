# TLA+ Specification: Parquet Write Path

Formal specification of the parquet-data-format plugin's write path, modelling the
document ingestion → Arrow VSR batching → native Parquet file generation pipeline.

## What It Verifies

| Property | Kind | Description |
|---|---|---|
| `VSRStateMachineInvariant` | Safety | VSR states follow ACTIVE → FROZEN → CLOSED |
| `NoDoubleWrite` | Safety | Each VSR batch is written to native exactly once |
| `NoMemoryLeak` | Safety | Every allocated VSR is accounted for (active, frozen, or in-flight) |
| `RowCountConservation` | Safety | Ingested rows = written rows + buffered rows at all times |
| `NativeWriterMonotonicity` | Safety | Writer phase only moves forward (OPEN → FLUSHED → SYNCED) |
| `NoWriteAfterFlush` | Safety | No native writes occur after the writer is finalized |
| `FrozenSlotConsistency` | Safety | Empty VSRs are never frozen |
| `BackgroundWriteSafety` | Safety | Background write always targets the frozen slot's VSR |
| `AllDocsEventuallyWritten` | Liveness | All ingested documents eventually reach the Parquet file |
| `PipelineTerminates` | Liveness | The pipeline always reaches the DONE state |

## Architecture Mapping

| TLA+ Concept | Java Class |
|---|---|
| `activeVSR` / `frozenVSR` | `VSRPool` double-buffer slots |
| `ManagedVSR` state machine | `ManagedVSR` + `VSRState` enum |
| `RotateVSR` | `VSRPool.maybeRotateActiveVSR()` |
| `SubmitBackgroundWrite` | `threadPool.submit(writeTask)` in `VSRManager` |
| `CompleteBackgroundWrite` | Background `Runnable` (export → write → close → unset) |
| `BeginFlush` | `VSRManager.flush()` |
| `Sync` | `NativeParquetWriter.sync()` → `RustBridge.syncToDisk()` |
| `nativeWriterPhase` | `NativeParquetWriter` lifecycle (writerFlushed flag) |

## Running

Install the [TLA+ tools](https://github.com/tlaplus/tlaplus/releases) or use the
VS Code TLA+ extension.

```bash
# Exhaustive model check with default small constants (MaxDocs=6, MaxRowsPerVSR=3)
tlc ParquetWritePath.tla -config ParquetWritePath.cfg

# Larger state space (slower, more thorough)
tlc ParquetWritePath.tla -config ParquetWritePath.cfg -workers auto \
    -D MaxDocs=10 -D MaxRowsPerVSR=4
```

The default configuration (`MaxDocs=6`, `MaxRowsPerVSR=3`) exercises two full VSR
rotations plus a partial final flush, which covers the core rotation and flush logic.

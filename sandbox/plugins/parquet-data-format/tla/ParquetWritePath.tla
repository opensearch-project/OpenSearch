--------------------------- MODULE ParquetWritePath ---------------------------
(*
 * TLA+ specification for the Parquet Data Format Plugin write path.
 *
 * Models the end-to-end flow from document ingestion through Arrow VSR
 * batching to native Parquet file generation, verifying:
 *   - VSR state machine correctness (ACTIVE → FROZEN → CLOSED)
 *   - Double-buffered VSR pool rotation safety
 *   - Background write coordination (no lost batches, no double-writes)
 *   - Flush/sync ordering guarantees
 *   - Memory lifecycle (no leaks, no use-after-close)
 *
 * Abstraction choices:
 *   - Documents are modelled as opaque units with a row count of 1.
 *   - Arrow memory allocation is abstracted to a simple counter.
 *   - The native Rust writer is modelled as an append-only sequence.
 *   - Thread pool submission is modelled as a non-deterministic step.
 *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxDocs,          \* Total documents to ingest (bounds the model)
    MaxRowsPerVSR     \* Row threshold triggering VSR rotation

ASSUME MaxDocs \in Nat /\ MaxDocs > 0
ASSUME MaxRowsPerVSR \in Nat /\ MaxRowsPerVSR > 0

---------------------------------------------------------------------------
(* VSR lifecycle states — mirrors VSRState.java *)
VSRStates == {"ACTIVE", "FROZEN", "CLOSED"}

(* Writer lifecycle phases *)
WriterPhases == {"OPEN", "FLUSHED", "SYNCED"}

(* Overall pipeline phases *)
PipelinePhases == {"INGESTING", "FLUSHING", "SYNCING", "DONE"}

VARIABLES
    (* --- VSR Pool (models VSRPool.java double-buffer) --- *)
    activeVSR,        \* Record: [id: Nat, state: VSRStates, rows: Nat] or NULL
    frozenVSR,        \* Record: [id: Nat, state: VSRStates, rows: Nat] or NULL
    vsrCounter,       \* Monotonic VSR id counter

    (* --- Background write state (models VSRManager async path) --- *)
    pendingWrite,     \* The frozen VSR id being written in background, or 0
    bgWriteDone,      \* Whether the background write has completed

    (* --- Native writer (models NativeParquetWriter.java) --- *)
    nativeWriterPhase,    \* Element of WriterPhases
    writtenBatches,       \* Sequence of [vsrId: Nat, rows: Nat] written to native
    totalRowsWritten,     \* Running total of rows sent to native writer

    (* --- Pipeline control --- *)
    docsIngested,     \* Number of documents added so far
    pipelinePhase,    \* Element of PipelinePhases

    (* --- Resource tracking --- *)
    allocatedVSRs,    \* Set of VSR ids whose Arrow memory is still allocated
    closedVSRs        \* Set of VSR ids that have been fully closed

vars == <<activeVSR, frozenVSR, vsrCounter,
          pendingWrite, bgWriteDone,
          nativeWriterPhase, writtenBatches, totalRowsWritten,
          docsIngested, pipelinePhase,
          allocatedVSRs, closedVSRs>>

---------------------------------------------------------------------------
(* Helper: create a fresh VSR record *)
NewVSR(id) == [id |-> id, state |-> "ACTIVE", rows |-> 0]

NULL == [id |-> 0, state |-> "NONE", rows |-> 0]

IsNull(vsr) == vsr.id = 0

---------------------------------------------------------------------------
(* INITIAL STATE *)
Init ==
    /\ vsrCounter = 1
    /\ activeVSR = NewVSR(1)
    /\ frozenVSR = NULL
    /\ pendingWrite = 0
    /\ bgWriteDone = FALSE
    /\ nativeWriterPhase = "OPEN"
    /\ writtenBatches = <<>>
    /\ totalRowsWritten = 0
    /\ docsIngested = 0
    /\ pipelinePhase = "INGESTING"
    /\ allocatedVSRs = {1}
    /\ closedVSRs = {}

---------------------------------------------------------------------------
(*
 * ACTION: AddDocument
 *
 * Models VSRManager.addDocument() — writes one document into the active VSR.
 * Precondition: pipeline is ingesting, active VSR exists and is ACTIVE,
 *               and we haven't reached MaxDocs yet.
 *)
AddDocument ==
    /\ pipelinePhase = "INGESTING"
    /\ docsIngested < MaxDocs
    /\ ~IsNull(activeVSR)
    /\ activeVSR.state = "ACTIVE"
    /\ activeVSR.rows < MaxRowsPerVSR  \* rotation must happen first if at threshold
    /\ activeVSR' = [activeVSR EXCEPT !.rows = activeVSR.rows + 1]
    /\ docsIngested' = docsIngested + 1
    /\ UNCHANGED <<frozenVSR, vsrCounter, pendingWrite, bgWriteDone,
                   nativeWriterPhase, writtenBatches, totalRowsWritten,
                   pipelinePhase, allocatedVSRs, closedVSRs>>

---------------------------------------------------------------------------
(*
 * ACTION: RotateVSR
 *
 * Models VSRPool.maybeRotateActiveVSR() — when the active VSR reaches the
 * row threshold AND the frozen slot is empty, freeze the active VSR and
 * create a new one.
 *
 * This corresponds to the guard in VSRPool:
 *   if (current.getRowCount() >= maxRowsPerVSR && frozenVSR.get() == null)
 *)
RotateVSR ==
    /\ pipelinePhase = "INGESTING"
    /\ ~IsNull(activeVSR)
    /\ activeVSR.state = "ACTIVE"
    /\ activeVSR.rows >= MaxRowsPerVSR
    /\ IsNull(frozenVSR)                \* frozen slot must be free
    /\ activeVSR.rows > 0               \* don't freeze empty VSRs
    /\ LET frozen == [activeVSR EXCEPT !.state = "FROZEN"]
           newId  == vsrCounter + 1
       IN
       /\ frozenVSR' = frozen
       /\ activeVSR' = NewVSR(newId)
       /\ vsrCounter' = newId
       /\ allocatedVSRs' = allocatedVSRs \union {newId}
       /\ UNCHANGED <<pendingWrite, bgWriteDone,
                      nativeWriterPhase, writtenBatches, totalRowsWritten,
                      docsIngested, pipelinePhase, closedVSRs>>

---------------------------------------------------------------------------
(*
 * ACTION: SubmitBackgroundWrite
 *
 * Models the threadPool.submit(writeTask) call in VSRManager.maybeRotateActiveVSR().
 * Takes the frozen VSR and submits it for background native write.
 * Precondition: frozen slot is occupied, no pending write in flight.
 *)
SubmitBackgroundWrite ==
    /\ pipelinePhase = "INGESTING"
    /\ ~IsNull(frozenVSR)
    /\ frozenVSR.state = "FROZEN"
    /\ pendingWrite = 0
    /\ pendingWrite' = frozenVSR.id
    /\ bgWriteDone' = FALSE
    /\ UNCHANGED <<activeVSR, frozenVSR, vsrCounter,
                   nativeWriterPhase, writtenBatches, totalRowsWritten,
                   docsIngested, pipelinePhase, allocatedVSRs, closedVSRs>>

---------------------------------------------------------------------------
(*
 * ACTION: CompleteBackgroundWrite
 *
 * Models the background Runnable completing: export → native write → close VSR → unset frozen.
 * This is the writeTask lambda in VSRManager.maybeRotateActiveVSR().
 *)
CompleteBackgroundWrite ==
    /\ pendingWrite # 0
    /\ bgWriteDone = FALSE
    /\ nativeWriterPhase = "OPEN"
    /\ ~IsNull(frozenVSR)
    /\ frozenVSR.id = pendingWrite
    /\ frozenVSR.state = "FROZEN"
    \* Native write: append batch to written sequence
    /\ writtenBatches' = Append(writtenBatches, [vsrId |-> frozenVSR.id, rows |-> frozenVSR.rows])
    /\ totalRowsWritten' = totalRowsWritten + frozenVSR.rows
    \* Close the VSR (FROZEN → CLOSED) and release memory
    /\ closedVSRs' = closedVSRs \union {frozenVSR.id}
    /\ allocatedVSRs' = allocatedVSRs \ {frozenVSR.id}
    \* Clear frozen slot
    /\ frozenVSR' = NULL
    /\ bgWriteDone' = TRUE
    /\ pendingWrite' = 0
    /\ UNCHANGED <<activeVSR, vsrCounter, nativeWriterPhase,
                   docsIngested, pipelinePhase>>

---------------------------------------------------------------------------
(*
 * ACTION: BeginFlush
 *
 * Models VSRManager.flush() — transitions from INGESTING to FLUSHING.
 * Waits for any pending background write, then freezes and writes the
 * remaining active VSR to native.
 *
 * Precondition: all docs ingested, no pending background write.
 *)
BeginFlush ==
    /\ pipelinePhase = "INGESTING"
    /\ docsIngested = MaxDocs
    /\ pendingWrite = 0
    /\ ~IsNull(activeVSR)
    /\ activeVSR.state = "ACTIVE"
    /\ activeVSR.rows > 0
    /\ nativeWriterPhase = "OPEN"
    \* Freeze the active VSR
    /\ LET frozen == [activeVSR EXCEPT !.state = "FROZEN"]
       IN
       \* Write to native synchronously
       /\ writtenBatches' = Append(writtenBatches, [vsrId |-> frozen.id, rows |-> frozen.rows])
       /\ totalRowsWritten' = totalRowsWritten + frozen.rows
       \* Close the VSR
       /\ closedVSRs' = closedVSRs \union {frozen.id}
       /\ allocatedVSRs' = allocatedVSRs \ {frozen.id}
       /\ activeVSR' = NULL
    \* Finalize native writer
    /\ nativeWriterPhase' = "FLUSHED"
    /\ pipelinePhase' = "FLUSHING"
    /\ UNCHANGED <<frozenVSR, vsrCounter, pendingWrite, bgWriteDone, docsIngested>>

---------------------------------------------------------------------------
(*
 * ACTION: BeginFlushEmpty
 *
 * Models flush when the active VSR has zero rows (edge case: all docs
 * were already flushed via rotation).
 *)
BeginFlushEmpty ==
    /\ pipelinePhase = "INGESTING"
    /\ docsIngested = MaxDocs
    /\ pendingWrite = 0
    /\ \/ (IsNull(activeVSR))
       \/ (~IsNull(activeVSR) /\ activeVSR.rows = 0)
    /\ nativeWriterPhase = "OPEN"
    \* Close empty active VSR if present
    /\ IF ~IsNull(activeVSR)
       THEN /\ closedVSRs' = closedVSRs \union {activeVSR.id}
            /\ allocatedVSRs' = allocatedVSRs \ {activeVSR.id}
       ELSE /\ closedVSRs' = closedVSRs
            /\ allocatedVSRs' = allocatedVSRs
    /\ activeVSR' = NULL
    /\ nativeWriterPhase' = "FLUSHED"
    /\ pipelinePhase' = "FLUSHING"
    /\ UNCHANGED <<frozenVSR, vsrCounter, pendingWrite, bgWriteDone,
                   writtenBatches, totalRowsWritten, docsIngested>>

---------------------------------------------------------------------------
(*
 * ACTION: Sync
 *
 * Models VSRManager.sync() → NativeParquetWriter.sync() → RustBridge.syncToDisk().
 * Transitions the native writer to SYNCED and the pipeline to DONE.
 *)
Sync ==
    /\ pipelinePhase = "FLUSHING"
    /\ nativeWriterPhase = "FLUSHED"
    /\ pendingWrite = 0
    /\ nativeWriterPhase' = "SYNCED"
    /\ pipelinePhase' = "SYNCING"
    /\ UNCHANGED <<activeVSR, frozenVSR, vsrCounter, pendingWrite, bgWriteDone,
                   writtenBatches, totalRowsWritten, docsIngested,
                   allocatedVSRs, closedVSRs>>

---------------------------------------------------------------------------
(*
 * ACTION: Done
 *
 * Terminal transition after sync completes.
 *)
Done ==
    /\ pipelinePhase = "SYNCING"
    /\ nativeWriterPhase = "SYNCED"
    /\ pipelinePhase' = "DONE"
    /\ UNCHANGED <<activeVSR, frozenVSR, vsrCounter, pendingWrite, bgWriteDone,
                   nativeWriterPhase, writtenBatches, totalRowsWritten,
                   docsIngested, allocatedVSRs, closedVSRs>>

---------------------------------------------------------------------------
(* NEXT-STATE RELATION *)
Next ==
    \/ AddDocument
    \/ RotateVSR
    \/ SubmitBackgroundWrite
    \/ CompleteBackgroundWrite
    \/ BeginFlush
    \/ BeginFlushEmpty
    \/ Sync
    \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

---------------------------------------------------------------------------
(* ======================== INVARIANTS ======================== *)

(*
 * INV1: VSR State Machine
 * A VSR can only be in a valid state, and transitions are monotonic:
 *   ACTIVE → FROZEN → CLOSED.
 * No VSR appears in both active and frozen slots simultaneously.
 *)
VSRStateMachineInvariant ==
    /\ (~IsNull(activeVSR) => activeVSR.state = "ACTIVE")
    /\ (~IsNull(frozenVSR) => frozenVSR.state = "FROZEN")
    /\ (~IsNull(activeVSR) /\ ~IsNull(frozenVSR) => activeVSR.id # frozenVSR.id)

(*
 * INV2: No Double Write
 * Each VSR id appears at most once in the written batches sequence.
 *)
NoDoubleWrite ==
    \A i, j \in 1..Len(writtenBatches) :
        (i # j) => writtenBatches[i].vsrId # writtenBatches[j].vsrId

(*
 * INV3: No Memory Leak
 * Every allocated VSR is either the active VSR, the frozen VSR,
 * or currently being written in the background.
 * Closed VSRs must not be in the allocated set.
 *)
NoMemoryLeak ==
    /\ allocatedVSRs \intersect closedVSRs = {}
    /\ \A id \in allocatedVSRs :
        \/ (~IsNull(activeVSR) /\ activeVSR.id = id)
        \/ (~IsNull(frozenVSR) /\ frozenVSR.id = id)
        \/ (pendingWrite = id)

(*
 * INV4: Row Count Conservation
 * The total rows written to native plus the rows in the active and frozen
 * VSRs always equals the number of documents ingested.
 *)
RowCountConservation ==
    LET activeRows == IF ~IsNull(activeVSR) THEN activeVSR.rows ELSE 0
        frozenRows == IF ~IsNull(frozenVSR) THEN frozenVSR.rows ELSE 0
    IN
    totalRowsWritten + activeRows + frozenRows = docsIngested

(*
 * INV5: Native Writer Monotonicity
 * The native writer phase only moves forward: OPEN → FLUSHED → SYNCED.
 * Once flushed, no more writes are accepted.
 *)
NativeWriterMonotonicity ==
    /\ (nativeWriterPhase = "FLUSHED" \/ nativeWriterPhase = "SYNCED")
       => pipelinePhase # "INGESTING"
    /\ (nativeWriterPhase = "SYNCED")
       => (pipelinePhase = "SYNCING" \/ pipelinePhase = "DONE")

(*
 * INV6: No Write After Flush
 * If the native writer has been flushed, no new batches can be appended.
 * (Checked implicitly by BeginFlush being the only action that sets FLUSHED,
 *  but stated explicitly for verification.)
 *)
NoWriteAfterFlush ==
    (nativeWriterPhase # "OPEN") => pendingWrite = 0

(*
 * INV7: Frozen Slot Consistency
 * The frozen slot is only occupied when there is data to write.
 * An empty (0-row) VSR is never frozen.
 *)
FrozenSlotConsistency ==
    ~IsNull(frozenVSR) => frozenVSR.rows > 0

(*
 * INV8: Background Write Safety
 * A background write is only in flight when the frozen slot holds the
 * corresponding VSR.
 *)
BackgroundWriteSafety ==
    (pendingWrite # 0) => (~IsNull(frozenVSR) /\ frozenVSR.id = pendingWrite)

---------------------------------------------------------------------------
(* ======================== LIVENESS / TEMPORAL ======================== *)

(*
 * PROP1: All Documents Eventually Written
 * If MaxDocs documents are ingested, the pipeline eventually reaches DONE
 * and all rows are persisted.
 *)
AllDocsEventuallyWritten ==
    <>(pipelinePhase = "DONE" /\ totalRowsWritten = MaxDocs)

(*
 * PROP2: Pipeline Termination
 * The pipeline eventually terminates.
 *)
PipelineTerminates == <>(pipelinePhase = "DONE")

(*
 * PROP3: No Orphaned Memory
 * When the pipeline is done, no Arrow memory remains allocated.
 *)
NoOrphanedMemoryAtEnd ==
    [](pipelinePhase = "DONE" => allocatedVSRs = {} \/ allocatedVSRs = {activeVSR.id})

---------------------------------------------------------------------------
(* ======================== TYPE INVARIANT ======================== *)

TypeOK ==
    /\ vsrCounter \in Nat
    /\ docsIngested \in 0..MaxDocs
    /\ totalRowsWritten \in Nat
    /\ pendingWrite \in Nat
    /\ bgWriteDone \in BOOLEAN
    /\ nativeWriterPhase \in WriterPhases
    /\ pipelinePhase \in PipelinePhases
    /\ allocatedVSRs \subseteq Nat
    /\ closedVSRs \subseteq Nat

=============================================================================

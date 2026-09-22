/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.NestedParquetField;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.fields.core.data.FlatObjectParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.stats.ParquetShardStatsTracker;
import org.opensearch.parquet.writer.FieldValuePair;
import org.opensearch.parquet.writer.MismatchedInputException;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Top-level orchestrator for the Arrow batching → Parquet file generation pipeline.
 *
 * <p>Combines {@link VSRPool} (Arrow batch management) with {@link NativeParquetWriter}
 * (native Rust Parquet writer) to provide a single entry point for document ingestion.
 * Handles the complete flow:
 * <ol>
 *   <li>{@link #addDocument(ParquetDocumentInput)} — transfers document fields into the active
 *       VSR's Arrow vectors, rotating the VSR if the row threshold is reached.</li>
 *   <li>{@link #flush()} — freezes the active VSR, exports it to the native writer,
 *       finalizes the Parquet file, and returns file metadata.</li>
 * </ol>
 *
 * <p>Field values are resolved to their Arrow vector types via {@link ArrowFieldRegistry}
 * during document ingestion.
 *
 * <p>This class is NOT Thread-Safe. External synchronization is required
 * if instances are shared across threads.
 */
public class VSRManager implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(VSRManager.class);

    private final AtomicReference<ManagedVSR> managedVSR = new AtomicReference<>();
    private final String fileName;
    private final IndexSettings indexSettings;
    private final VSRPool vsrPool;
    private final ThreadPool threadPool;
    private final String vsrRotationThread;
    private final long writerGeneration;
    private final ParquetShardStatsTracker stats;
    private volatile Future<?> pendingWrite;
    private final NativeParquetWriter writer;
    private final int ROTATION_TIMEOUT = 120;
    private LongAdder rowCount = new LongAdder();
    private long acceptedRows = 0L;

    /**
     * Creates a new VSRManager with asynchronous background writes (production default).
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        long writerGeneration,
        ParquetShardStatsTracker stats
    ) {
        this(fileName, indexSettings, schema, bufferPool, maxRowsPerVSR, threadPool, true, writerGeneration, stats);
    }

    /**
     * Creates a new VSRManager with asynchronous background writes and no stats collection.
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        long writerGeneration
    ) {
        this(
            fileName,
            indexSettings,
            schema,
            bufferPool,
            maxRowsPerVSR,
            threadPool,
            true,
            writerGeneration,
            new ParquetShardStatsTracker()
        );
    }

    /**
     * Creates a new VSRManager without stats collection.
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        boolean runAsync,
        long writerGeneration
    ) {
        this(
            fileName,
            indexSettings,
            schema,
            bufferPool,
            maxRowsPerVSR,
            threadPool,
            runAsync,
            writerGeneration,
            new ParquetShardStatsTracker()
        );
    }

    /**
     * Creates a new VSRManager.
     *
     * @param fileName output Parquet file path
     * @param indexSettings the index settings (sort config is read from here)
     * @param schema Arrow schema for vector creation
     * @param bufferPool shared Arrow buffer pool
     * @param maxRowsPerVSR row threshold triggering VSR rotation
     * @param threadPool the thread pool for background native writes
     * @param runAsync if true, frozen VSR writes run on the background thread pool;
     *                 if false, they run on the calling thread (for benchmarks/tests)
     * @param writerGeneration the writer generation to store in file metadata
     * @param stats shard-level stats tracker
     */
    public VSRManager(
        String fileName,
        IndexSettings indexSettings,
        Schema schema,
        ArrowBufferPool bufferPool,
        int maxRowsPerVSR,
        ThreadPool threadPool,
        boolean runAsync,
        long writerGeneration,
        ParquetShardStatsTracker stats
    ) {
        this.fileName = fileName;
        this.indexSettings = indexSettings;
        this.writerGeneration = writerGeneration;
        this.stats = stats;
        this.vsrPool = new VSRPool("pool-" + fileName, schema, bufferPool, maxRowsPerVSR);
        this.threadPool = threadPool;
        this.vsrRotationThread = runAsync ? ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME : ThreadPool.Names.SAME;
        this.managedVSR.set(vsrPool.getActiveVSR());
        this.writer = new NativeParquetWriter(fileName, stats);
    }

    /**
     * Adds a document to the active VSR, rotating if necessary.
     * Transfers collected fields from the document input into the active VSR
     * using the ArrowFieldRegistry to resolve typed vector writes.
     * <p>
     * Field Shape is decided at the {@link ParquetDocumentInput} layer: fields mapped with
     * {@code multi_value: true} accumulate every value into one list-valued pair,
     * and all other fields still reject a second value.
     *
     * @param doc the document input containing field-value pairs
     */
    public void addDocument(ParquetDocumentInput doc) throws IOException {
        if (pendingWrite != null && pendingWrite.isDone()) {
            Future.State state = pendingWrite.state();
            if (state == Future.State.FAILED) {
                stats.incBackgroundWriteFailures();
                throw new IllegalStateException(pendingWrite.exceptionNow());
            } else if (state == Future.State.CANCELLED) {
                throw new IllegalStateException("Background write was cancelled");
            }
        }
        maybeRotateActiveVSR();
        // Re-check the rowId invariant so a single-format Parquet path is protected too.
        if (doc.getRowId() != acceptedRows) {
            throw new IllegalStateException(
                "rowId [" + doc.getRowId() + "] does not match accepted row count [" + acceptedRows + "] for " + fileName
            );
        }
        ManagedVSR activeVSR = managedVSR.get();
        final int rowIndex = activeVSR.getRowCount();
        // Track how far admission progressed for this row so a mid-document failure can scrub exactly
        // the slots a successful write already allocated. writtenFields is the count of leading source
        // fields fully written (getFinalInput() is written in order); rowIdWritten covers the rowId
        // vector written after the loop. See scrubPartialRow.
        int writtenFields = 0;
        boolean rowIdWritten = false;
        try {
            for (FieldValuePair pair : doc.getFinalInput()) {
                MappedFieldType fieldType = pair.getFieldType();
                ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldType.typeName());
                if (parquetField == null) {
                    // Defense-in-depth: schema reconciliation is supposed to happen in
                    // ParquetWriter.updateMappingVersion before any addDocument with a new
                    // field type. If we still see an unmapped type here, the writer is
                    // out of sync with the mapping — surface as a recoverable failure.
                    // TODO:: we can remove this post the validation on mapping update
                    throw new MismatchedInputException(
                        "No ParquetField mapping for field [" + fieldType.name() + "] of type [" + fieldType.typeName() + "]"
                    );
                }
                if (activeVSR.getVector(fieldType.name()) == null) {
                    logger.error(
                        "[Gen: {}] VSR schema mismatch: field [{}] not in active VSR. VSR schema fields: {}",
                        writerGeneration,
                        fieldType.name(),
                        activeVSR.getSchema().getFields().stream().map(f -> f.getName()).collect(java.util.stream.Collectors.joining(", "))
                    );
                    throw new MismatchedInputException(
                        "Active VSR has no vector for field ["
                            + fieldType.name()
                            + "] — schema reconciliation must run via updateMappingVersion before addDocument"
                    );
                }
                parquetField.createField(fieldType, activeVSR, pair.getValue());
                writtenFields++;
            }
            NestedParquetField nestedField = (NestedParquetField) ArrowFieldRegistry.getParquetField(ObjectMapper.NESTED_CONTENT_TYPE);
            nestedField.writeNestedChildren(doc, activeVSR, rowIndex);
            FlatObjectParquetField flatObjectField = (FlatObjectParquetField) ArrowFieldRegistry.getParquetField(
                FlatObjectFieldMapper.CONTENT_TYPE
            );
            flatObjectField.writeTopLevelMaps(doc, activeVSR, rowIndex);
            BigIntVector rowIdVector = (BigIntVector) activeVSR.getVector(DocumentInput.ROW_ID_FIELD);
            if (rowIdVector != null) {
                rowIdVector.setSafe(rowIndex, doc.getRowId());
                rowIdWritten = true;
            }
            activeVSR.setRowCount(rowIndex + 1);
            acceptedRows++;
        } catch (Exception e) {
            // Any failure between the first field write and acceptance leaves an uncounted partial
            // row. Scrub it (best-effort, never throws) so no stale value can leak into the next doc
            // that reuses this row index, then rethrow the original failure unchanged. Precise
            // rethrow keeps addDocument's throws clause unchanged.
            scrubPartialRow(doc, activeVSR, rowIndex, writtenFields, rowIdWritten);
            throw e;
        }
    }

    /**
     * Scrubs a partially-written, uncounted row after a mid-document failure so no stale value
     * survives to leak into the next document that reuses this row index (issue #22417).
     *
     * <p>Only the vectors whose write for this row already <em>succeeded</em> are reset: the leading
     * {@code writtenFields} fields of {@link ParquetDocumentInput#getFinalInput()} (which is written in
     * order) plus, when {@code rowIdWritten}, the rowId vector. Because each of those writes completed,
     * its slot at {@code rowIndex} is already allocated, so {@link #setNull} degrades to a plain
     * validity-bit clear and never triggers a (re)allocation — safe to run under the memory pressure
     * that may have caused the failure. The field that actually failed, and any fields ordered after it,
     * are intentionally left untouched: their slot may not be allocated, so clearing them could force a
     * growth allocation.
     *
     * <p>Best-effort and strictly non-throwing (see {@link #scrubVector}): a failure clearing any single
     * vector is logged and the remaining vectors are still scrubbed, so the original write failure (which
     * the caller rethrows) is never masked.
     *
     * @param doc           the document being admitted; source of the written field prefix, in order
     * @param activeVSR     the VSR the row was being written into
     * @param rowIndex      the uncommitted row index to clear
     * @param writtenFields number of leading source fields fully written for this row
     * @param rowIdWritten  whether the rowId vector was written for this row
     */
    private void scrubPartialRow(ParquetDocumentInput doc, ManagedVSR activeVSR, int rowIndex, int writtenFields, boolean rowIdWritten) {
        List<FieldValuePair> fields = doc.getFinalInput();
        for (int i = 0; i < writtenFields; i++) {
            scrubVector(activeVSR.getVector(fields.get(i).getFieldType().name()), rowIndex);
        }
        if (rowIdWritten) {
            scrubVector(activeVSR.getVector(DocumentInput.ROW_ID_FIELD), rowIndex);
        }
    }

    /**
     * Clears a single vector's slot at {@code rowIndex}, guarding the clear so it never throws. A
     * failure on one vector is logged and swallowed so the remaining vectors are still scrubbed and the
     * caller's original failure is never masked. No-op if {@code vector} is null.
     */
    private void scrubVector(FieldVector vector, int rowIndex) {
        if (vector == null) {
            return;
        }
        try {
            setNull(vector, rowIndex);
        } catch (RuntimeException | Error scrubFailure) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "[Gen: {}] Failed to scrub partial row {} for vector [{}] in {}; column may retain a stale value",
                    writerGeneration,
                    rowIndex,
                    vector.getName(),
                    fileName
                ),
                scrubFailure
            );
        }
    }

    /**
     * Clears the value at {@code index} by unsetting its validity bit, so the slot reads as null.
     * In Arrow 18.1.0 every vector's {@code setNull} clears only the validity bit at {@code index}
     * (growing the validity/offset buffers first if {@code index} is beyond capacity); it does not
     * rewrite the value/offset data of {@code index} or any other row. Callers here only pass slots
     * whose successful write already allocated them, so no growth occurs. Every vector type the Parquet
     * field registry produces is either fixed- or variable-width; the final branch is a non-throwing
     * fallback for any other vector type (e.g. a future nested/view field) that clears only the
     * top-level validity bit.
     */
    private static void setNull(FieldVector vector, int index) {
        switch (vector) {
            case BaseFixedWidthVector fixed -> fixed.setNull(index);
            case BaseVariableWidthVector variable -> variable.setNull(index);
            case BaseLargeVariableWidthVector large -> large.setNull(index);
            default -> BitVectorHelper.unsetBit(vector.getValidityBuffer(), index);
        }
    }

    public long getAcceptedRows() {
        return acceptedRows;
    }

    /**
     * Reconciles the active VSR with the given schema: adds vectors for any fields present in
     * {@code newSchema} but not yet in the active VSR, and for a field that already exists, recurses
     * into it to add any missing child too — so a mapping update that only adds a leaf to an existing
     * nested field is patched into this writer rather than dropped until the next rotation.
     * <p>
     * Called from {@link org.opensearch.parquet.writer.ParquetWriter#updateMappingVersion} when the
     * mapping version advances. No-op if every field in {@code newSchema} is already present.
     * <p>
     * Top-level fields are matched by name on read, so always safe to append. Nested struct children
     * are matched by position instead — see {@link #requireSortedAppendPosition}.
     *
     * @param newSchema the schema to reconcile against
     * @throws SchemaChangeRequiresWriterRotationException if a missing nested struct child can't be
     *         patched in without breaking the sorted position downstream reads rely on — the caller
     *         must end this writer generation as-is and retry on a fresh one. Thrown by the dry-run
     *         validation pass BEFORE anything is mutated, so reconcile is all-or-nothing: a rejected
     *         reconcile leaves both the live vectors and the cached schema exactly as they were, and
     *         the retired writer's buffered rows still export cleanly. (Patching earlier fields
     *         before rejecting a later one would buy nothing — the caller retires this generation
     *         anyway, so the patched field would never receive data — while leaving the cached
     *         schema declaring children the live vectors don't have, which poisons the flush.)
     */
    public boolean reconcileSchema(Schema newSchema) {
        ManagedVSR activeVSR = managedVSR.get();
        validateReconcilable(activeVSR, newSchema);
        boolean changed = false;
        try {
            for (Field schemaField : newSchema.getFields()) {
                FieldVector existingVector = activeVSR.getVector(schemaField.getName());
                if (existingVector == null) {
                    // Pass the schema field through as-is: rebuilding it from name + FieldType alone
                    // would drop getChildren(), leaving a LIST column with no element vector.
                    activeVSR.addFieldVector(schemaField);
                    changed = true;
                } else if (reconcileExistingChildren(existingVector, schemaField)) {
                    changed = true;
                } else if (hasSameStorageShape(existingVector.getField(), schemaField) == false) {
                    throw new SchemaChangeRequiresWriterRotationException(
                        schemaField.getName(),
                        existingVector.getField().getType(),
                        schemaField.getType()
                    );
                }
            }
        } finally {
            // The dry-run validation above means the rotation-exception class can no longer leave a
            // partial application — this refresh normally runs only after a fully-applied reconcile,
            // where newSchema exactly describes the live vectors. It stays in a finally as a backstop
            // for unexpected mid-apply failures (Arrow allocation errors): the cached schema then
            // over-declares the unapplied remainder, which ManagedVSR#exportToArrow's pre-export
            // validation reports as a clear error instead of a misaligned native export. Also updates
            // the pool schema so a VSR rotated later in this same generation includes the new fields.
            if (changed) {
                activeVSR.refreshSchema(newSchema);
                vsrPool.updateSchema(activeVSR.getSchema());
            } else {
                logger.debug("no changes in schema despite change in mapping version");
            }
        }
        return changed;
    }

    /**
     * Dry-run of {@link #reconcileSchema}: walks {@code newSchema} against the active VSR exactly the
     * way the apply loop will, raising the same {@link SchemaChangeRequiresWriterRotationException}s
     * it would, without mutating anything. Running this first makes reconcile all-or-nothing.
     * Mirrors the apply loop's structure; the shape check fires only when the walk would add no
     * child, matching the apply loop's else-if ordering.
     */
    private void validateReconcilable(ManagedVSR activeVSR, Schema newSchema) {
        for (Field schemaField : newSchema.getFields()) {
            FieldVector existingVector = activeVSR.getVector(schemaField.getName());
            if (existingVector == null) {
                continue; // top-level fields are matched by name on read — appending is always safe
            }
            if (validateExistingChildren(existingVector, schemaField) == false
                && hasSameStorageShape(existingVector.getField(), schemaField) == false) {
                throw new SchemaChangeRequiresWriterRotationException(
                    schemaField.getName(),
                    existingVector.getField().getType(),
                    schemaField.getType()
                );
            }
        }
    }

    /** Dry-run counterpart of {@link #reconcileExistingChildren}: true if the apply walk would add a child. */
    private boolean validateExistingChildren(FieldVector existingVector, Field schemaField) {
        if (existingVector instanceof ListVector existingList && schemaField.getChildren().size() == 1) {
            FieldVector existingElement = existingList.getDataVector();
            if (existingElement instanceof StructVector existingStruct) {
                return validateStructChildren(existingStruct, schemaField.getChildren().get(0));
            }
        } else if (existingVector instanceof StructVector existingStruct) {
            return validateStructChildren(existingStruct, schemaField);
        }
        return false;
    }

    /**
     * Dry-run counterpart of {@link #reconcileStructChildren}: applies the
     * {@link #requireSortedAppendPosition} rule to each missing child, tracking the simulated
     * last-appended name so a second missing child is checked against the first one's position —
     * the same state the apply loop's live appends would produce.
     */
    private boolean validateStructChildren(StructVector existingStruct, Field structField) {
        boolean wouldChange = false;
        List<Field> existingChildren = existingStruct.getField().getChildren();
        String lastChildName = existingChildren.isEmpty() ? null : existingChildren.get(existingChildren.size() - 1).getName();
        for (Field childField : structField.getChildren()) {
            FieldVector existingChild = existingStruct.getChild(childField.getName());
            if (existingChild == null) {
                if (lastChildName != null && childField.getName().compareTo(lastChildName) < 0) {
                    throw new SchemaChangeRequiresWriterRotationException(
                        "Cannot patch struct child ["
                            + childField.getName()
                            + "] into an already-active vector: appending it after ["
                            + lastChildName
                            + "] would not match its sorted position (it sorts before ["
                            + lastChildName
                            + "]), and struct children are matched by position downstream. Writer generation "
                            + "must rotate so a fresh generation can rebuild this struct fully sorted."
                    );
                }
                lastChildName = childField.getName();
                wouldChange = true;
            } else if (validateExistingChildren(existingChild, childField)) {
                wouldChange = true;
            }
        }
        return wouldChange;
    }

    /**
     * Recurses into an already-present complex vector (a nested field's {@code LIST<STRUCT>}, or a
     * struct/map inside one) and adds any child {@code schemaField} declares that {@code existingVector}
     * doesn't have yet. Returns true if anything was added. No-op (returns false) for a scalar leaf —
     * nothing to walk into.
     */
    private boolean reconcileExistingChildren(FieldVector existingVector, Field schemaField) {
        if (existingVector instanceof ListVector existingList && schemaField.getChildren().size() == 1) {
            // path: LIST<STRUCT<...>> (nested) or MAP<Utf8,Utf8> (flat_object, its "element" is the
            // key_value struct) — either way, the single child is the element/entries struct.
            FieldVector existingElement = existingList.getDataVector();
            if (existingElement instanceof StructVector existingStruct) {
                return reconcileStructChildren(existingStruct, schemaField.getChildren().get(0));
            }
        } else if (existingVector instanceof StructVector existingStruct) {
            return reconcileStructChildren(existingStruct, schemaField);
        }
        return false;
    }

    /**
     * Adds any child of {@code structField} missing from {@code existingStruct}, recursing into
     * children present in both.
     *
     * @throws SchemaChangeRequiresWriterRotationException if a missing child would have to be
     *         appended at a position other than where the fully-sorted fresh schema would put
     *         it — see {@link #requireSortedAppendPosition}.
     */
    private boolean reconcileStructChildren(StructVector existingStruct, Field structField) {
        boolean changed = false;
        for (Field childField : structField.getChildren()) {
            FieldVector existingChild = existingStruct.getChild(childField.getName());
            if (existingChild == null) {
                requireSortedAppendPosition(existingStruct, childField);
                addMissingChild(existingStruct, childField);
                changed = true;
            } else if (reconcileExistingChildren(existingChild, childField)) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Verifies that appending {@code childField} to {@code existingStruct} reproduces the position a
     * fresh, fully-sorted build would give it, before {@link #addMissingChild} is allowed to patch it
     * in. {@code existingStruct}'s children are always already sorted by name (fresh-built, or every
     * prior patch through this method was itself verified) — so appending stays sorted iff
     * {@code childField}'s name sorts after the last existing child's name.
     * <p>
     * Struct children are matched BY POSITION downstream (Substrait/DataFusion), so an append that
     * doesn't reproduce sorted position would desync this generation's on-disk order from every
     * other's — not safe to patch at any point; the caller must rotate to a fresh generation instead.
     *
     * @throws SchemaChangeRequiresWriterRotationException if {@code childField}'s name sorts
     *         before the last existing child's name
     */
    private void requireSortedAppendPosition(StructVector existingStruct, Field childField) {
        List<Field> existingChildren = existingStruct.getField().getChildren();
        if (existingChildren.isEmpty()) {
            // Nothing to be out of order with — a single element is trivially sorted.
            return;
        }
        String lastExistingName = existingChildren.get(existingChildren.size() - 1).getName();
        if (childField.getName().compareTo(lastExistingName) < 0) {
            throw new SchemaChangeRequiresWriterRotationException(
                "Cannot patch struct child ["
                    + childField.getName()
                    + "] into an already-active vector: appending it after ["
                    + lastExistingName
                    + "] would not match its sorted position (it sorts before ["
                    + lastExistingName
                    + "]), and struct children are matched by position downstream. Writer generation "
                    + "must rotate so a fresh generation can rebuild this struct fully sorted."
            );
        }
    }

    /**
     * Adds {@code childField} as a new named child of {@code parentStruct}, building out its full
     * subtree with the same names {@code Field#createVector} would use for a fresh schema (e.g.
     * {@code "element"} for a LIST's struct, {@code "key_value"} for a MAP's entries struct) — NOT
     * {@code addOrGetList}/{@code addOrGetMap}/{@code addOrGetVector(FieldType)}'s hardcoded internal
     * defaults ({@code "$data$"}/{@code "entries"}), which would silently diverge from the schema this
     * same field gets when built fresh, e.g. by {@link org.opensearch.parquet.fields.NestedParquetField}.
     * Safe to call for a field confirmed missing: it only ever creates new vectors, never touches an
     * existing child. Callers must have already verified (see {@link #requireSortedAppendPosition})
     * that appending preserves this struct's sorted-by-name invariant.
     */
    private void addMissingChild(StructVector parentStruct, Field childField) {
        parentStruct.initializeChildrenFromFields(List.of(childField));
    }

    /**
     * Compares the Arrow storage shape of two fields while ignoring field names. Arrow Java
     * renames a {@code ListVector} child from {@code element} to {@code $data$} internally, so
     * full {@link Field#equals(Object)} comparisons spuriously report a schema change.
     */
    private static boolean hasSameStorageShape(Field existingField, Field schemaField) {
        if (existingField.getType().equals(schemaField.getType()) == false) {
            return false;
        }
        List<Field> existingChildren = existingField.getChildren();
        List<Field> schemaChildren = schemaField.getChildren();
        if (existingChildren.size() != schemaChildren.size()) {
            return false;
        }
        for (int i = 0; i < existingChildren.size(); i++) {
            if (hasSameStorageShape(existingChildren.get(i), schemaChildren.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if VSR rotation is needed before accepting the next document.
     * If the active VSR has reached the row threshold and the frozen slot is empty,
     * freezes the active VSR, submits it for background native write, and creates
     * a new active VSR. If the frozen slot is occupied, rotation is skipped.
     */
    public void maybeRotateActiveVSR() throws IOException {
        boolean rotated = vsrPool.maybeRotateActiveVSR();
        if (rotated == false) {
            return;
        }
        stats.incVsrRotations();
        logger.debug("VSR rotation occurred for {}", fileName);
        ManagedVSR frozenVSR = vsrPool.getFrozenVSR();
        if (frozenVSR != null) {
            logger.debug("Writing frozen VSR {} ({} rows) for {}", frozenVSR.getId(), frozenVSR.getRowCount(), fileName);
            maybeInitializeWriter(frozenVSR);
            Runnable writeTask = () -> {
                try {
                    try (ArrowExport export = frozenVSR.exportToArrow()) {
                        rowCount.add(frozenVSR.getRowCount());
                        writer.write(export.getArrayAddress(), export.getSchemaAddress());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                vsrPool.completeVSR(frozenVSR);
                vsrPool.unsetFrozenVSR();
            };
            try {
                pendingWrite = threadPool.executor(vsrRotationThread).submit(writeTask);
            } catch (OpenSearchRejectedExecutionException e) {
                // Pool saturated — count the rejection and re-throw (surfaces as HTTP 429).
                stats.incNativeWriteRejections();
                throw e;
            }
        }
        ManagedVSR newVSR = vsrPool.getActiveVSR();
        if (newVSR == null) {
            throw new IOException("No active VSR available after rotation");
        }
        managedVSR.set(newVSR);
        logger.debug("VSR rotation completed for {}, new active VSR: {}", fileName, newVSR.getId());
    }

    /**
     * Flushes the current VSR data to a Parquet file via native bridge.
     *
     * @return metadata about the written Parquet file, or null if no data to flush
     */
    public ParquetFileMetadata flush() throws IOException {
        awaitPendingWrite(ROTATION_TIMEOUT, false);
        ManagedVSR currentVSR = managedVSR.get();
        if (currentVSR != null && currentVSR.getRowCount() > 0) {
            logger.info("Flushing {} rows for {}", currentVSR.getRowCount(), fileName);
            currentVSR.moveToFrozen();
            maybeInitializeWriter(currentVSR);
            try (ArrowExport export = currentVSR.exportToArrow()) {
                rowCount.add(currentVSR.getRowCount());
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
            vsrPool.completeVSR(currentVSR);
            managedVSR.set(null);
        }
        ParquetFileMetadata metadata = writer.flush();
        assert metadata == null || metadata.numRows() == rowCount.sum() : "Row count mismatch between Java managed VSR and Rust writer";
        logger.debug("Flush completed for {} with metadata: {}", fileName, metadata);
        return metadata;
    }

    /**
     * Syncs the Parquet file to disk. Must be called after {@link #flush()}.
     */

    @Override
    public void close() {
        // vsrPool.close() MUST run even if awaitPendingWrite / writer.flush() throws: a failed or
        // timed-out background write (IOException from awaitPendingWrite) previously skipped it,
        // stranding the pool's per-VSR child allocators (their off-heap Arrow buffers leaked onto
        // the ingest pool for the node's lifetime — "Memory was leaked by query"). Release the pool
        // in a finally so the buffers are reclaimed regardless of the drain/flush outcome.
        try {
            awaitPendingWrite(ROTATION_TIMEOUT, true);
            if (writer != null) {
                writer.flush();
            }
        } catch (Exception e) {
            logger.error("Error during close for {}: {}", fileName, e.getMessage());
            throw new RuntimeException("Failed to close VSRManager: " + e.getMessage(), e);
        } finally {
            try {
                vsrPool.close();
            } catch (Exception e) {
                logger.error("Error releasing VSR pool during close for {}: {}", fileName, e.getMessage());
            }
            // Guarantee the native writer registry entry is gone even if flush() above was skipped or
            // threw (e.g. a background write / flush failed under an Arrow OOM). Idempotent: a no-op
            // when flush() already finalized and removed the entry. Without this, a stranded entry
            // blocks recovery's re-create for the same file ("Writer already exists").
            if (writer != null) {
                writer.cleanup();
            }
            managedVSR.set(null);
        }
    }

    /**
     * Initializes the native writer on first use, using the schema from the given VSR.
     */
    private void maybeInitializeWriter(ManagedVSR vsr) throws IOException {
        if (writer.isInitialized() == false) {
            String indexName = indexSettings.getIndex().getName();
            ParquetSortConfig sortConfig = new ParquetSortConfig(indexSettings);
            try (ArrowSchema schema = vsr.exportSchema()) {
                writer.initialize(indexName, schema.memoryAddress(), sortConfig, writerGeneration);
            }
        }
    }

    /**
     * Waits for any in-flight background write to complete with an optional timeout.
     *
     * @param timeoutSeconds timeout in seconds (0 means wait indefinitely)
     * @param ignoreTimeout if true, log a warning on timeout instead of throwing
     */
    private void awaitPendingWrite(long timeoutSeconds, boolean ignoreTimeout) throws IOException {
        if (pendingWrite == null) {
            return;
        }
        long startNanos = System.nanoTime();
        try {
            if (timeoutSeconds > 0) {
                pendingWrite.get(timeoutSeconds, TimeUnit.SECONDS);
            } else {
                pendingWrite.get();
            }
            stats.incBackgroundWriteTotal();
        } catch (TimeoutException e) {
            stats.incBackgroundWriteTimeouts();
            if (ignoreTimeout) {
                logger.warn("Timed out waiting for background VSR write for {}", fileName);
            } else {
                throw new IOException("Timed out waiting for background VSR write for " + fileName, e);
            }
        } catch (Exception e) {
            stats.incBackgroundWriteFailures();
            throw new IOException("Background VSR write failed for " + fileName, e.getCause());
        } finally {
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            stats.addBackgroundWriteWaitMillis(elapsed);
            pendingWrite = null;
        }
    }

    /**
     * Rolls the VSR back to hold exactly {@code rowCount} admitted rows. No-op if already
     * at the target. Throws if the target is higher than current or if the active VSR
     * doesn't have enough rows to trim (rollback crossed a rotation boundary).
     *
     * @param rowCount the desired row count after this call
     */
    public void rollbackTo(long rowCount) {
        if (rowCount > acceptedRows) {
            throw new IllegalStateException("Cannot rollback to " + rowCount + ": only " + acceptedRows + " rows in VSR");
        }
        if (rowCount == acceptedRows) {
            return;
        }
        ManagedVSR activeVSR = managedVSR.get();
        long diff = acceptedRows - rowCount;
        if (diff > activeVSR.getRowCount()) {
            throw new IllegalStateException("Cannot rollback " + diff + " rows: active VSR only has " + activeVSR.getRowCount() + " rows");
        }
        activeVSR.setRowCount(activeVSR.getRowCount() - (int) diff);
        acceptedRows = rowCount;
    }

    /**
     * Returns whether the schema can still evolve (native writer not yet initialized).
     *
     * @return true if the schema is mutable
     */
    public boolean isSchemaMutable() {
        return writer.isInitialized() == false;
    }

    // Visible for testing only
    ManagedVSR getActiveManagedVSR() {
        return managedVSR.get();
    }

    /**
     * Returns the row ID mapping produced during the last flush's sort-on-close
     * as a memory-efficient packed mapping, or null if no sorting was configured
     * or the file was empty.
     */
    public RowIdMapping getRowIdMapping() {
        return writer.getRowIdMapping();
    }

    /** Visible for testing — returns the pending background write future, or null. */
    Future<?> getPendingWrite() {
        return pendingWrite;
    }

    /** Visible for testing — injects a pending background write future to exercise close() paths. */
    void setPendingWrite(Future<?> future) {
        this.pendingWrite = future;
    }
}

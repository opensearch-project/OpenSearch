/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * POC (child-table nested design): a second, co-located writer stack that materialises a shard's
 * {@code nested} elements as rows of a <em>child table</em>.
 *
 * <p>The design (see {@code MustangDevConfig design/nested-field-support/09} and {@code 10}) stores
 * one child row per nested array element rather than block-joining elements into hidden Lucene child
 * documents. Because an element <em>is</em> a row, intra-element correlation becomes a flat same-row
 * AND, so the child table's Lucene secondary is exact at element grain rather than a superset — and
 * the parent table keeps its {@code docId == __row_id__ == parquet row} identity untouched.
 *
 * <h2>What this class proves, and what it deliberately does not</h2>
 * This is the <em>reduced</em> proof of the risky mechanism: that one shard can run two co-located
 * writer stacks and stamp a lockstep foreign key. It reaches that with three deliberate shortcuts:
 * <ul>
 *   <li><b>Same delegate engines, second generation.</b> Rather than introduce a table dimension the
 *       indexing SPI does not have, the child writers are created from the <em>same</em> per-format
 *       engines as the parent, at generation {@code parentGeneration + }{@value #CHILD_GENERATION_OFFSET}.
 *       Both the parquet file name and the Lucene temp directory are generation-derived, so the child
 *       gets its own files under the same shard directory for free, and the offset keeps the pairing
 *       legible ({@code childGeneration - OFFSET == parentGeneration}).</li>
 *   <li><b>Shared schema.</b> Child rows are written against the parent's Arrow schema. Parent rows
 *       leave the nested leaf columns null; child rows leave the parent's own columns null. A real
 *       implementation would give the child table its own narrow schema.</li>
 *   <li><b>Primary format only.</b> {@link #flush} publishes the child's <em>value store</em> as an
 *       {@linkplain FileInfos#auxiliarySegments() auxiliary segment}, so it reaches the catalog and a
 *       reader opens on it. The child table's <em>secondary</em> (Lucene) index — the pruning index
 *       the design's semi-join is meant to exploit — is not written yet; see {@link #flush} for the
 *       one structural reason it cannot simply be published alongside. Merges and delete fan-out
 *       remain out of scope.</li>
 * </ul>
 *
 * <h2>The foreign key</h2>
 * {@link #addElements} is called by {@link CompositeWriter} only after every parent writer has
 * accepted the document, so the parent's {@code __row_id__} is already final and unambiguous. That
 * value is stamped into each of its elements' {@code parent_row_id} column, alongside the element's
 * source position in {@code elem_ord}. Co-location is what makes the bare row id a sufficient key:
 * a {@code __row_id__} is unique only within one index + shard + generation, so a child row in a
 * <em>different</em> shard could not identify its parent by row id alone.
 *
 * <h2>Keeping the foreign key stable across a reorder</h2>
 * A {@code __row_id__} is a <em>position</em>, and a sorted flush renumbers positions: with
 * {@code index.sort.*} configured the parquet writer sorts on close and returns a
 * {@link RowIdMapping} of old row id to new. A foreign key captured during {@code addDoc} therefore
 * names the <em>pre-sort</em> row, and because a stale row id is still a <em>valid</em> row id the
 * mistake would surface as a wrong document rather than an error.
 *
 * <p>Parquet rows cannot be edited once handed to the writer, so the fix is to delay the write
 * rather than repair it. When the index has an index sort — the only case in which the parent can
 * renumber at flush — elements are <b>staged</b> here and materialised in {@link #flush} once the
 * mapping is known, with each foreign key passed through
 * {@link RowIdMapping#getNewRowId(long, long)}. Without an index sort the parent keeps insertion
 * order, so rows are written eagerly and nothing is buffered; a mapping arriving on that path is
 * treated as a hard error rather than silently stamped.
 *
 * <p>Each child row also carries the parent's {@code _seq_no} in {@link #PARENT_SEQ_NO_FIELD} when
 * the mapping declares that column. Unlike a row id, a sequence number is assigned per operation and
 * is never renumbered by a sort or a merge, which makes it an oracle for the positional key: the
 * parent row {@code parent_row_id} points at must carry the same {@code _seq_no}. That turns this
 * whole class of bug from silently-wrong into checkable.
 *
 * <p>Merges are the other renumbering mechanism, and only half of that is handled. A child segment is
 * now a merge <em>participant</em>: it is selected together with the parent segments it belongs to,
 * merged by the same delegate merger, and swapped into the catalog with them, so a merge no longer
 * strands or drops it (see {@code CompositeMergeExecutor#executeAuxiliary}). What a merge does
 * <em>not</em> yet do is rewrite the foreign key. A merge mapping is keyed by
 * {@code (oldRowId, oldGeneration)} and applying it means editing a column inside an already-written
 * parquet file, which the native merge kernel has no path for today — so after a merge
 * {@code parent_row_id} still names pre-merge positions. {@link #PARENT_SEQ_NO_FIELD} is unaffected,
 * being a value rather than a position. See {@code 10} Phase 4b.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class NestedChildStack implements Closeable {

    private static final Logger logger = LogManager.getLogger(NestedChildStack.class);

    /**
     * Added to the parent's writer generation to derive the child's. Aliases
     * {@link AuxiliaryDataFormat#GENERATION_OFFSET}, which is where the contract lives now that the
     * merge path has to invert it to pair a child segment back to its parent.
     */
    static final long CHILD_GENERATION_OFFSET = AuxiliaryDataFormat.GENERATION_OFFSET;

    /** Child-table column holding the parent document's {@code __row_id__} — the foreign key. */
    static final String PARENT_ROW_ID_FIELD = "parent_row_id";

    /** Child-table column holding the element's 0-based position in its source array. */
    static final String ELEMENT_ORDINAL_FIELD = "elem_ord";

    /**
     * Optional child-table column holding the parent document's {@code _seq_no}. Unlike
     * {@link #PARENT_ROW_ID_FIELD} a sequence number never renumbers, so it is the oracle the
     * positional foreign key is checked against. Stamped only when the mapping declares it.
     */
    static final String PARENT_SEQ_NO_FIELD = "parent_seq_no";

    private final CompositeIndexingExecutionEngine engine;
    private final long childGeneration;
    private final MappedFieldType parentRowIdFieldType;
    private final MappedFieldType elementOrdinalFieldType;
    /** Null when the mapping does not declare {@link #PARENT_SEQ_NO_FIELD}. */
    private final MappedFieldType parentSeqNoFieldType;
    private final Writer<DocumentInput<?>> primaryWriter;
    /**
     * True when the parent may renumber its rows at flush, which is exactly when the index has an
     * index sort. Elements are then staged until {@link #flush} can remap their foreign keys.
     */
    private final boolean stageUntilFlush;
    /** Staged elements awaiting a {@link RowIdMapping}; always empty unless {@link #stageUntilFlush}. */
    private final List<StagedParent> staged = new ArrayList<>();
    private int stagedElementCount = 0;
    private long childRowId = 0L;
    private boolean closed;

    /**
     * One parent document's elements, held until the parent's row permutation is known.
     *
     * @param parentRowId the parent's insertion-order {@code __row_id__}, still to be remapped
     * @param parentSeqNo the parent's {@code _seq_no}, which no reorder can change
     * @param elements    the parent's nested elements, in source order
     * @param inherited   parent metadata every child row carries
     */
    private record StagedParent(
        long parentRowId,
        long parentSeqNo,
        List<CompositeDocumentInput.NestedElement> elements,
        List<CompositeDocumentInput.FieldValue> inherited
    ) {}

    /**
     * Creates the child stack for one parent writer generation, opening a child writer on the
     * primary (value store) engine. Callers must pre-check {@link #isEnabledFor} — the foreign-key
     * columns are resolved eagerly here and their absence is a programming error.
     *
     * @param engine           the composite engine whose primary delegate backs both stacks
     * @param parentGeneration the parent writer's generation, from which the child's is derived
     */
    @SuppressWarnings("unchecked")
    NestedChildStack(CompositeIndexingExecutionEngine engine, long parentGeneration) {
        this.engine = engine;
        this.childGeneration = AuxiliaryDataFormat.generationFor(parentGeneration);

        MapperService mapperService = engine.mapperService();
        this.parentRowIdFieldType = mapperService.fieldType(PARENT_ROW_ID_FIELD);
        this.elementOrdinalFieldType = mapperService.fieldType(ELEMENT_ORDINAL_FIELD);
        assert parentRowIdFieldType != null && elementOrdinalFieldType != null
            : "child-table columns must exist; call isEnabledFor() first";
        this.parentSeqNoFieldType = mapperService.fieldType(PARENT_SEQ_NO_FIELD);
        this.stageUntilFlush = mapperService.getIndexSettings().getIndexSortConfig().hasIndexSort();

        // Primary only. A secondary child writer would be opened and then abandoned: the child's
        // segment bypasses the per-format indexing engines' refresh (that is what an auxiliary
        // segment is), and for the Lucene backend it is exactly that refresh which hardlinks a
        // writer's temp directory into the shard's index/ and cleans it up. Opening one anyway would
        // index every element into a Lucene index nothing reads and leave its temp directory — lock
        // file included — behind on every refresh.
        WriterConfig childConfig = new WriterConfig(childGeneration);
        IndexingExecutionEngine<?, ?> primaryDelegate = engine.getPrimaryDelegate();
        this.primaryWriter = (Writer<DocumentInput<?>>) primaryDelegate.createWriter(childConfig);
        logger.info(
            "Opened nested child-table stack at generation [{}] (parent generation [{}]), foreign-key mode [{}], "
                + "seq-no oracle column [{}]",
            childGeneration,
            parentGeneration,
            stageUntilFlush ? "staged (index sort configured; parent rows renumber at flush)" : "eager (no index sort)",
            parentSeqNoFieldType != null ? "present" : "absent"
        );
    }

    /**
     * Returns true when this shard's mapping declares the child table's bookkeeping columns, which
     * is how an index opts in to the child-table POC. Without them the foreign key has nowhere to
     * land, so nested elements are dropped instead.
     */
    static boolean isEnabledFor(MapperService mapperService) {
        return mapperService != null
            && mapperService.fieldType(PARENT_ROW_ID_FIELD) != null
            && mapperService.fieldType(ELEMENT_ORDINAL_FIELD) != null;
    }

    /**
     * Accepts one parent document's nested elements, keyed by the row id the parent just committed to.
     *
     * <p>Called after the parent document is fully accepted, so a failure here leaves the parent row
     * committed and its child rows missing. The reduced proof accepts that: making the two stacks
     * atomic is a separate obligation (see {@code 09} §11b) and needs the child table to be a real
     * catalog participant first.
     *
     * <p>Whether the rows are written now or at flush depends on {@link #stageUntilFlush} — see the
     * class javadoc. {@code parentRowId} is an insertion-order row id either way; the staged path is
     * the one that may still have to translate it.
     *
     * @param parentRowId the parent document's {@code __row_id__}
     * @param elements    the parent's nested elements, in source order
     * @param inherited   parent metadata every child row carries ({@code _id}, {@code _seq_no}, ...)
     * @throws IOException if a child writer fails to admit a row
     */
    void addElements(
        long parentRowId,
        List<CompositeDocumentInput.NestedElement> elements,
        List<CompositeDocumentInput.FieldValue> inherited
    ) throws IOException {
        long parentSeqNo = seqNoOf(inherited);
        if (stageUntilFlush) {
            staged.add(new StagedParent(parentRowId, parentSeqNo, elements, inherited));
            stagedElementCount += elements.size();
            return;
        }
        writeRows(parentRowId, parentSeqNo, elements, inherited);
    }

    /**
     * Writes one child row per element, stamping {@code parentRowId} as the foreign key. By the time
     * this runs the foreign key is final: either the parent never renumbers (eager path) or the
     * permutation has already been applied (staged path).
     */
    private void writeRows(
        long parentRowId,
        long parentSeqNo,
        List<CompositeDocumentInput.NestedElement> elements,
        List<CompositeDocumentInput.FieldValue> inherited
    ) throws IOException {
        for (CompositeDocumentInput.NestedElement element : elements) {
            CompositeDocumentInput childDoc = engine.newDocumentInput();
            for (CompositeDocumentInput.FieldValue metadata : inherited) {
                childDoc.addField(metadata.fieldType(), metadata.value());
            }
            List<MappedFieldType> fieldTypes = element.fieldTypes();
            List<Object> values = element.values();
            for (int i = 0; i < fieldTypes.size(); i++) {
                childDoc.addField(fieldTypes.get(i), values.get(i));
            }
            childDoc.addField(parentRowIdFieldType, parentRowId);
            childDoc.addField(elementOrdinalFieldType, (long) element.ordinal());
            if (parentSeqNoFieldType != null) {
                childDoc.addField(parentSeqNoFieldType, parentSeqNo);
            }
            childDoc.setRowId(DocumentInput.ROW_ID_FIELD, childRowId);

            WriteResult result = primaryWriter.addDoc(childDoc.getPrimaryInput());
            if (result instanceof WriteResult.Failure failure) {
                throw new IOException("Failed to add child row for parent row id [" + parentRowId + "]", failure.cause());
            }
            childRowId++;
        }
    }

    /**
     * Reads the parent's {@code _seq_no} out of its inherited metadata. The engine stamps the real,
     * already-assigned sequence number onto the {@code DocumentInput} before {@code addDoc}
     * ({@code DataFormatAwareEngine#indexIntoEngine}), so by the time elements reach here the value
     * is final rather than a placeholder.
     */
    private static long seqNoOf(List<CompositeDocumentInput.FieldValue> inherited) {
        for (CompositeDocumentInput.FieldValue metadata : inherited) {
            if (SeqNoFieldMapper.NAME.equals(metadata.fieldType().name()) && metadata.value() instanceof Number seqNo) {
                return seqNo.longValue();
            }
        }
        return SequenceNumbers.UNASSIGNED_SEQ_NO;
    }

    /**
     * Materialises any staged rows against the parent's row permutation, then flushes every child
     * writer and returns the child table as a {@link Segment} of its own, ready for the parent's
     * {@link FileInfos#auxiliarySegments()}.
     *
     * <p>The segment is keyed by {@linkplain AuxiliaryDataFormat auxiliary} format name rather than
     * the delegate's, which is what keeps the two tables apart in one catalog: a reader asking for
     * {@code parquet} sees only parent rows and one asking for {@code aux__parquet__nested} sees only
     * elements, even though both sets of files sit in the same directory. Its own generation is what
     * makes that legal — {@code CatalogSnapshotManager} requires every format inside one segment to
     * report the same row count, which a 2-document parent and its 3-element child cannot both do.
     *
     * <p>Only the primary (value store) format is here at all. The child's Lucene index is the
     * pruning index the design's semi-join is meant to exploit, but it cannot be published the way
     * the value store is: a Lucene <em>directory</em> is one index with one {@code segments_N}, so
     * two independent tables cannot share it, whereas two parquet tables sharing a directory is safe
     * because their file names are generation-derived. So {@link AuxiliaryDataFormat#storageFormat()}
     * — a side table stores beside its delegate — holds for parquet and does not hold for Lucene:
     * the child table needs a Lucene directory, shared writer and commit stack of its own. Until
     * then the semi-join scans the child's parquet, which is correct but unpruned.
     *
     * @param parentRowIdMapping the permutation the parent applied to its rows during this flush, or
     *                           null when it kept insertion order
     * @param parentRowCount     rows the parent accepted, used to check the mapping covers them all
     * @return the child table's segment, or empty when this stack produced no rows
     */
    List<Segment> flush(RowIdMapping parentRowIdMapping, long parentRowCount) throws IOException {
        materialiseStagedRows(parentRowIdMapping, parentRowCount);
        if (childRowId == 0L) {
            logger.debug("Nested child-table stack at generation [{}] has no rows; skipping flush", childGeneration);
            return List.of();
        }
        DataFormat primaryFormat = engine.getPrimaryDelegate().getDataFormat();
        FileInfos primaryFileInfos = primaryWriter.flush(FlushInput.EMPTY);

        WriterFileSet childFiles = primaryFileInfos.getWriterFileSet(primaryFormat).orElse(null);
        if (childFiles == null) {
            // The stack accounts for rows, so the primary writer must have produced a file for them.
            // Returning nothing here would drop the child table silently while the parent's rows are
            // already committed — exactly the class of failure this design has to stay clear of.
            throw new IOException(
                "Nested child table at generation ["
                    + childGeneration
                    + "] wrote ["
                    + childRowId
                    + "] rows but its primary format ["
                    + primaryFormat.name()
                    + "] produced no files; flushed formats were "
                    + primaryFileInfos.writerFilesMap().keySet()
            );
        }
        String childFormatName = AuxiliaryDataFormat.nameFor(primaryFormat.name(), AuxiliaryDataFormat.NESTED_CHILD_ROLE);
        logger.info(
            "Nested child table published: generation [{}] format [{}] rows [{}] files {} in [{}]",
            childGeneration,
            childFormatName,
            childRowId,
            childFiles.files(),
            childFiles.directory()
        );
        return List.of(Segment.builder(childGeneration).addSearchableFiles(childFormatName, childFiles).build());
    }

    /**
     * Translates every staged parent row id through the parent's flush permutation and writes the
     * child rows. A no-op on the eager path, which has nothing staged.
     *
     * <p>Every disagreement between what this stack expected and what the parent actually did is
     * raised rather than absorbed. A positional foreign key that is merely <em>plausible</em> is
     * indistinguishable from a correct one downstream, so guessing here would trade a loud failure at
     * flush for a silently wrong query result later.
     */
    private void materialiseStagedRows(RowIdMapping parentRowIdMapping, long parentRowCount) throws IOException {
        if (stageUntilFlush == false) {
            if (parentRowIdMapping != null) {
                throw new IllegalStateException(
                    "Parent reordered its rows at flush (RowIdMapping of size ["
                        + parentRowIdMapping.size()
                        + "]) but nested child rows for generation ["
                        + childGeneration
                        + "] were already written with insertion-order foreign keys. Every ["
                        + PARENT_ROW_ID_FIELD
                        + "] would point at the wrong parent."
                );
            }
            return;
        }
        if (staged.isEmpty()) {
            return;
        }
        if (parentRowIdMapping == null) {
            throw new IllegalStateException(
                "Index sort is configured, so nested elements for generation ["
                    + childGeneration
                    + "] were staged awaiting the parent's row permutation, but the primary format produced no "
                    + "RowIdMapping. Cannot tell whether the parent reordered, so refusing to stamp ["
                    + PARENT_ROW_ID_FIELD
                    + "]."
            );
        }
        if (parentRowIdMapping.size() != parentRowCount) {
            throw new IllegalStateException(
                "Parent RowIdMapping covers ["
                    + parentRowIdMapping.size()
                    + "] rows but the parent accepted ["
                    + parentRowCount
                    + "]; the permutation does not describe the rows the foreign keys refer to"
            );
        }
        for (StagedParent parent : staged) {
            long remapped = parentRowIdMapping.getNewRowId(parent.parentRowId(), RowIdMapping.SINGLE_GEN);
            if (remapped < 0L) {
                throw new IllegalStateException(
                    "Parent RowIdMapping has no entry for insertion-order row id ["
                        + parent.parentRowId()
                        + "] (parent _seq_no ["
                        + parent.parentSeqNo()
                        + "]) in generation ["
                        + childGeneration
                        + "]"
                );
            }
            writeRows(remapped, parent.parentSeqNo(), parent.elements(), parent.inherited());
        }
        logger.info(
            "Remapped [{}] nested child rows across [{}] parent documents through the flush permutation "
                + "for child generation [{}]",
            stagedElementCount,
            staged.size(),
            childGeneration
        );
        staged.clear();
        stagedElementCount = 0;
    }

    /**
     * Number of child rows this stack accounts for — written plus, on the staged path, still awaiting
     * the parent's permutation.
     */
    long childRowCount() {
        return childRowId + stagedElementCount;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.close(primaryWriter);
    }
}

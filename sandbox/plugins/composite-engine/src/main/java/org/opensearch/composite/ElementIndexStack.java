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
import org.opensearch.index.mapper.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Engine-4 (parallel LIST columns + element index): a co-located, Lucene-only writer stack that
 * indexes each {@code nested} array element as its own postings-only Lucene document, so a filter on
 * a nested field is exact at element grain — a same-element {@code AND} on one element doc, no block
 * join (see {@code MustangDevConfig design/nested-field-support/11} §3c and {@code 12} Phase W4).
 *
 * <p>Unlike the child-table {@code NestedChildStack}, this writes <em>only</em> a Lucene index (not a
 * parquet child table), carries <em>no</em> foreign key or inherited metadata, and needs no
 * staging/remap: the element→parent mapping is a plain {@link DocumentInput#NESTED_PARENT_ROW_FIELD}
 * doc-value on each element, and the parent's own {@code LIST} columns + bridge columns hold the
 * values (written by the parquet primary). The element index exists to answer nested filters.
 *
 * <h2>Layout</h2>
 * The stack opens a {@link Writer} on the composite's Lucene secondary delegate at generation
 * {@code parentGeneration + }{@link AuxiliaryDataFormat#GENERATION_OFFSET}, so the element index gets
 * its own {@code lucene_gen_<auxGeneration>} directory (its own {@code segments_N}) — the one reason
 * the child stack could not publish a Lucene side table. It is published under the auxiliary format
 * name {@code aux__lucene__nested} so the catalog holds its files apart from the parent's main index.
 *
 * <h2>Per element document</h2>
 * <ul>
 *   <li>{@code __row_id__} = the element's own 0-based doc id (the {@link Writer} invariant), assigned
 *       sequentially in row order, so element doc id equals the element's global position — which is
 *       exactly the parent row's bridge offset plus the element's ordinal.</li>
 *   <li>the element's <em>string-family</em> leaves as postings ({@code addField} self-filters: a long
 *       or date leaf is not owned by Lucene and is skipped, matching {@code 11} §3c).</li>
 *   <li>{@link DocumentInput#NESTED_PARENT_ROW_FIELD} = the parent row id, as a doc-value.</li>
 * </ul>
 *
 * <h2>Sorted flush</h2>
 * Without an index sort, elements are written eagerly in insertion order (insertion order is the final
 * row order). With an index sort, the parquet primary renumbers parent rows on flush and emits a
 * {@link RowIdMapping} (oldRow→newRow); elements are then <b>staged</b> and, at {@link #flush}, written
 * in <em>new</em>-row order with {@code __parent_row__ = mapping.getNewRowId(oldRow)} — so element doc
 * ids follow the sorted row order and each element points at its post-sort parent. This is the
 * Phase-4a remap pattern ({@code 12} Phase W5): the mapping must cover every parent row and resolve
 * every staged row, else the flush throws rather than stamp a stale row.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class ElementIndexStack implements Closeable {

    private static final Logger logger = LogManager.getLogger(ElementIndexStack.class);

    private final long auxGeneration;
    private final DataFormat luceneFormat;
    private final Writer<DocumentInput<?>> elementWriter;
    private final IndexingExecutionEngine<?, ?> luceneDelegate;
    /**
     * True when the parent may renumber its rows at flush — exactly when the index has an index sort.
     * Elements are then staged until {@link #flush} knows the parent's permutation.
     */
    private final boolean stageUntilFlush;
    /** Staged elements awaiting the parent's {@link RowIdMapping}; always empty unless {@link #stageUntilFlush}. */
    private final List<StagedParent> staged = new ArrayList<>();
    private int stagedElementCount = 0;
    /** Next element doc id — equals the number of element docs written so far (global, row order). */
    private long elementDocId = 0L;
    private boolean closed;

    /** One parent document's elements held until its post-sort row id is known. */
    private record StagedParent(long parentRowId, List<CompositeDocumentInput.NestedElement> elements) {
    }

    /**
     * Opens the element index for one parent writer generation on the Lucene secondary delegate.
     * Callers must pre-check {@link #isEnabledFor}.
     *
     * @param engine           the composite engine whose Lucene secondary backs the element index
     * @param parentGeneration the parent writer's generation
     */
    @SuppressWarnings("unchecked")
    ElementIndexStack(CompositeIndexingExecutionEngine engine, long parentGeneration) {
        this.stageUntilFlush = engine.mapperService().getIndexSettings().getIndexSortConfig().hasIndexSort();
        this.auxGeneration = AuxiliaryDataFormat.generationFor(parentGeneration);
        this.luceneDelegate = luceneDelegate(engine);
        this.luceneFormat = luceneDelegate.getDataFormat();
        this.elementWriter = (Writer<DocumentInput<?>>) luceneDelegate.createWriter(new WriterConfig(auxGeneration));
        logger.info(
            "Opened element index at generation [{}] (parent generation [{}]), mode [{}]",
            auxGeneration,
            parentGeneration,
            stageUntilFlush ? "staged (index sort configured; parent rows renumber at flush)" : "eager (no index sort)"
        );
    }

    /** The composite's Lucene secondary delegate; the element index is a Lucene-only side index. */
    private static IndexingExecutionEngine<?, ?> luceneDelegate(CompositeIndexingExecutionEngine engine) {
        for (IndexingExecutionEngine<?, ?> delegate : engine.getSecondaryDelegates()) {
            if (delegate.getDataFormat().name().equals("lucene")) {
                return delegate;
            }
        }
        throw new IllegalStateException("Engine-4 element index requires a Lucene secondary format, but none is configured");
    }

    /** True when the mapping declares at least one {@code nested} object — the Engine-4 opt-in. */
    static boolean isEnabledFor(MapperService mapperService) {
        if (mapperService == null) {
            return false;
        }
        var documentMapper = mapperService.documentMapperWithAutoCreate().getDocumentMapper();
        if (documentMapper == null) {
            return false;
        }
        for (ObjectMapper objectMapper : documentMapper.mappers().objectMappers().values()) {
            if (objectMapper.nested().isNested()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Accepts one parent document's nested elements, keyed to the row id the parent just committed to.
     * Called by {@link CompositeWriter} after the parent row is accepted, so {@code parentRowId} is
     * final for the eager path. With an index sort the rows may still renumber at flush, so elements are
     * staged and written by {@link #flush} once the permutation is known.
     *
     * @param parentRowId this parent document's insertion-order {@code __row_id__}
     * @param elements    the parent's nested elements, in source order
     */
    void addElements(long parentRowId, List<CompositeDocumentInput.NestedElement> elements) throws IOException {
        if (stageUntilFlush) {
            staged.add(new StagedParent(parentRowId, List.copyOf(elements)));
            stagedElementCount += elements.size();
            return;
        }
        writeElements(parentRowId, elements);
    }

    /**
     * Writes one element doc per element, stamping {@code parentRow} as {@code __parent_row__}. Element
     * doc ids advance in call order, so they follow the row order the caller writes in.
     */
    private void writeElements(long parentRow, List<CompositeDocumentInput.NestedElement> elements) throws IOException {
        for (CompositeDocumentInput.NestedElement element : elements) {
            DocumentInput<?> elementDoc = luceneDelegate.newDocumentInput();
            elementDoc.setRowId(DocumentInput.ROW_ID_FIELD, elementDocId);
            List<MappedFieldType> fieldTypes = element.fieldTypes();
            List<Object> values = element.values();
            for (int i = 0; i < fieldTypes.size(); i++) {
                Object value = values.get(i);
                if (value != null) {
                    // addField self-filters: only Lucene-owned (string-family) leaves are indexed.
                    elementDoc.addField(fieldTypes.get(i), value);
                }
            }
            elementDoc.addNumericDocValue(DocumentInput.NESTED_PARENT_ROW_FIELD, parentRow);

            WriteResult result = elementWriter.addDoc(elementDoc);
            if (result instanceof WriteResult.Failure failure) {
                throw new IOException(
                    "Failed to index element doc [" + elementDocId + "] for parent row [" + parentRow + "]",
                    failure.cause()
                );
            }
            elementDocId++;
        }
    }

    /**
     * Materialises staged elements against the parent's flush permutation: writes each document's
     * elements in <em>new</em>-row order, stamping {@code __parent_row__ = mapping.getNewRowId(oldRow)}.
     * A no-op on the eager path (nothing staged). Every disagreement between what was staged and what
     * the parent actually did is raised rather than absorbed — a plausible-but-stale parent row is
     * indistinguishable from a correct one downstream (mirrors {@code NestedChildStack} Phase 4a).
     */
    private void materialiseStaged(RowIdMapping mapping, long parentRowCount) throws IOException {
        if (stageUntilFlush == false) {
            if (mapping != null) {
                throw new IllegalStateException(
                    "Parent reordered its rows at flush (RowIdMapping size ["
                        + mapping.size()
                        + "]) but element docs for generation ["
                        + auxGeneration
                        + "] were already written with insertion-order parent rows. Every __parent_row__ would be stale."
                );
            }
            return;
        }
        if (staged.isEmpty()) {
            return;
        }
        if (mapping == null) {
            throw new IllegalStateException(
                "Index sort is configured, so elements for generation ["
                    + auxGeneration
                    + "] were staged awaiting the parent's row permutation, but the primary produced no RowIdMapping. "
                    + "Cannot tell whether the parent reordered; refusing to stamp __parent_row__."
            );
        }
        if (mapping.size() != parentRowCount) {
            throw new IllegalStateException(
                "Parent RowIdMapping covers ["
                    + mapping.size()
                    + "] rows but the parent accepted ["
                    + parentRowCount
                    + "]; the permutation does not describe the rows the element parent pointers refer to"
            );
        }
        // Resolve every staged parent's new row up front (fail loud before writing anything), then write
        // in new-row order so element doc ids line up with the sorted parent rows.
        record Ordered(long newRow, StagedParent parent) {
        }
        List<Ordered> ordered = new ArrayList<>(staged.size());
        for (StagedParent parent : staged) {
            long newRow = mapping.getNewRowId(parent.parentRowId(), RowIdMapping.SINGLE_GEN);
            if (newRow < 0L) {
                throw new IllegalStateException(
                    "Parent RowIdMapping has no entry for insertion-order row id ["
                        + parent.parentRowId()
                        + "] in generation ["
                        + auxGeneration
                        + "]"
                );
            }
            ordered.add(new Ordered(newRow, parent));
        }
        ordered.sort(Comparator.comparingLong(Ordered::newRow));
        for (Ordered entry : ordered) {
            writeElements(entry.newRow(), entry.parent().elements());
        }
        logger.info(
            "Remapped [{}] element docs across [{}] parent documents through the flush permutation for generation [{}]",
            stagedElementCount,
            staged.size(),
            auxGeneration
        );
        staged.clear();
        stagedElementCount = 0;
    }

    /**
     * Flushes the element index and returns it as its own {@link Segment} for the parent's
     * {@link FileInfos#auxiliarySegments()}, keyed by the {@code aux__lucene__nested} format so the
     * catalog holds its files apart from the parent's main index. Empty when no element was written.
     *
     * @param parentRowIdMapping the permutation the parent applied to its rows during this flush, or
     *                           null when it kept insertion order
     * @param parentRowCount     rows the parent accepted, used to check the mapping covers them all
     */
    List<Segment> flush(RowIdMapping parentRowIdMapping, long parentRowCount) throws IOException {
        materialiseStaged(parentRowIdMapping, parentRowCount);
        if (elementDocId == 0L) {
            logger.debug("Element index at generation [{}] has no elements; skipping flush", auxGeneration);
            return List.of();
        }
        FileInfos elementFileInfos = elementWriter.flush(FlushInput.EMPTY);
        WriterFileSet elementFiles = elementFileInfos.getWriterFileSet(luceneFormat).orElse(null);
        if (elementFiles == null) {
            throw new IOException(
                "Element index at generation ["
                    + auxGeneration
                    + "] wrote ["
                    + elementDocId
                    + "] element docs but its Lucene format produced no files; flushed formats were "
                    + elementFileInfos.writerFilesMap().keySet()
            );
        }
        String elementFormatName = AuxiliaryDataFormat.nameFor(luceneFormat.name(), AuxiliaryDataFormat.NESTED_CHILD_ROLE);
        logger.info(
            "Element index published: generation [{}] format [{}] elements [{}] files {} in [{}]",
            auxGeneration,
            elementFormatName,
            elementDocId,
            elementFiles.files(),
            elementFiles.directory()
        );
        return List.of(Segment.builder(auxGeneration).addSearchableFiles(elementFormatName, elementFiles).build());
    }

    /** Number of element docs this stack accounts for — written plus still-staged. */
    long elementCount() {
        return elementDocId + stagedElementCount;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.close(elementWriter);
    }
}

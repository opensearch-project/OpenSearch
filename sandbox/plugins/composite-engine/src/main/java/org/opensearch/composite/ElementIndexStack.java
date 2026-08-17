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
 * <h2>v1 scope</h2>
 * Eager write in insertion order. An index sort would renumber parent rows at flush, invalidating the
 * eagerly-stamped {@code __parent_row__}; that case is rejected at construction (staged remap is
 * deferred, see {@code 12} Phase W5).
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
    /** Next element doc id — equals the number of element docs written so far (global, row order). */
    private long elementDocId = 0L;
    private boolean closed;

    /**
     * Opens the element index for one parent writer generation on the Lucene secondary delegate.
     * Callers must pre-check {@link #isEnabledFor}.
     *
     * @param engine           the composite engine whose Lucene secondary backs the element index
     * @param parentGeneration the parent writer's generation
     */
    @SuppressWarnings("unchecked")
    ElementIndexStack(CompositeIndexingExecutionEngine engine, long parentGeneration) {
        if (engine.mapperService().getIndexSettings().getIndexSortConfig().hasIndexSort()) {
            throw new IllegalStateException(
                "Engine-4 nested fields do not yet support index sort: the element index stamps parent row ids eagerly, "
                    + "which a sorted flush would renumber. Remove index.sort.* or drop the nested field (see 12 Phase W5)."
            );
        }
        this.auxGeneration = AuxiliaryDataFormat.generationFor(parentGeneration);
        this.luceneDelegate = luceneDelegate(engine);
        this.luceneFormat = luceneDelegate.getDataFormat();
        this.elementWriter = (Writer<DocumentInput<?>>) luceneDelegate.createWriter(new WriterConfig(auxGeneration));
        logger.info("Opened element index at generation [{}] (parent generation [{}])", auxGeneration, parentGeneration);
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
     * Writes one element doc per element of a parent document, in source order. Called by
     * {@link CompositeWriter} after the parent row is accepted, so {@code parentRowId} is final. Element
     * doc ids advance in row order, keeping them in step with the parent row's bridge offset.
     *
     * @param parentRowId this parent document's {@code __row_id__}
     * @param elements    the parent's nested elements, in source order
     */
    void addElements(long parentRowId, List<CompositeDocumentInput.NestedElement> elements) throws IOException {
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
            elementDoc.addNumericDocValue(DocumentInput.NESTED_PARENT_ROW_FIELD, parentRowId);

            WriteResult result = elementWriter.addDoc(elementDoc);
            if (result instanceof WriteResult.Failure failure) {
                throw new IOException(
                    "Failed to index element doc [" + elementDocId + "] for parent row [" + parentRowId + "]",
                    failure.cause()
                );
            }
            elementDocId++;
        }
    }

    /**
     * Flushes the element index and returns it as its own {@link Segment} for the parent's
     * {@link FileInfos#auxiliarySegments()}, keyed by the {@code aux__lucene__nested} format so the
     * catalog holds its files apart from the parent's main index. Empty when no element was written.
     */
    List<Segment> flush() throws IOException {
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

    /** Number of element docs this stack has written. */
    long elementCount() {
        return elementDocId;
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.DocumentMetadataResolver.DocumentMetadata;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.indices.IndicesModule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Core orchestrator for document lookup. Coordinates row-location resolution,
 * backend-specific execution, and result assembly.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DocumentLookupService {

    /**
     * Engine/storage metadata fields excluded from reconstructed {@code _source}, sourced from the mapper
     * registry's built-in metadata mappers rather than a hand-maintained list. {@code _primary_term} (a
     * sub-column emitted by the {@code _seq_no} mapper) and {@code __row_id__} (an engine-internal column)
     * are not registered mappers, so they are excluded separately by constant in {@link #buildResultFromRow}.
     */
    private static final Set<String> METADATA_FIELDS = IndicesModule.getBuiltInMetadataFields();

    private final DocumentMetadataResolver documentResolver;
    private final DocumentRowReader executor;

    public DocumentLookupService(DocumentMetadataResolver documentResolver, DocumentRowReader executor) {
        this.documentResolver = documentResolver;
        this.executor = executor;
    }

    public DocumentLookupResult getById(String id, IndexReaderProvider.Reader reader, Index index) throws IOException {
        DocumentMetadata metadata = documentResolver.resolveMetadata(reader, id);
        if (metadata == null) {
            return DocumentLookupResult.notFound(id);
        }
        return buildResultFromRow(id, fetchRow(metadata, reader));
    }

    /**
     * Resolves version metadata without reconstructing {@code _source}. Uses resolver metadata when
     * available; legacy segments without these doc values fall back to a primary-store row lookup.
     */
    public DocumentLookupResult getVersionMetadata(String id, IndexReaderProvider.Reader reader, Index index) throws IOException {
        DocumentMetadata metadata = documentResolver.resolveMetadata(reader, id);
        if (metadata == null) {
            return DocumentLookupResult.notFound(id);
        }
        if (metadata.hasVersionMetadata()) {
            return new DocumentLookupResult(
                id,
                metadata.version(),
                true,
                null,
                metadata.seqNo(),
                metadata.primaryTerm(),
                Map.of(),
                Map.of()
            );
        }
        Map<String, Object> row = fetchRow(metadata, reader);
        long seqNo = extractLong(row, "_seq_no", SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = extractLong(row, "_primary_term", SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        long version = extractLong(row, "_version", Versions.NOT_FOUND);
        return new DocumentLookupResult(id, version, true, null, seqNo, primaryTerm, Map.of(), Map.of());
    }

    /** Fetches the raw row for an already-resolved document location. */
    private Map<String, Object> fetchRow(DocumentMetadata metadata, IndexReaderProvider.Reader reader) throws IOException {
        String id = metadata.id();
        WriterFileSet fileSet = reader.catalogSnapshot().findFileSet(executor.formatName(), metadata.writerGeneration());
        if (fileSet == null) {
            throw new IllegalStateException(
                "Resolver located id ["
                    + id
                    + "] at writer generation ["
                    + metadata.writerGeneration()
                    + "] but no matching file set was found"
            );
        }

        Map<String, Object> row = executor.executeSingleRow(metadata.rowId(), fileSet);
        if (row == null) {
            throw new IllegalStateException(
                "Resolver located id ["
                    + id
                    + "] at writer generation ["
                    + metadata.writerGeneration()
                    + "] rowId ["
                    + metadata.rowId()
                    + "] but backend returned no row"
            );
        }
        return row;
    }

    public List<DocumentLookupResult> getDocsAboveSeqNo(long fromSeqNoExclusive, IndexReaderProvider.Reader reader, Index index)
        throws IOException {
        List<WriterFileSet> fileSets = new ArrayList<>();
        for (Segment segment : reader.catalogSnapshot().getSegments()) {
            WriterFileSet fileSet = segment.dfGroupedSearchableFiles().get(executor.formatName());
            if (fileSet != null && !fileSet.files().isEmpty()) {
                fileSets.add(fileSet);
            }
        }
        List<DocumentLookupResult> results = new ArrayList<>();
        for (Map<String, Object> row : executor.executeRowsAboveSeqNo(fileSets, fromSeqNoExclusive)) {
            Object idVal = row.get("_id");
            if (idVal != null) {
                results.add(buildResultFromRow(idVal.toString(), row));
            }
        }
        return results;
    }

    /** Builds a DocumentLookupResult from a raw row, filtering metadata/internal fields out of {@code _source}. */
    private static DocumentLookupResult buildResultFromRow(String id, Map<String, Object> row) throws IOException {
        long seqNo = extractLong(row, "_seq_no", SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = extractLong(row, "_primary_term", SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        long version = extractLong(row, "_version", Versions.NOT_FOUND);

        BytesReference source = asBytesReference(row.get(SourceFieldMapper.NAME));
        if (source == null) {
            // Reconstruct _source only when it was not stored. This is intentionally limited to
            // append-only indexes because column values cannot reproduce the original source exactly.
            Map<String, Object> filtered = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : row.entrySet()) {
                // Exclude metadata and engine-internal columns from reconstructed _source.
                String name = e.getKey();
                if (METADATA_FIELDS.contains(name)
                    || SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(name)
                    || DocumentInput.ROW_ID_FIELD.equals(name)) {
                    continue;
                }
                filtered.put(name, e.getValue());
            }
            try (XContentBuilder xcb = XContentFactory.jsonBuilder()) {
                xcb.map(filtered);
                source = BytesReference.bytes(xcb);
            }
        }

        return new DocumentLookupResult(id, version, true, source, seqNo, primaryTerm, Map.of(), Map.of());
    }

    /**
     * Converts a stored {@code _source} value to {@link BytesReference}, or returns {@code null} when
     * source must be reconstructed.
     */
    private static BytesReference asBytesReference(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BytesReference br) {
            return br;
        }
        if (value instanceof BytesRef ref) {
            return new BytesArray(ref.bytes, ref.offset, ref.length);
        }
        if (value instanceof byte[] bytes) {
            return new BytesArray(bytes);
        }
        if (value instanceof ByteBuffer buf) {
            byte[] copy = new byte[buf.remaining()];
            buf.duplicate().get(copy);
            return new BytesArray(copy);
        }
        throw new IllegalStateException("Unsupported _source column value type: " + value.getClass().getName());
    }

    public static long extractLong(Map<String, Object> row, String key, long fallback) {
        Object v = row.get(key);
        if (v == null) return fallback;
        if (v instanceof Number) return ((Number) v).longValue();
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}

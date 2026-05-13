/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.get;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.engine.Engine;

import java.util.Map;
import java.util.Objects;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Minimal result of a pluggable get-by-id lookup. Decouples callers from
 * {@link org.opensearch.index.engine.Engine.GetResult}, which is structured
 * around a Lucene {@code Searcher} and {@code DocIdAndVersion}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DocumentLookupResult {

    private final String id;
    private final long version;
    private final boolean exists;
    @Nullable
    private final BytesReference source;
    private final long seqNo;
    private final long primaryTerm;
    private final Map<String, DocumentField> documentFields;
    private final Map<String, DocumentField> metadataFields;

    public static DocumentLookupResult notFound(String id) {
        return new DocumentLookupResult(id, Versions.NOT_FOUND, false, null, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, Map.of(), Map.of());
    }

    public DocumentLookupResult(
        String id,
        long version,
        boolean exists,
        @Nullable BytesReference source,
        long seqNo,
        long primaryTerm,
        Map<String, DocumentField> documentFields,
        Map<String, DocumentField> metadataFields
    ) {
        this.id = id;
        this.version = version;
        this.exists = exists;
        this.source = source;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.documentFields = documentFields == null ? Map.of() : documentFields;
        this.metadataFields = metadataFields == null ? Map.of() : metadataFields;
    }

    public String id() {
        return id;
    }

    public long version() {
        return version;
    }

    public boolean exists() {
        return exists;
    }

    @Nullable
    public BytesReference source() {
        return source;
    }

    public long seqNo() {
        return seqNo;
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public Map<String, DocumentField> documentFields() {
        return documentFields;
    }

    public Map<String, DocumentField> metadataFields() {
        return metadataFields;
    }

    /**
     * Wraps this lookup as an {@link Engine.GetResult} so {@link ShardGetService} can
     * resolve a non-Lucene get-by-id through the same {@code IndexShard.get(Engine.Get)}
     * return type as the stored-fields path. {@link ShardGetService} detects the
     * {@link PreMaterialized} shape and skips the Lucene stored-fields load.
     *
     * <p>The synthesized {@link DocIdAndVersion} carries the lookup's version / seqNo /
     * primaryTerm; its {@code reader} and {@code docId} are {@code null}/{@code -1} and
     * must not be dereferenced — {@code ShardGetService} only reads them on the
     * stored-fields path, which is skipped for the pre-materialized case.
     */
    public Engine.GetResult toGetResult() {
        return new PreMaterialized(this);
    }

    /**
     * Marker {@link Engine.GetResult} carrying a {@link DocumentLookupResult}. Constructed
     * via {@link DocumentLookupResult#toGetResult()}; detected in {@link ShardGetService}
     * via {@code instanceof} so the pluggable get-by-id path can materialize source/fields
     * from the lookup instead of the Lucene stored-fields visitor.
     */
    public static final class PreMaterialized extends Engine.GetResult {
        private final DocumentLookupResult lookup;

        private PreMaterialized(DocumentLookupResult lookup) {
            super(null, new DocIdAndVersion(-1, lookup.version, lookup.seqNo, lookup.primaryTerm, null, 0), false);
            this.lookup = lookup;
        }

        public DocumentLookupResult lookup() {
            return lookup;
        }

        @Override
        public boolean exists() {
            return lookup.exists;
        }

        @Override
        public long version() {
            return lookup.version;
        }

        @Override
        public void close() {
            // no searcher to release
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DocumentLookupResult == false) return false;
        DocumentLookupResult other = (DocumentLookupResult) o;
        return version == other.version
            && exists == other.exists
            && seqNo == other.seqNo
            && primaryTerm == other.primaryTerm
            && Objects.equals(id, other.id)
            && Objects.equals(source, other.source)
            && Objects.equals(documentFields, other.documentFields)
            && Objects.equals(metadataFields, other.metadataFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, exists, source, seqNo, primaryTerm, documentFields, metadataFields);
    }
}

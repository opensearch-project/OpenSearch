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
 * Minimal result of get-by-id lookup. Decouples callers from
 * {@link Engine.GetResult}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record DocumentLookupResult(String id, long version, boolean exists, @Nullable BytesReference source, long seqNo, long primaryTerm,
    Map<String, DocumentField> documentFields, Map<String, DocumentField> metadataFields) {

    public static DocumentLookupResult notFound(String id) {
        return new DocumentLookupResult(
            id,
            Versions.NOT_FOUND,
            false,
            null,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            Map.of(),
            Map.of()
        );
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

    /**
     * Wraps this lookup as a {@link PreMaterialized} {@link Engine.GetResult} so the get-by-id path
     * shares the {@code IndexShard.get(Engine.Get)} return type. The synthesized
     * {@link DocIdAndVersion} carries version/seqNo/primaryTerm only; its reader/docId are unset
     * and must not be dereferenced.
     */
    public Engine.GetResult toGetResult() {
        return new PreMaterialized(this);
    }

    /**
     * {@link Engine.GetResult} carrying a {@link DocumentLookupResult}. {@link ShardGetService}
     * detects it via {@code instanceof} and materializes source/fields from the lookup instead of
     * the engine stored-fields path. Has no searcher/docId — those accessors throw.
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
        public Engine.Searcher searcher() {
            throw new UnsupportedOperationException("PreMaterialized has no searcher");
        }

        @Override
        public DocIdAndVersion docIdAndVersion() {
            throw new UnsupportedOperationException("PreMaterialized has no docId");
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

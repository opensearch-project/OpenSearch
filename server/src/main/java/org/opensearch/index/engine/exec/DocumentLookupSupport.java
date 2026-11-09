/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.plugins.DocumentLookupProvider;

import java.io.IOException;

/**
 * Shared get-by-id helper for the {@code DataFormatAware*} engines. Centralizes the pluggable lookup
 * (via the installed {@link DocumentLookupProvider}) and the read-time version-conflict checks so
 * the engines don't duplicate them. Immutable and thread-safe; one per shard.
 *
 * @opensearch.internal
 */
public final class DocumentLookupSupport {

    private final ShardId shardId;
    @Nullable
    private final DocumentLookupProvider provider;
    private final DocumentMetadataResolver resolver;

    public DocumentLookupSupport(ShardId shardId, @Nullable DocumentLookupProvider provider, DocumentMetadataResolver resolver) {
        this.shardId = shardId;
        this.provider = provider;
        this.resolver = resolver;
    }

    /** Whether a {@link DocumentLookupProvider} is installed (i.e. get-by-id is supported). */
    public boolean isSupported() {
        return provider != null;
    }

    /**
     * Resolves {@code get} against {@code reader} through the installed provider.
     * Returns {@link DocumentLookupResult#notFound} when the reader snapshot has no segments.
     *
     * @throws UnsupportedOperationException if no {@link DocumentLookupProvider} is installed
     */
    public DocumentLookupResult lookupFromReader(Engine.Get get, IndexReaderProvider.Reader reader) throws IOException {
        if (provider == null) {
            throw new UnsupportedOperationException("getById not supported: no DocumentLookupProvider installed");
        }
        if (reader.catalogSnapshot().getSegments().isEmpty()) {
            return DocumentLookupResult.notFound(get.id());
        }
        return provider.getById(get, reader, shardId.getIndex(), resolver);
    }

    /**
     * Applies read-time version-conflict checks against a resolved {@code result}, mirroring the
     * get semantics in {@code InternalEngine}. A no-op when the document does not exist.
     *
     * @throws VersionConflictEngineException if the requested version or {@code if_seq_no} /
     *         {@code if_primary_term} preconditions conflict with the resolved document
     */
    public void applyReadVersionConflicts(Engine.Get get, DocumentLookupResult result) {
        if (result.exists() == false) {
            return;
        }
        if (get.versionType().isVersionConflictForReads(result.version(), get.version())) {
            throw new VersionConflictEngineException(
                shardId,
                get.id(),
                get.versionType().explainConflictForReads(result.version(), get.version())
            );
        }
        if (get.getIfSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
            && (get.getIfSeqNo() != result.seqNo() || get.getIfPrimaryTerm() != result.primaryTerm())) {
            throw new VersionConflictEngineException(
                shardId,
                get.id(),
                get.getIfSeqNo(),
                get.getIfPrimaryTerm(),
                result.seqNo(),
                result.primaryTerm()
            );
        }
    }

    /**
     * Convenience: {@link #lookupFromReader} followed by {@link #applyReadVersionConflicts}.
     * Used by the read-only and NRT replica engines, which read directly from the segment
     * snapshot without a live version map.
     */
    public DocumentLookupResult getById(Engine.Get get, IndexReaderProvider.Reader reader) throws IOException {
        DocumentLookupResult result = lookupFromReader(get, reader);
        applyReadVersionConflicts(get, result);
        return result;
    }
}

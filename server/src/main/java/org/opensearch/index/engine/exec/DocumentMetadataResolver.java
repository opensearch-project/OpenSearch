/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Resolves a document's physical row location ({@code rowId}, {@code writerGeneration}) from the
 * index's secondary structure, used to fetch the row from the primary store. Returns {@code null}
 * ("not found") when there is no id present.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentMetadataResolver {

    /** Sentinel for {@link DocumentMetadata} location fields that were not populated. */
    long UNSET = -1L;

    /** No-op resolver used when no backend provides one. */
    DocumentMetadataResolver NOOP = new DocumentMetadataResolver() {
        @Override
        public DocumentMetadata resolveMetadata(IndexReaderProvider.Reader reader, String id) {
            return null;
        }
    };

    /**
     * Physical row location for a document, plus its version metadata. When it does not, the version fields are {@link #UNSET} and the caller must read
     * them from the primary store.
     *
     * @param id the document id
     * @param rowId the row offset within the data file
     * @param writerGeneration the writer generation identifying the data file
     * @param version the document version, or {@link #UNSET}
     * @param seqNo the sequence number, or {@link #UNSET}
     * @param primaryTerm the primary term, or {@link #UNSET}
     */
    @ExperimentalApi
    record DocumentMetadata(String id, long rowId, long writerGeneration, long version, long seqNo, long primaryTerm) {

        /** Row location only; version metadata reports {@link #UNSET}. */
        public DocumentMetadata(String id, long rowId, long writerGeneration) {
            this(id, rowId, writerGeneration, UNSET, UNSET, UNSET);
        }

        /** Whether version metadata was resolved and can be used without reading the primary store. */
        public boolean hasVersionMetadata() {
            return version != UNSET && seqNo != UNSET && primaryTerm != UNSET;
        }
    }

    /**
     * Resolve row location for a document id.
     *
     * @param reader the point-in-time reader snapshot
     * @param id the document id to resolve
     * @return the {@link DocumentMetadata}, or {@code null} if not found
     */
    DocumentMetadata resolveMetadata(IndexReaderProvider.Reader reader, String id) throws IOException;
}

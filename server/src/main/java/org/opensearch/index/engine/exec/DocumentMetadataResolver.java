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
     * Physical row location for a document. Version metadata lives in the primary store, not here.
     *
     * @param id the document id
     * @param rowId the row offset within the data file
     * @param writerGeneration the writer generation identifying the data file
     */
    @ExperimentalApi
    record DocumentMetadata(String id, long rowId, long writerGeneration) {
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.get.DocumentLookupResult;

import java.io.IOException;
import java.util.List;

/**
 * SPI for pluggable get-by-id lookup. Implementations resolve a document id
 * against the shard's current reader snapshot and return a
 * {@link DocumentLookupResult} with the source (and optionally other fields)
 * for the matching row.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentLookupProvider {

    /**
     * Resolve the document referenced by {@code get} against {@code reader}.
     *
     * @param get the get request (id, realtime flag, etc.)
     * @param reader a point-in-time index reader acquired by the caller
     * @param index the index, used by implementations as the scan table name
     * @param resolver the {@link DocumentMetadataResolver} used to resolve a document's row location
     * @return the lookup result; never {@code null}
     * @throws IOException if resolution fails
     */
    DocumentLookupResult getById(Engine.Get get, IndexReaderProvider.Reader reader, Index index, DocumentMetadataResolver resolver)
        throws IOException;

    /**
     * Resolves only version metadata ({@code _version}/{@code _seq_no}/{@code _primary_term}) for an id,
     * skipping {@code _source} reconstruction. The {@code resolver} resolves the document's row location.
     */
    default DocumentLookupResult getVersionMetadata(
        String id,
        IndexReaderProvider.Reader reader,
        Index index,
        DocumentMetadataResolver resolver
    ) throws IOException {
        return DocumentLookupResult.notFound(id);
    }

    /**
     * Returns metadata for all documents with {@code _seq_no > fromSeqNoExclusive}. Default returns empty list.
     * The {@code resolver} resolves each document's row location.
     */
    default List<DocumentLookupResult> getDocsAboveSeqNo(
        long fromSeqNoExclusive,
        IndexReaderProvider.Reader reader,
        Index index,
        DocumentMetadataResolver resolver
    ) throws IOException {
        return List.of();
    }
}

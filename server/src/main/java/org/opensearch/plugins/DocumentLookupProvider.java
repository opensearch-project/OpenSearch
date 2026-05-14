/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.get.DocumentLookupResult;

import java.io.IOException;

/**
 * SPI for pluggable get-by-id lookup. Implementations resolve a document id
 * against the shard's current reader snapshot and return a
 * {@link DocumentLookupResult} with the source (and optionally other fields)
 * for the matching row.
 *
 * <p>Wired on the non-Lucene read path when the active indexer is not an
 * {@code EngineBackedIndexer}. At most one implementation may be installed;
 * multiple registrations cause startup to fail with {@code IllegalStateException}.
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
     * @param indexName the index name, used by implementations as the scan table name
     * @return the lookup result; never {@code null}
     * @throws IOException if resolution fails
     */
    DocumentLookupResult getById(Engine.Get get, IndexReaderProvider.Reader reader, String indexName) throws IOException;
}

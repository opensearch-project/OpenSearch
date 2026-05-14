/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.Nullable;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.plugins.DocumentLookupProvider;

/**
 * {@link IndexerFactory} that creates a {@link DataFormatAwareEngine},
 * used when the pluggable data format feature is enabled.
 *
 * @opensearch.internal
 */
public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Nullable
    private DocumentLookupProvider documentLookupProvider;

    /** Wires the optional {@link DocumentLookupProvider} used by {@link DataFormatAwareEngine#getById}. */
    public void setGetByIdPlugin(@Nullable DocumentLookupProvider documentLookupProvider) {
        this.documentLookupProvider = documentLookupProvider;
    }

    @Override
    public Indexer createIndexer(EngineConfig config) {
        return new DataFormatAwareEngine(config, documentLookupProvider);
    }
}

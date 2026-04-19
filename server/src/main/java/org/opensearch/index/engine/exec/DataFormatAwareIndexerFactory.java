/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.EngineConfig;

/**
 * {@link IndexerFactory} that creates a {@link DataFormatAwareEngine},
 * used when the pluggable data format feature is enabled.
 *
 * @opensearch.internal
 */
public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Override
    public Indexer createIndexer(EngineConfig config) {
        return new DataFormatAwareEngine(config);
    }
}

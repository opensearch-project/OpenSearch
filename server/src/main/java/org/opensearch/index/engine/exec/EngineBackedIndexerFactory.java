/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.EngineBackedIndexer;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;

/**
 * {@link IndexerFactory} that wraps an {@link EngineFactory} to produce an
 * {@link EngineBackedIndexer}. This is the default factory used for the
 * traditional Lucene-based indexing path.
 *
 * @opensearch.internal
 */
public class EngineBackedIndexerFactory implements IndexerFactory {

    private final EngineFactory engineFactory;

    public EngineBackedIndexerFactory(EngineFactory engineFactory) {
        this.engineFactory = engineFactory;
    }

    @Override
    public Indexer createIndexer(EngineConfig engineConfig) {
        return new EngineBackedIndexer(engineFactory.newReadWriteEngine(engineConfig));
    }

    public EngineFactory getEngineFactory() {
        return engineFactory;
    }
}

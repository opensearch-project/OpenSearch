/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;

/**
 * Engine Factory implementation used with streaming ingestion.
 */
public class IngestionEngineFactory implements EngineFactory {
    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        return new IngestionEngineV1(config);
    }
}

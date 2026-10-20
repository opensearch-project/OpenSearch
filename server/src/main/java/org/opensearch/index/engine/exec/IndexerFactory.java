/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineConfig;

/**
 * Factory for creating {@link Indexer} instances from an {@link EngineConfig}.
 * <p>
 * Implementations decide which {@link Indexer} to instantiate based on the
 * engine configuration — for example, an engine-backed indexer that wraps a
 * Lucene {@code InternalEngine}, or a {@code DataFormatAwareEngine} for
 * pluggable data formats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface IndexerFactory {

    Indexer createIndexer(EngineConfig config);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

/**
 * {@link HybridQueryExecutorCollectorManager} is responsible for creating new {@link HybridQueryExecutorCollector} instances
 */
public interface HybridQueryExecutorCollectorManager<C extends HybridQueryExecutorCollector> {
    /**
     * Return a new Collector instance that extends {@link HybridQueryExecutor}.
     * This will be used during Hybrid Search when sub queries wants to execute part of
     * operation that is independent of each other that can be parallelized to improve
     * the performance.
     * @return HybridQueryExecutorCollector
     */
    C newCollector();
}

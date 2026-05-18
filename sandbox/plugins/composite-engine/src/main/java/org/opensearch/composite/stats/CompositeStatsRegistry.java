/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton registry that tracks all active {@link CompositeIndexingExecutionEngine} instances
 * by their {@link ShardId}. Thread-safe via ConcurrentHashMap.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeStatsRegistry {

    private static final CompositeStatsRegistry INSTANCE = new CompositeStatsRegistry();

    private final ConcurrentHashMap<ShardId, CompositeIndexingExecutionEngine> engines = new ConcurrentHashMap<>();
    private volatile IndicesService indicesService;

    private CompositeStatsRegistry() {}

    public static CompositeStatsRegistry getInstance() {
        return INSTANCE;
    }

    public void setIndicesService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    public IndicesService getIndicesService() {
        return indicesService;
    }

    public void register(ShardId shardId, CompositeIndexingExecutionEngine engine) {
        engines.put(shardId, engine);
    }

    public void unregister(ShardId shardId) {
        engines.remove(shardId);
    }

    public Map<ShardId, CompositeIndexingExecutionEngine> getEngines() {
        return Collections.unmodifiableMap(engines);
    }
}

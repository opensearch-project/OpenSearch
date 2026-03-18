/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

/**
 * Opaque context representing a shard-level execution environment.
 *
 * <p>The analytics engine creates an instance of this context for each
 * shard being queried and passes it to back-end plugins via
 * {@link EngineBridge#initialize(ShardExecutionContext)}. Back-end
 * plugins that need shard-level resources (e.g., Lucene readers,
 * query contexts) downcast to the concrete implementation provided
 * by the engine.
 *
 * <p>This interface lives in {@code analytics-framework} (a {@code libs}
 * module) and intentionally does not reference server types like
 * {@code IndexShard} or {@code QueryShardContext}. Concrete
 * implementations that wrap those types live in the engine plugin.
 *
 * @opensearch.internal
 */
public interface ShardExecutionContext {

    /**
     * Returns the shard identifier (index name + shard number) for
     * logging and diagnostics.
     */
    String shardId();
}

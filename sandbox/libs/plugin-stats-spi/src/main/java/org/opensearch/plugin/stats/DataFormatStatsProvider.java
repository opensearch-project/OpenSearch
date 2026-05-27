/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.util.Optional;

/**
 * SPI implemented by each data-format plugin to expose its stats via the per-format
 * REST endpoints:
 * <ul>
 *   <li>{@code GET /_plugins/{formatName}/{index}/_stats}</li>
 *   <li>{@code GET /_plugins/{formatName}/_nodes/[{nodeId}/]_stats}</li>
 * </ul>
 *
 * <p>Each plugin owns its endpoints — the plugin registers REST handlers and transport
 * actions itself in its {@code getRestHandlers()} / {@code getActions()}. Composite-engine
 * has no role in stats endpoint registration.
 *
 * <p>Adding a new data format:
 * <ol>
 *   <li>Implement this interface in the new format's plugin</li>
 *   <li>Register it via {@code new MyStatsProvider()} in the plugin's
 *       {@code createGuiceModules()} so the registry is populated</li>
 *   <li>Subclass the SPI's transport bases and register them via
 *       {@code ActionPlugin.getActions()} / {@code getRestHandlers()}</li>
 * </ol>
 *
 * @param <T> concrete shard-stats type (e.g., {@code ParquetShardStats})
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatStatsProvider<T extends DataFormatShardStats<T>> {

    /**
     * Identifier for this data format. Used as the URL path component
     * (e.g., {@code "parquet"} maps to {@code /_plugins/parquet/...}).
     * Must be unique across all data format plugins loaded.
     */
    String formatName();

    /**
     * Returns shard-level stats for the given shard, or empty if this format
     * has no engine for that shard (e.g., the format is not configured on the index).
     *
     * <p>Called on the data node that hosts the shard, by the per-index transport.
     */
    Optional<T> shardStats(ShardId shardId);

    /**
     * Returns aggregate stats summed across all shards of this format on the
     * local node, or empty if no shards of this format are present on this node.
     *
     * <p>Called on each target node by the per-node transport.
     */
    Optional<T> aggregateNodeStats();

    /**
     * Reader for deserializing wire-encoded stats from another node. Used by
     * the transport to reconstruct {@link DataFormatShardStats} sent across
     * the cluster.
     */
    Writeable.Reader<T> shardStatsReader();
}

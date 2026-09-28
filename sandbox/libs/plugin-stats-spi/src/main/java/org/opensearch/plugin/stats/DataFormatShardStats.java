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
import org.opensearch.core.xcontent.ToXContentFragment;

/**
 * Marker interface for shard-level statistics emitted by a {@link DataFormatStatsProvider}.
 *
 * <p>Implementations supply both wire-format serialization ({@link Writeable}) and REST
 * rendering ({@link ToXContentFragment}). The single behavioral method is {@link #add},
 * which lets each format define its own per-counter aggregation rules (e.g., sum for
 * monotonic counters, max for high-water marks, last-write-wins for gauges).
 *
 * <p>The recursive type bound {@code T extends DataFormatShardStats<T>} lets the coordinator
 * merge typed snapshots without casting and without relying on generic JSON re-parsing.
 *
 * @param <T> concrete implementing class (e.g., {@code ParquetShardStats})
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatShardStats<T extends DataFormatShardStats<T>> extends Writeable, ToXContentFragment {

    /**
     * Merge this snapshot with another and return the combined result.
     *
     * <p>Each implementation chooses the per-counter rule (sum, max, min, last). Counters
     * that monotonically grow (e.g., {@code docs_indexed_total}) are typically summed;
     * gauges and high-water marks (e.g., {@code current_generation}) are typically maxed.
     *
     * <p>Implementations should be pure (no mutation of {@code this} or {@code other}).
     */
    T add(T other);
}

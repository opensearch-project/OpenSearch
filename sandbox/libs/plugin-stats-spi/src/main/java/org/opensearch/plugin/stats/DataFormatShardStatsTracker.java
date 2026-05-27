/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Mutable tracker interface for shard-level data format statistics.
 * Implementations accumulate metrics and produce an immutable {@link DataFormatShardStats} snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatShardStatsTracker {

    void addDocsIndexed(long count);

    void addIndexTime(long millis);

    void incIndexFailures();

    void incMergeTotal();

    void addMergeTime(long millis);

    void incMergeFailures();

    DataFormatShardStats stats();
}

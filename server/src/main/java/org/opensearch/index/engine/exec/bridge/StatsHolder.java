/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.search.suggest.completion.CompletionStats;

@PublicApi(since = "1.0.0")
public interface StatsHolder {

    CommitStats commitStats();

    DocsStats docStats();

    SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments);

    CompletionStats completionStats(String... fieldNamePatterns);

    PollingIngestStats pollingIngestStats();

    MergeStats getMergeStats();
}

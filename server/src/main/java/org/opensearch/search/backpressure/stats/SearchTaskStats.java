/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;

import java.io.IOException;
import java.util.Map;

/**
 * Stats related to cancelled SearchTasks.
 */

public class SearchTaskStats extends SearchBackpressureTaskStats {

    public SearchTaskStats(
        long cancellationCount,
        long limitReachedCount,
        Map<TaskResourceUsageTrackerType, TaskResourceUsageTracker.Stats> resourceUsageTrackerStats
    ) {
        super(cancellationCount, limitReachedCount, resourceUsageTrackerStats);
    }

    public SearchTaskStats(StreamInput in) throws IOException {
        super(in);
    }
}

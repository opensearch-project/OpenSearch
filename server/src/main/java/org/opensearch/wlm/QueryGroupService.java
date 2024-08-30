/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.stats.QueryGroupStats;
import org.opensearch.wlm.stats.QueryGroupStats.QueryGroupStatsHolder;

import java.util.HashMap;
import java.util.Map;

/**
 * As of now this is a stub and main implementation PR will be raised soon.Coming PR will collate these changes with core QueryGroupService changes
 */
public class QueryGroupService {
    // This map does not need to be concurrent since we will process the cluster state change serially and update
    // this map with new additions and deletions of entries. QueryGroupState is thread safe
    private final Map<String, QueryGroupState> queryGroupStateMap;

    public QueryGroupService() {
        this(new HashMap<>());
    }

    public QueryGroupService(Map<String, QueryGroupState> queryGroupStateMap) {
        this.queryGroupStateMap = queryGroupStateMap;
    }

    /**
     * updates the failure stats for the query group
     * @param queryGroupId query group identifier
     */
    public void incrementFailuresFor(final String queryGroupId) {
        QueryGroupState queryGroupState = queryGroupStateMap.get(queryGroupId);
        // This can happen if the request failed for a deleted query group
        // or new queryGroup is being created and has not been acknowledged yet
        if (queryGroupState == null) {
            return;
        }
        queryGroupState.failures.inc();
    }

    /**
     *
     * @return node level query group stats
     */
    public QueryGroupStats nodeStats() {
        final Map<String, QueryGroupStatsHolder> statsHolderMap = new HashMap<>();
        for (Map.Entry<String, QueryGroupState> queryGroupsState : queryGroupStateMap.entrySet()) {
            final String queryGroupId = queryGroupsState.getKey();
            final QueryGroupState currentState = queryGroupsState.getValue();

            statsHolderMap.put(queryGroupId, QueryGroupStatsHolder.from(currentState));
        }

        return new QueryGroupStats(statsHolderMap);
    }

    /**
     *
     * @param queryGroupId query group identifier
     */
    public void rejectIfNeeded(String queryGroupId) {
        if (queryGroupId == null) return;
        boolean reject = false;
        final StringBuilder reason = new StringBuilder();
        // TODO: At this point this is dummy and we need to decide whether to cancel the request based on last
        // reported resource usage for the queryGroup. We also need to increment the rejection count here for the
        // query group
        if (reject) {
            throw new OpenSearchRejectedExecutionException("QueryGroup " + queryGroupId + " is already contended." + reason.toString());
        }
    }
}

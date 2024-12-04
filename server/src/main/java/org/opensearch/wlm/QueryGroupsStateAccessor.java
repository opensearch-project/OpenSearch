/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.wlm.stats.QueryGroupState;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to decouple {@link QueryGroupService} and {@link org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService} to share the
 * {@link QueryGroupState}s
 */
public class QueryGroupsStateAccessor {
    // This map does not need to be concurrent since we will process the cluster state change serially and update
    // this map with new additions and deletions of entries. QueryGroupState is thread safe
    private final Map<String, QueryGroupState> queryGroupStateMap;

    public QueryGroupsStateAccessor() {
        this(new HashMap<>());
    }

    public QueryGroupsStateAccessor(Map<String, QueryGroupState> queryGroupStateMap) {
        this.queryGroupStateMap = queryGroupStateMap;
    }

    /**
     * returns the query groups state
     */
    public Map<String, QueryGroupState> getQueryGroupStateMap() {
        return queryGroupStateMap;
    }

    /**
     * return QueryGroupState for the given queryGroupId
     * @param queryGroupId
     * @return QueryGroupState for the given queryGroupId, if id is invalid return default query group state
     */
    public QueryGroupState getQueryGroupState(String queryGroupId) {
        return queryGroupStateMap.getOrDefault(queryGroupId, queryGroupStateMap.get(QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get()));
    }

    /**
     * adds new QueryGroupState against given queryGroupId
     * @param queryGroupId
     */
    public void addNewQueryGroup(String queryGroupId) {
        this.queryGroupStateMap.putIfAbsent(queryGroupId, new QueryGroupState());
    }

    /**
     * removes QueryGroupState against given queryGroupId
     * @param queryGroupId
     */
    public void removeQueryGroup(String queryGroupId) {
        this.queryGroupStateMap.remove(queryGroupId);
    }
}

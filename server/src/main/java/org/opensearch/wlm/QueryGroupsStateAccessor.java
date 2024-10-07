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

    public Map<String, QueryGroupState> getQueryGroupStateMap() {
        return queryGroupStateMap;
    }

    public QueryGroupState getQueryGroupState(String queryGroupId) {
        return queryGroupStateMap.getOrDefault(queryGroupId, queryGroupStateMap.get(QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get()));
    }

    public void addNewQueryGroup(String queryGroupId) {
        this.queryGroupStateMap.putIfAbsent(queryGroupId, new QueryGroupState());
    }

    public void removeQueryGroup(String queryGroupId) {
        this.queryGroupStateMap.remove(queryGroupId);
    }
}

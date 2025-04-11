/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to decouple {@link WorkloadGroupService} and {@link org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService} to share the
 * {@link WorkloadGroupState}s
 */
public class WorkloadGroupsStateAccessor {
    // This map does not need to be concurrent since we will process the cluster state change serially and update
    // this map with new additions and deletions of entries. WorkloadGroupState is thread safe
    private final Map<String, WorkloadGroupState> queryGroupStateMap;

    public WorkloadGroupsStateAccessor() {
        this(new HashMap<>());
    }

    public WorkloadGroupsStateAccessor(Map<String, WorkloadGroupState> queryGroupStateMap) {
        this.queryGroupStateMap = queryGroupStateMap;
    }

    /**
     * returns the query groups state
     */
    public Map<String, WorkloadGroupState> getWorkloadGroupStateMap() {
        return queryGroupStateMap;
    }

    /**
     * return WorkloadGroupState for the given queryGroupId
     * @param queryGroupId
     * @return WorkloadGroupState for the given queryGroupId, if id is invalid return default query group state
     */
    public WorkloadGroupState getWorkloadGroupState(String queryGroupId) {
        return queryGroupStateMap.getOrDefault(queryGroupId, queryGroupStateMap.get(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get()));
    }

    /**
     * adds new WorkloadGroupState against given queryGroupId
     * @param queryGroupId
     */
    public void addNewWorkloadGroup(String queryGroupId) {
        this.queryGroupStateMap.putIfAbsent(queryGroupId, new WorkloadGroupState());
    }

    /**
     * removes WorkloadGroupState against given queryGroupId
     * @param queryGroupId
     */
    public void removeWorkloadGroup(String queryGroupId) {
        this.queryGroupStateMap.remove(queryGroupId);
    }
}

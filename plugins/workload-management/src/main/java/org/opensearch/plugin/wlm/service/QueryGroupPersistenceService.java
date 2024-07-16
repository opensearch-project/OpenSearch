/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.QueryGroup.ResiliencyMode;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler.ThrottlingKey;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.UpdateQueryGroupRequest;
import org.opensearch.plugin.wlm.UpdateQueryGroupResponse;
import org.opensearch.search.ResourceType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

import static org.opensearch.search.query_group.QueryGroupServiceSettings.MAX_QUERY_GROUP_COUNT;

/**
 * This class defines the functions for QueryGroup persistence
 */
public class QueryGroupPersistenceService implements Persistable<QueryGroup> {
    private static final Logger logger = LogManager.getLogger(QueryGroupPersistenceService.class);
    private final ClusterService clusterService;
    private static final String SOURCE = "query-group-persistence-service";
    private static final String CREATE_QUERY_GROUP_THROTTLING_KEY = "create-query-group";
    private static final String UPDATE_QUERY_GROUP_THROTTLING_KEY = "update-query-group";
    private static final String DELETE_QUERY_GROUP_THROTTLING_KEY = "delete-query-group";
    private final AtomicInteger inflightCreateQueryGroupRequestCount;
    private final Map<String, DoubleAdder> inflightResourceLimitValues;
    private volatile int maxQueryGroupCount;
    final ThrottlingKey createQueryGroupThrottlingKey;
    final ThrottlingKey updateQueryGroupThrottlingKey;
    final ThrottlingKey deleteQueryGroupThrottlingKey;

    /**
     * Constructor for QueryGroupPersistenceService
     *
     * @param clusterService {@link ClusterService} - The cluster service to be used by QueryGroupPersistenceService
     * @param settings {@link Settings} - The settings to be used by QueryGroupPersistenceService
     * @param clusterSettings {@link ClusterSettings} - The cluster settings to be used by QueryGroupPersistenceService
     */
    @Inject
    public QueryGroupPersistenceService(
        final ClusterService clusterService,
        final Settings settings,
        final ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        this.createQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(CREATE_QUERY_GROUP_THROTTLING_KEY, true);
        this.deleteQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(DELETE_QUERY_GROUP_THROTTLING_KEY, true);
        this.updateQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(UPDATE_QUERY_GROUP_THROTTLING_KEY, true);
        maxQueryGroupCount = MAX_QUERY_GROUP_COUNT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxQueryGroupCount);
        inflightCreateQueryGroupRequestCount = new AtomicInteger();
        inflightResourceLimitValues = new HashMap<>();
    }

    /**
     * Set maxQueryGroupCount to be newMaxQueryGroupCount
     * @param newMaxQueryGroupCount - the max number of QueryGroup allowed
     */
    public void setMaxQueryGroupCount(int newMaxQueryGroupCount) {
        if (newMaxQueryGroupCount < 0) {
            throw new IllegalArgumentException("node.query_group.max_count can't be negative");
        }
        this.maxQueryGroupCount = newMaxQueryGroupCount;
    }

    @Override
    public void update(UpdateQueryGroupRequest updateQueryGroupRequest, ActionListener<UpdateQueryGroupResponse> listener) {
        updateInClusterStateMetadata(updateQueryGroupRequest, listener);
    }

    /**
     * Get the allocation value for resourceName for the QueryGroup
     * @param resourceName - the resourceName we want to get the usage for
     * @param resourceLimits  - the resource limit from which to get the allocation value for resourceName
     */
    private double getResourceLimitValue(String resourceName, final Map<ResourceType, Object> resourceLimits) {
        for (ResourceType resourceType : resourceLimits.keySet()) {
            if (resourceType.getName().equals(resourceName)) {
                return (double) resourceLimits.get(resourceType);
            }
        }
        return 0.0;
    }

    /**
     * This method restores the inflight values to be before the QueryGroup is processed
     * @param resourceLimits - the resourceLimits we're currently restoring
     */
    void restoreInflightValues(Map<ResourceType, Object> resourceLimits) {
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            return;
        }
        for (ResourceType resourceType : resourceLimits.keySet()) {
            String currResourceName = resourceType.getName();
            inflightResourceLimitValues.get(currResourceName).add(-getResourceLimitValue(currResourceName, resourceLimits));
        }
    }

    /**
     * This method calculates the existing total usage of the resource (except the group that we're updating here)
     * @param resourceName - the resource name we're calculating
     * @param groupsMap - existing QueryGroups
     * @param groupName - the QueryGroup name we're updating
     */
    private double calculateExistingUsage(String resourceName, Map<String, QueryGroup> groupsMap, String groupName) {
        double existingUsage = 0;
        for (String currGroupId : groupsMap.keySet()) {
            QueryGroup currGroup = groupsMap.get(currGroupId);
            if (!currGroup.getName().equals(groupName)) {
                existingUsage += getResourceLimitValue(resourceName, currGroup.getResourceLimits());
            }
        }
        return existingUsage;
    }

    /**
     * Modify cluster state to update the QueryGroup
     * @param toUpdateGroup {@link QueryGroup} - the QueryGroup that we want to update
     */
    void updateInClusterStateMetadata(UpdateQueryGroupRequest toUpdateGroup, ActionListener<UpdateQueryGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateQueryGroupInClusterState(toUpdateGroup, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return updateQueryGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                restoreInflightValues(toUpdateGroup.getResourceLimits());
                logger.warn("Failed to update QueryGroup due to error: {}, for source: {}", e.getMessage(), source);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                restoreInflightValues(toUpdateGroup.getResourceLimits());
                String name = toUpdateGroup.getName();
                Optional<QueryGroup> findUpdatedGroup = newState.metadata()
                    .queryGroups()
                    .values()
                    .stream()
                    .filter(group -> group.getName().equals(name))
                    .findFirst();
                assert findUpdatedGroup.isPresent();
                QueryGroup updatedGroup = findUpdatedGroup.get();
                UpdateQueryGroupResponse response = new UpdateQueryGroupResponse(updatedGroup, RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * Modify cluster state to update the existing QueryGroup
     * @param updateQueryGroupRequest {@link QueryGroup} - the QueryGroup that we want to update
     * @param currentState - current cluster state
     */
    public ClusterState updateQueryGroupInClusterState(UpdateQueryGroupRequest updateQueryGroupRequest, ClusterState currentState) {
        final Metadata metadata = currentState.metadata();
        Map<String, QueryGroup> existingGroups = currentState.metadata().queryGroups();
        String name = updateQueryGroupRequest.getName();
        String resourceNameWithThresholdExceeded = "";

        // check if there's any resource allocation that exceed limit of 1.0
        if (updateQueryGroupRequest.getResourceLimits() != null) {
            for (ResourceType resourceType : updateQueryGroupRequest.getResourceLimits().keySet()) {
                String resourceName = resourceType.getName();
                double existingUsage = calculateExistingUsage(resourceName, existingGroups, name);
                double newGroupUsage = getResourceLimitValue(resourceName, updateQueryGroupRequest.getResourceLimits());
                inflightResourceLimitValues.computeIfAbsent(resourceName, k -> new DoubleAdder()).add(newGroupUsage);
                double totalUsage = existingUsage + inflightResourceLimitValues.get(resourceName).doubleValue();
                if (totalUsage > 1) {
                    resourceNameWithThresholdExceeded = resourceName;
                }
            }
        }

        Optional<QueryGroup> findExistingGroup = existingGroups.values().stream().filter(group -> group.getName().equals(name)).findFirst();

        if (findExistingGroup.isEmpty()) {
            logger.warn("No QueryGroup exists with the provided name: {}", name);
            throw new RuntimeException("No QueryGroup exists with the provided name: " + name);
        }
        if (!resourceNameWithThresholdExceeded.isEmpty()) {
            logger.error("Total resource allocation for {} will go above the max limit of 1.0", resourceNameWithThresholdExceeded);
            throw new RuntimeException(
                "Total resource allocation for " + resourceNameWithThresholdExceeded + " will go above the max limit of 1.0"
            );
        }

        // build the QueryGroup with updated fields
        QueryGroup existingGroup = findExistingGroup.get();
        String _id = existingGroup.get_id();
        long updatedAtInMillis = updateQueryGroupRequest.getUpdatedAtInMillis();
        Map<ResourceType, Object> existingResourceLimits = existingGroup.getResourceLimits();
        Map<ResourceType, Object> updatedResourceLimits = new HashMap<>();
        if (existingResourceLimits != null) {
            updatedResourceLimits.putAll(existingResourceLimits);
        }
        if (updateQueryGroupRequest.getResourceLimits() != null) {
            updatedResourceLimits.putAll(updateQueryGroupRequest.getResourceLimits());
        }
        ResiliencyMode mode = updateQueryGroupRequest.getResiliencyMode() == null
            ? existingGroup.getResiliencyMode()
            : updateQueryGroupRequest.getResiliencyMode();

        QueryGroup updatedGroup = new QueryGroup(name, _id, mode, updatedResourceLimits, updatedAtInMillis);
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(metadata).remove(existingGroup).put(updatedGroup).build())
            .build();
    }

    /**
     * inflightCreateQueryGroupRequestCount getter
     */
    public AtomicInteger getInflightCreateQueryGroupRequestCount() {
        return inflightCreateQueryGroupRequestCount;
    }

    /**
     * inflightResourceLimitValues getter
     */
    public Map<String, DoubleAdder> getInflightResourceLimitValues() {
        return inflightResourceLimitValues;
    }

    /**
     * clusterService getter
     */
    public ClusterService getClusterService() {
        return clusterService;
    }
}

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
import org.opensearch.cluster.service.ClusterManagerTaskThrottler.ThrottlingKey;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.action.CreateQueryGroupResponse;
import org.opensearch.search.ResourceType;

import java.util.Map;
import java.util.Optional;

import static org.opensearch.search.query_group.QueryGroupServiceSettings.MAX_QUERY_GROUP_COUNT;
import static org.opensearch.search.query_group.QueryGroupServiceSettings.QUERY_GROUP_COUNT_SETTING_NAME;

/**
 * This class defines the functions for QueryGroup persistence
 */
public class QueryGroupPersistenceService {
    private static final Logger logger = LogManager.getLogger(QueryGroupPersistenceService.class);
    private final ClusterService clusterService;
    private static final String SOURCE = "query-group-persistence-service";
    private static final String CREATE_QUERY_GROUP_THROTTLING_KEY = "create-query-group";
    private static final String UPDATE_QUERY_GROUP_THROTTLING_KEY = "update-query-group";
    private static final String DELETE_QUERY_GROUP_THROTTLING_KEY = "delete-query-group";
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

    /**
     * Update cluster state to include the new QueryGroup
     * @param queryGroup {@link QueryGroup} - the QueryGroup we're currently creating
     * @param listener - ActionListener for CreateQueryGroupResponse
     */
    public void persistInClusterStateMetadata(QueryGroup queryGroup, ActionListener<CreateQueryGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return saveQueryGroupInClusterState(queryGroup, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return createQueryGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to save QueryGroup object due to error: {}, for source: {}", e.getMessage(), source);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                CreateQueryGroupResponse response = new CreateQueryGroupResponse(queryGroup, RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * This method will be executed before we submit the new cluster state
     * @param queryGroup - the QueryGroup we're currently creating
     * @param currentClusterState - the cluster state before the update
     */
    ClusterState saveQueryGroupInClusterState(final QueryGroup queryGroup, final ClusterState currentClusterState) {
        final Metadata metadata = currentClusterState.metadata();
        String groupName = queryGroup.getName();
        final Map<String, QueryGroup> previousGroups = metadata.queryGroups();

        // check if maxQueryGroupCount will breach
        if (previousGroups.size() + 1 > maxQueryGroupCount) {
            logger.error("{} value exceeded its assigned limit of {}", QUERY_GROUP_COUNT_SETTING_NAME, maxQueryGroupCount);
            throw new RuntimeException("Can't create more than " + maxQueryGroupCount + " QueryGroups in the system");
        }

        // check for duplicate name
        Optional<QueryGroup> findExistingGroup = previousGroups.values()
            .stream()
            .filter(group -> group.getName().equals(groupName))
            .findFirst();
        if (findExistingGroup.isPresent()) {
            logger.warn("QueryGroup with name {} already exists. Not creating a new one.", groupName);
            throw new RuntimeException("QueryGroup with name " + groupName + " already exists. Not creating a new one.");
        }

        // check if there's any resource allocation that exceed limit of 1.0
        for (ResourceType resourceType : queryGroup.getResourceLimits().keySet()) {
            String resourceName = resourceType.getName();
            double existingUsage = calculateExistingUsage(resourceName, previousGroups, groupName);
            double newGroupUsage = getResourceLimitValue(resourceName, queryGroup.getResourceLimits());
            if (existingUsage + newGroupUsage > 1) {
                logger.error("Total resource allocation for {} will go above the max limit of 1.0", resourceName);
                throw new RuntimeException("Total resource allocation for " + resourceName + " will go above the max limit of 1.0");
            }
        }

        Metadata newData = Metadata.builder(metadata).put(queryGroup).build();
        return ClusterState.builder(currentClusterState).metadata(newData).build();
    }

    /**
     * Get the allocation value for resourceName from the resourceLimits of a QueryGroup
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
}

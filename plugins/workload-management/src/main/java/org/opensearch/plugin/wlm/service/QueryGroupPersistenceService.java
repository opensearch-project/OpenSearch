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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.action.CreateQueryGroupResponse;
import org.opensearch.search.ResourceType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class defines the functions for QueryGroup persistence
 */
public class QueryGroupPersistenceService {
    public static final String QUERY_GROUP_COUNT_SETTING_NAME = "node.query_group.max_count";
    private static final String SOURCE = "query-group-persistence-service";
    private static final String CREATE_QUERY_GROUP_THROTTLING_KEY = "create-query-group";
    public static final int DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE = 100;
    public static final Setting<Integer> MAX_QUERY_GROUP_COUNT = Setting.intSetting(
        QUERY_GROUP_COUNT_SETTING_NAME,
        DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE,
        0,
        (newVal) -> {
            if (newVal > 100 || newVal < 1) throw new IllegalArgumentException(
                QUERY_GROUP_COUNT_SETTING_NAME + " should be in range [1-100]"
            );
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(QueryGroupPersistenceService.class);
    private final ClusterService clusterService;
    private volatile int maxQueryGroupCount;
    final ThrottlingKey createQueryGroupThrottlingKey;

    /**
     * Constructor for QueryGroupPersistenceService
     *
     * @param clusterService {@link ClusterService} - The cluster service to be used by QueryGroupPersistenceService
     * @param settings {@link Settings} - The settings to be used by QueryGroupPersistenceService
     */
    @Inject
    public QueryGroupPersistenceService(
        final ClusterService clusterService,
        final Settings settings,
        final ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        this.createQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(CREATE_QUERY_GROUP_THROTTLING_KEY, true);
        setMaxQueryGroupCount(MAX_QUERY_GROUP_COUNT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxQueryGroupCount);
    }

    /**
     * Set maxQueryGroupCount to be newMaxQueryGroupCount
     * @param newMaxQueryGroupCount - the max number of QueryGroup allowed
     */
    public void setMaxQueryGroupCount(int newMaxQueryGroupCount) {
        if (newMaxQueryGroupCount > 100 || newMaxQueryGroupCount < 1) {
            throw new IllegalArgumentException(QUERY_GROUP_COUNT_SETTING_NAME + " should be in range [1-100]");
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
        final Map<String, QueryGroup> existingQueryGroups = currentClusterState.metadata().queryGroups();
        String groupName = queryGroup.getName();

        // check if maxQueryGroupCount will breach
        if (existingQueryGroups.size() + 1 > maxQueryGroupCount) {
            logger.warn("{} value exceeded its assigned limit of {}", QUERY_GROUP_COUNT_SETTING_NAME, maxQueryGroupCount);
            throw new RuntimeException("Can't create more than " + maxQueryGroupCount + " QueryGroups in the system");
        }

        // check for duplicate name
        Optional<QueryGroup> findExistingGroup = existingQueryGroups.values()
            .stream()
            .filter(group -> group.getName().equals(groupName))
            .findFirst();
        if (findExistingGroup.isPresent()) {
            logger.warn("QueryGroup with name {} already exists. Not creating a new one.", groupName);
            throw new RuntimeException("QueryGroup with name " + groupName + " already exists. Not creating a new one.");
        }

        // check if there's any resource allocation that exceed limit of 1.0
        Map<ResourceType, Double> existingUsageMap = calculateExistingUsage(existingQueryGroups, groupName);
        for (ResourceType resourceType : queryGroup.getResourceLimits().keySet()) {
            double totalUsage = existingUsageMap.getOrDefault(resourceType, 0.0) + queryGroup.getResourceLimits().get(resourceType);
            if (totalUsage > 1) {
                logger.warn("Total resource allocation for {} will go above the max limit of 1.0", resourceType.getName());
                throw new RuntimeException(
                    "Total resource allocation for " + resourceType.getName() + " will go above the max limit of 1.0"
                );
            }
        }

        return ClusterState.builder(currentClusterState)
            .metadata(
                Metadata.builder(currentClusterState.metadata())
                    .put(queryGroup)
                    .build()
            )
            .build();
    }

    /**
     * This method calculates the existing total usage of the all the resource limits (except the group that we're updating here)
     * @param existingQueryGroups - existing QueryGroups
     * @param groupName - the QueryGroup name we're updating
     */
    private Map<ResourceType, Double> calculateExistingUsage(Map<String, QueryGroup> existingQueryGroups, String groupName) {
        Map<ResourceType, Double> map = new HashMap<>();
        for (QueryGroup currGroup : existingQueryGroups.values()) {
            if (!currGroup.getName().equals(groupName)) {
                for (ResourceType resourceType : currGroup.getResourceLimits().keySet()) {
                    map.put(resourceType, map.getOrDefault(resourceType, 0.0) + currGroup.getResourceLimits().get(resourceType));
                }
            }
        }
        return map;
    }
}

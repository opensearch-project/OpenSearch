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
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.metadata.QueryGroup.ResiliencyMode;
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
import org.opensearch.plugin.wlm.action.DeleteQueryGroupRequest;

import org.opensearch.wlm.ResourceType;
import org.opensearch.plugin.wlm.UpdateQueryGroupRequest;
import org.opensearch.plugin.wlm.UpdateQueryGroupResponse;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.HashMap;

/**
 * This class defines the functions for QueryGroup persistence
 */
public class QueryGroupPersistenceService {
    static final String SOURCE = "query-group-persistence-service";
    private static final String CREATE_QUERY_GROUP_THROTTLING_KEY = "create-query-group";
    private static final String DELETE_QUERY_GROUP_THROTTLING_KEY = "delete-query-group";
    private static final String UPDATE_QUERY_GROUP_THROTTLING_KEY = "update-query-group";
    private static final Logger logger = LogManager.getLogger(QueryGroupPersistenceService.class);
    /**
     *  max QueryGroup count setting name
     */
    public static final String QUERY_GROUP_COUNT_SETTING_NAME = "node.query_group.max_count";
    /**
     * default max queryGroup count on any node at any given point in time
     */
    private static final int DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE = 100;
    /**
     * min queryGroup count on any node at any given point in time
     */
    private static final int MIN_QUERY_GROUP_COUNT_VALUE = 1;
    /**
     *  max QueryGroup count setting
     */
    public static final Setting<Integer> MAX_QUERY_GROUP_COUNT = Setting.intSetting(
        QUERY_GROUP_COUNT_SETTING_NAME,
        DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE,
        0,
        QueryGroupPersistenceService::validateMaxQueryGroupCount,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private final ClusterService clusterService;
    private volatile int maxQueryGroupCount;
    final ThrottlingKey createQueryGroupThrottlingKey;
    final ThrottlingKey deleteQueryGroupThrottlingKey;
    final ThrottlingKey updateQueryGroupThrottlingKey;

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
        setMaxQueryGroupCount(MAX_QUERY_GROUP_COUNT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxQueryGroupCount);
    }

    /**
     * Set maxQueryGroupCount to be newMaxQueryGroupCount
     * @param newMaxQueryGroupCount - the max number of QueryGroup allowed
     */
    public void setMaxQueryGroupCount(int newMaxQueryGroupCount) {
        validateMaxQueryGroupCount(newMaxQueryGroupCount);
        this.maxQueryGroupCount = newMaxQueryGroupCount;
    }

    /**
     * Validator for maxQueryGroupCount
     * @param maxQueryGroupCount - the maxQueryGroupCount number to be verified
     */
    private static void validateMaxQueryGroupCount(int maxQueryGroupCount) {
        if (maxQueryGroupCount > DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE || maxQueryGroupCount < MIN_QUERY_GROUP_COUNT_VALUE) {
            throw new IllegalArgumentException(QUERY_GROUP_COUNT_SETTING_NAME + " should be in range [1-100].");
        }
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
                logger.warn("failed to save QueryGroup object due to error: {}, for source: {}.", e.getMessage(), source);
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
        if (existingQueryGroups.size() == maxQueryGroupCount) {
            logger.warn("{} value exceeded its assigned limit of {}.", QUERY_GROUP_COUNT_SETTING_NAME, maxQueryGroupCount);
            throw new IllegalStateException("Can't create more than " + maxQueryGroupCount + " QueryGroups in the system.");
        }

        // check for duplicate name
        Optional<QueryGroup> findExistingGroup = existingQueryGroups.values()
            .stream()
            .filter(group -> group.getName().equals(groupName))
            .findFirst();
        if (findExistingGroup.isPresent()) {
            logger.warn("QueryGroup with name {} already exists. Not creating a new one.", groupName);
            throw new IllegalArgumentException("QueryGroup with name " + groupName + " already exists. Not creating a new one.");
        }

        // check if there's any resource allocation that exceed limit of 1.0
        validateTotalUsage(existingQueryGroups, groupName, queryGroup.getResourceLimits());

        return ClusterState.builder(currentClusterState)
            .metadata(Metadata.builder(currentClusterState.metadata()).put(queryGroup).build())
            .build();
    }

    /**
     * Get the QueryGroups with the specified name from cluster state
     * @param name - the QueryGroup name we are getting
     * @param currentState - current cluster state
     */
    public static Collection<QueryGroup> getFromClusterStateMetadata(String name, ClusterState currentState) {
        final Map<String, QueryGroup> currentGroups = currentState.getMetadata().queryGroups();
        if (name == null || name.isEmpty()) {
            return currentGroups.values();
        }
        return currentGroups.values()
            .stream()
            .filter(group -> group.getName().equals(name))
            .findAny()
            .stream()
            .collect(Collectors.toList());
    }

    /**
     * Modify cluster state to delete the QueryGroup
     * @param deleteQueryGroupRequest - request to delete a QueryGroup
     * @param listener - ActionListener for AcknowledgedResponse
     */
    public void deleteInClusterStateMetadata(
        DeleteQueryGroupRequest deleteQueryGroupRequest,
        ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask(SOURCE, new AckedClusterStateUpdateTask<>(deleteQueryGroupRequest, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return deleteQueryGroupInClusterState(deleteQueryGroupRequest.getName(), currentState);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return deleteQueryGroupThrottlingKey;
            }

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }
        });
    }

    /**
     * Modify cluster state to delete the QueryGroup, and return the new cluster state
     * @param name - the name for QueryGroup to be deleted
     * @param currentClusterState - current cluster state
     */
    ClusterState deleteQueryGroupInClusterState(final String name, final ClusterState currentClusterState) {
        final Metadata metadata = currentClusterState.metadata();
        final QueryGroup queryGroupToRemove = metadata.queryGroups()
            .values()
            .stream()
            .filter(queryGroup -> queryGroup.getName().equals(name))
            .findAny()
            .orElseThrow(() -> new ResourceNotFoundException("No QueryGroup exists with the provided name: " + name));

        return ClusterState.builder(currentClusterState).metadata(Metadata.builder(metadata).remove(queryGroupToRemove).build()).build();
    }

    /**
     * Modify cluster state to update the QueryGroup
     * @param toUpdateGroup {@link QueryGroup} - the QueryGroup that we want to update
     * @param listener - ActionListener for UpdateQueryGroupResponse
     */
    public void updateInClusterStateMetadata(UpdateQueryGroupRequest toUpdateGroup, ActionListener<UpdateQueryGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.NORMAL) {
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
                logger.warn("Failed to update QueryGroup due to error: {}, for source: {}", e.getMessage(), source);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
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
    ClusterState updateQueryGroupInClusterState(UpdateQueryGroupRequest updateQueryGroupRequest, ClusterState currentState) {
        final Metadata metadata = currentState.metadata();
        final Map<String, QueryGroup> existingGroups = currentState.metadata().queryGroups();
        String name = updateQueryGroupRequest.getName();

        final QueryGroup existingGroup = existingGroups.values()
            .stream()
            .filter(group -> group.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new ResourceNotFoundException("No QueryGroup exists with the provided name: " + name));

        // build the QueryGroup with updated fields
        final Map<ResourceType, Double> updatedResourceLimits = new HashMap<>(existingGroup.getResourceLimits());
        if (updateQueryGroupRequest.getResourceLimits() != null && !updateQueryGroupRequest.getResourceLimits().isEmpty()) {
            validateTotalUsage(existingGroups, name, updateQueryGroupRequest.getResourceLimits());
            updatedResourceLimits.putAll(updateQueryGroupRequest.getResourceLimits());
        }

        final ResiliencyMode mode = Optional.ofNullable(updateQueryGroupRequest.getResiliencyMode())
            .orElse(existingGroup.getResiliencyMode());

        final QueryGroup updatedGroup = new QueryGroup(
            name,
            existingGroup.get_id(),
            mode,
            updatedResourceLimits,
            updateQueryGroupRequest.getUpdatedAtInMillis()
        );
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(metadata).remove(existingGroup).put(updatedGroup).build())
            .build();
    }

    /**
     * This method checks if there's any resource allocation that exceed limit of 1.0
     * @param existingQueryGroups - existing QueryGroups in the system
     * @param resourceLimits - the QueryGroup we're creating or updating
     */
    private void validateTotalUsage(Map<String, QueryGroup> existingQueryGroups, String name, Map<ResourceType, Double> resourceLimits) {
        final Map<ResourceType, Double> totalUsage = new EnumMap<>(ResourceType.class);
        totalUsage.putAll(resourceLimits);
        for (QueryGroup currGroup : existingQueryGroups.values()) {
            if (!currGroup.getName().equals(name)) {
                for (ResourceType resourceType : resourceLimits.keySet()) {
                    totalUsage.compute(resourceType, (k, v) -> v + currGroup.getResourceLimits().getOrDefault(resourceType, 0.0));
                }
            }
        }
        totalUsage.forEach((resourceType, total) -> {
            if (total > 1.0) {
                logger.warn("Total resource allocation for {} will go above the max limit of 1.0.", resourceType.getName());
                throw new IllegalArgumentException(
                    "Total resource allocation for " + resourceType.getName() + " will go above the max limit of 1.0."
                );
            }
        });
    }

    /**
     * maxQueryGroupCount getter
     */
    public int getMaxQueryGroupCount() {
        return maxQueryGroupCount;
    }

    /**
     * clusterService getter
     */
    public ClusterService getClusterService() {
        return clusterService;
    }
}

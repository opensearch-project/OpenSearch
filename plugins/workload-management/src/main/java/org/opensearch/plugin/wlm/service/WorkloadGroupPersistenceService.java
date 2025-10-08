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
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler.ThrottlingKey;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupResponse;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupResponse;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.WorkloadGroup.updateExistingWorkloadGroup;
import static org.opensearch.cluster.service.ClusterManagerTask.CREATE_QUERY_GROUP;
import static org.opensearch.cluster.service.ClusterManagerTask.DELETE_QUERY_GROUP;
import static org.opensearch.cluster.service.ClusterManagerTask.UPDATE_QUERY_GROUP;

/**
 * This class defines the functions for WorkloadGroup persistence
 */
public class WorkloadGroupPersistenceService {
    static final String SOURCE = "query-group-persistence-service";
    private static final Logger logger = LogManager.getLogger(WorkloadGroupPersistenceService.class);
    /**
     *  max WorkloadGroup count setting name
     */
    public static final String WORKLOAD_GROUP_COUNT_SETTING_NAME = "node.workload_group.max_count";
    /**
     * default max workloadGroup count on any node at any given point in time
     */
    private static final int DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE = 100;
    /**
     * min workloadGroup count on any node at any given point in time
     */
    private static final int MIN_QUERY_GROUP_COUNT_VALUE = 1;
    /**
     *  max WorkloadGroup count setting
     */
    public static final Setting<Integer> MAX_QUERY_GROUP_COUNT = Setting.intSetting(
        WORKLOAD_GROUP_COUNT_SETTING_NAME,
        DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE,
        0,
        WorkloadGroupPersistenceService::validateMaxWorkloadGroupCount,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private final ClusterService clusterService;
    private volatile int maxWorkloadGroupCount;
    final ThrottlingKey createWorkloadGroupThrottlingKey;
    final ThrottlingKey deleteWorkloadGroupThrottlingKey;
    final ThrottlingKey updateWorkloadGroupThrottlingKey;

    /**
     * Constructor for WorkloadGroupPersistenceService
     *
     * @param clusterService {@link ClusterService} - The cluster service to be used by WorkloadGroupPersistenceService
     * @param settings {@link Settings} - The settings to be used by WorkloadGroupPersistenceService
     * @param clusterSettings {@link ClusterSettings} - The cluster settings to be used by WorkloadGroupPersistenceService
     */
    @Inject
    public WorkloadGroupPersistenceService(
        final ClusterService clusterService,
        final Settings settings,
        final ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        this.createWorkloadGroupThrottlingKey = clusterService.registerClusterManagerTask(CREATE_QUERY_GROUP, true);
        this.deleteWorkloadGroupThrottlingKey = clusterService.registerClusterManagerTask(DELETE_QUERY_GROUP, true);
        this.updateWorkloadGroupThrottlingKey = clusterService.registerClusterManagerTask(UPDATE_QUERY_GROUP, true);
        setMaxWorkloadGroupCount(MAX_QUERY_GROUP_COUNT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxWorkloadGroupCount);
    }

    /**
     * Set maxWorkloadGroupCount to be newMaxWorkloadGroupCount
     * @param newMaxWorkloadGroupCount - the max number of WorkloadGroup allowed
     */
    public void setMaxWorkloadGroupCount(int newMaxWorkloadGroupCount) {
        validateMaxWorkloadGroupCount(newMaxWorkloadGroupCount);
        this.maxWorkloadGroupCount = newMaxWorkloadGroupCount;
    }

    /**
     * Validator for maxWorkloadGroupCount
     * @param maxWorkloadGroupCount - the maxWorkloadGroupCount number to be verified
     */
    private static void validateMaxWorkloadGroupCount(int maxWorkloadGroupCount) {
        if (maxWorkloadGroupCount > DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE || maxWorkloadGroupCount < MIN_QUERY_GROUP_COUNT_VALUE) {
            throw new IllegalArgumentException(WORKLOAD_GROUP_COUNT_SETTING_NAME + " should be in range [1-100].");
        }
    }

    /**
     * Update cluster state to include the new WorkloadGroup
     * @param workloadGroup {@link WorkloadGroup} - the WorkloadGroup we're currently creating
     * @param listener - ActionListener for CreateWorkloadGroupResponse
     */
    public void persistInClusterStateMetadata(WorkloadGroup workloadGroup, ActionListener<CreateWorkloadGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return saveWorkloadGroupInClusterState(workloadGroup, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return createWorkloadGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to save WorkloadGroup object due to error: {}, for source: {}.", e.getMessage(), source);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                CreateWorkloadGroupResponse response = new CreateWorkloadGroupResponse(workloadGroup, RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * This method will be executed before we submit the new cluster state
     * @param workloadGroup - the WorkloadGroup we're currently creating
     * @param currentClusterState - the cluster state before the update
     */
    ClusterState saveWorkloadGroupInClusterState(final WorkloadGroup workloadGroup, final ClusterState currentClusterState) {
        final Map<String, WorkloadGroup> existingWorkloadGroups = currentClusterState.metadata().workloadGroups();
        String groupName = workloadGroup.getName();

        // check if maxWorkloadGroupCount will breach
        if (existingWorkloadGroups.size() == maxWorkloadGroupCount) {
            logger.warn("{} value exceeded its assigned limit of {}.", WORKLOAD_GROUP_COUNT_SETTING_NAME, maxWorkloadGroupCount);
            throw new IllegalStateException("Can't create more than " + maxWorkloadGroupCount + " WorkloadGroups in the system.");
        }

        // check for duplicate name
        Optional<WorkloadGroup> findExistingGroup = existingWorkloadGroups.values()
            .stream()
            .filter(group -> group.getName().equals(groupName))
            .findFirst();
        if (findExistingGroup.isPresent()) {
            logger.warn("WorkloadGroup with name {} already exists. Not creating a new one.", groupName);
            throw new IllegalArgumentException("WorkloadGroup with name " + groupName + " already exists. Not creating a new one.");
        }

        // check if there's any resource allocation that exceed limit of 1.0
        validateTotalUsage(existingWorkloadGroups, groupName, workloadGroup.getResourceLimits());

        return ClusterState.builder(currentClusterState)
            .metadata(Metadata.builder(currentClusterState.metadata()).put(workloadGroup).build())
            .build();
    }

    /**
     * Get the WorkloadGroups with the specified name from cluster state
     * @param name - the WorkloadGroup name we are getting
     * @param currentState - current cluster state
     */
    public static Collection<WorkloadGroup> getFromClusterStateMetadata(String name, ClusterState currentState) {
        final Map<String, WorkloadGroup> currentGroups = currentState.getMetadata().workloadGroups();
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
     * Modify cluster state to delete the WorkloadGroup
     * @param deleteWorkloadGroupRequest - request to delete a WorkloadGroup
     * @param listener - ActionListener for AcknowledgedResponse
     */
    public void deleteInClusterStateMetadata(
        DeleteWorkloadGroupRequest deleteWorkloadGroupRequest,
        ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask(SOURCE, new AckedClusterStateUpdateTask<>(deleteWorkloadGroupRequest, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return deleteWorkloadGroupInClusterState(deleteWorkloadGroupRequest.getName(), currentState);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return deleteWorkloadGroupThrottlingKey;
            }

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }
        });
    }

    /**
     * Modify cluster state to delete the WorkloadGroup, and return the new cluster state
     * @param name - the name for WorkloadGroup to be deleted
     * @param currentClusterState - current cluster state
     */
    ClusterState deleteWorkloadGroupInClusterState(final String name, final ClusterState currentClusterState) {
        final Metadata metadata = currentClusterState.metadata();
        final WorkloadGroup workloadGroupToRemove = getWorkloadGroup(name, metadata);

        return ClusterState.builder(currentClusterState).metadata(Metadata.builder(metadata).remove(workloadGroupToRemove).build()).build();
    }

    private static WorkloadGroup getWorkloadGroup(String name, Metadata metadata) {
        return metadata.workloadGroups()
            .values()
            .stream()
            .filter(workloadGroup -> workloadGroup.getName().equals(name))
            .findAny()
            .orElseThrow(() -> new ResourceNotFoundException("No WorkloadGroup exists with the provided name: " + name));
    }

    /**
     * Modify cluster state to update the WorkloadGroup
     * @param toUpdateGroup {@link WorkloadGroup} - the WorkloadGroup that we want to update
     * @param listener - ActionListener for UpdateWorkloadGroupResponse
     */
    public void updateInClusterStateMetadata(
        UpdateWorkloadGroupRequest toUpdateGroup,
        ActionListener<UpdateWorkloadGroupResponse> listener
    ) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateWorkloadGroupInClusterState(toUpdateGroup, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return updateWorkloadGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to update WorkloadGroup due to error: {}, for source: {}", e.getMessage(), source);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                String name = toUpdateGroup.getName();
                Optional<WorkloadGroup> findUpdatedGroup = newState.metadata()
                    .workloadGroups()
                    .values()
                    .stream()
                    .filter(group -> group.getName().equals(name))
                    .findFirst();
                assert findUpdatedGroup.isPresent();
                WorkloadGroup updatedGroup = findUpdatedGroup.get();
                UpdateWorkloadGroupResponse response = new UpdateWorkloadGroupResponse(updatedGroup, RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * Modify cluster state to update the existing WorkloadGroup
     * @param updateWorkloadGroupRequest {@link WorkloadGroup} - the WorkloadGroup that we want to update
     * @param currentState - current cluster state
     */
    ClusterState updateWorkloadGroupInClusterState(UpdateWorkloadGroupRequest updateWorkloadGroupRequest, ClusterState currentState) {
        final Metadata metadata = currentState.metadata();
        final Map<String, WorkloadGroup> existingGroups = currentState.metadata().workloadGroups();
        String name = updateWorkloadGroupRequest.getName();
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment = updateWorkloadGroupRequest.getmMutableWorkloadGroupFragment();

        final WorkloadGroup existingGroup = existingGroups.values()
            .stream()
            .filter(group -> group.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new ResourceNotFoundException("No WorkloadGroup exists with the provided name: " + name));

        validateTotalUsage(existingGroups, name, mutableWorkloadGroupFragment.getResourceLimits());
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(metadata)
                    .remove(existingGroup)
                    .put(updateExistingWorkloadGroup(existingGroup, mutableWorkloadGroupFragment))
                    .build()
            )
            .build();
    }

    /**
     * This method checks if there's any resource allocation that exceed limit of 1.0
     * @param existingWorkloadGroups - existing WorkloadGroups in the system
     * @param resourceLimits - the WorkloadGroup we're creating or updating
     */
    private void validateTotalUsage(
        Map<String, WorkloadGroup> existingWorkloadGroups,
        String name,
        Map<ResourceType, Double> resourceLimits
    ) {
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            return;
        }
        final Map<ResourceType, Double> totalUsage = new EnumMap<>(ResourceType.class);
        totalUsage.putAll(resourceLimits);
        for (WorkloadGroup currGroup : existingWorkloadGroups.values()) {
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
     * maxWorkloadGroupCount getter
     */
    public int getMaxWorkloadGroupCount() {
        return maxWorkloadGroupCount;
    }

    /**
     * clusterService getter
     */
    public ClusterService getClusterService() {
        return clusterService;
    }
}

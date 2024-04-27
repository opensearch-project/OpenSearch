/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.metadata.ResourceLimitGroup.ResourceLimit;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler.ThrottlingKey;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.resource_limit_group.CreateResourceLimitGroupResponse;
import org.opensearch.plugin.resource_limit_group.DeleteResourceLimitGroupResponse;
import org.opensearch.plugin.resource_limit_group.GetResourceLimitGroupResponse;
import org.opensearch.plugin.resource_limit_group.UpdateResourceLimitGroupResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;

import static org.opensearch.search.resource_limit_group.ResourceLimitGroupServiceSettings.MAX_RESOURCE_LIMIT_GROUP_COUNT;
import static org.opensearch.search.resource_limit_group.ResourceLimitGroupServiceSettings.RESOURCE_LIMIT_GROUP_COUNT_SETTING_NAME;

/**
 * This class defines the functions for Resource Limit Group persistence
 */
public class ResourceLimitGroupPersistenceService implements Persistable<ResourceLimitGroup> {
    private static final Logger logger = LogManager.getLogger(ResourceLimitGroupPersistenceService.class);
    private final ClusterService clusterService;
    private static final String SOURCE = "resource-limit-group-persistence-service";
    private static final String CREATE_RESOURCE_LIMIT_GROUP_THROTTLING_KEY = "create-resource-limit-group";
    private static final String UPDATE_RESOURCE_LIMIT_GROUP_THROTTLING_KEY = "update-resource-limit-group";
    private static final String DELETE_RESOURCE_LIMIT_GROUP_THROTTLING_KEY = "delete-resource-limit-group";
    private static AtomicInteger inflightCreateResourceLimitGroupRequestCount;
    Map<String, DoubleAdder> inflightResourceLimitValues;
    private volatile int maxResourceLimitGroupCount;
    final ThrottlingKey createResourceLimitGroupThrottlingKey;
    final ThrottlingKey updateResourceLimitGroupThrottlingKey;
    final ThrottlingKey deleteResourceLimitGroupThrottlingKey;

    /**
     * Constructor for Resource Limit GroupPersistenceService
     *
     * @param clusterService {@link ClusterService} - The cluster service to be used by ResourceLimitGroupPersistenceService
     * @param settings {@link Settings} - The settings to be used by ResourceLimitGroupPersistenceService
     * @param clusterSettings {@link ClusterSettings} - The cluster settings to be used by ResourceLimitGroupPersistenceService
     */
    @Inject
    public ResourceLimitGroupPersistenceService(
        final ClusterService clusterService,
        final Settings settings,
        final ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        this.createResourceLimitGroupThrottlingKey = clusterService.registerClusterManagerTask(
            CREATE_RESOURCE_LIMIT_GROUP_THROTTLING_KEY,
            true
        );
        this.deleteResourceLimitGroupThrottlingKey = clusterService.registerClusterManagerTask(
            DELETE_RESOURCE_LIMIT_GROUP_THROTTLING_KEY,
            true
        );
        this.updateResourceLimitGroupThrottlingKey = clusterService.registerClusterManagerTask(
            UPDATE_RESOURCE_LIMIT_GROUP_THROTTLING_KEY,
            true
        );
        maxResourceLimitGroupCount = MAX_RESOURCE_LIMIT_GROUP_COUNT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_RESOURCE_LIMIT_GROUP_COUNT, this::setMaxResourceLimitGroupCount);
        inflightCreateResourceLimitGroupRequestCount = new AtomicInteger();
        inflightResourceLimitValues = new HashMap<>();
    }

    /**
     * Set maxResourceLimitGroupCount to be newMaxResourceLimitGroupCount
     *
     * @param newMaxResourceLimitGroupCount - the max number of resource limit group allowed
     */
    public void setMaxResourceLimitGroupCount(int newMaxResourceLimitGroupCount) {
        if (newMaxResourceLimitGroupCount < 0) {
            throw new IllegalArgumentException("node.resource_limit_group.max_count can't be negative");
        }
        this.maxResourceLimitGroupCount = newMaxResourceLimitGroupCount;
    }

    @Override
    public void persist(ResourceLimitGroup resourceLimitGroup, ActionListener<CreateResourceLimitGroupResponse> listener) {
        persistInClusterStateMetadata(resourceLimitGroup, listener);
    }

    @Override
    public void update(ResourceLimitGroup resourceLimitGroup, ActionListener<UpdateResourceLimitGroupResponse> listener) {
        ClusterState currentState = clusterService.state();
        Map<String, ResourceLimitGroup> currentGroupsMap = currentState.metadata().resourceLimitGroups();
        List<ResourceLimitGroup> currentGroupsList = new ArrayList<>(currentGroupsMap.values());
        String name = resourceLimitGroup.getName();

        if (!currentGroupsMap.containsKey(name)) {
            logger.warn("No Resource Limit Group exists with the provided name: {}", name);
            Exception e = new RuntimeException("No Resource Limit Group exists with the provided name: " + name);
            UpdateResourceLimitGroupResponse response = new UpdateResourceLimitGroupResponse();
            response.setRestStatus(RestStatus.NOT_FOUND);
            listener.onFailure(e);
            return;
        }

        // check if there's any resource allocation that exceed limit of 1.0
        if (resourceLimitGroup.getResourceLimits() != null) {
            String resourceNameWithThresholdExceeded = "";
            for (ResourceLimit resourceLimit : resourceLimitGroup.getResourceLimits()) {
                String resourceName = resourceLimit.getResourceName();
                double existingUsage = calculateExistingUsage(resourceName, currentGroupsList, name);
                double newGroupUsage = getResourceLimitValue(resourceName, resourceLimitGroup);
                inflightResourceLimitValues.computeIfAbsent(resourceName, k -> new DoubleAdder()).add(newGroupUsage);
                double totalUsage = existingUsage + inflightResourceLimitValues.get(resourceName).doubleValue();
                if (totalUsage > 1) {
                    resourceNameWithThresholdExceeded = resourceName;
                }
            }
            if (!resourceNameWithThresholdExceeded.isEmpty()) {
                restoreInflightValues(resourceLimitGroup);
                Exception e = new RuntimeException(
                    "Total resource allocation for " + resourceNameWithThresholdExceeded + " will go above the max limit of 1.0"
                );
                UpdateResourceLimitGroupResponse response = new UpdateResourceLimitGroupResponse();
                response.setRestStatus(RestStatus.CONFLICT);
                listener.onFailure(e);
                return;
            }
        }

        // build the resource limit group with updated fields
        ResourceLimitGroup existingGroup = currentGroupsMap.get(name);
        String uuid = existingGroup.getUUID();
        String createdAt = existingGroup.getCreatedAt();
        String updatedAt = resourceLimitGroup.getUpdatedAt();
        List<ResourceLimit> resourceLimit;
        if (resourceLimitGroup.getResourceLimits() == null || resourceLimitGroup.getResourceLimits().isEmpty()) {
            resourceLimit = existingGroup.getResourceLimits();
        } else {
            resourceLimit = new ArrayList<>(existingGroup.getResourceLimits());
            Map<String, Double> resourceLimitMap = resourceLimitGroup.getResourceLimits()
                .stream()
                .collect(Collectors.toMap(ResourceLimit::getResourceName, ResourceLimit::getValue));
            for (ResourceLimit rl : resourceLimit) {
                String currResourceName = rl.getResourceName();
                if (resourceLimitMap.containsKey(currResourceName)) {
                    rl.setValue(resourceLimitMap.get(currResourceName));
                }
            }
        }
        String enforcement = resourceLimitGroup.getEnforcement() == null
            ? existingGroup.getEnforcement()
            : resourceLimitGroup.getEnforcement();

        ResourceLimitGroup updatedGroup = new ResourceLimitGroup(name, uuid, resourceLimit, enforcement, createdAt, updatedAt);
        updateInClusterStateMetadata(existingGroup, resourceLimitGroup, updatedGroup, listener);
    }

    @Override
    public void get(String name, ActionListener<GetResourceLimitGroupResponse> listener) {
        ClusterState currentState = clusterService.state();
        List<ResourceLimitGroup> resultGroups = getFromClusterStateMetadata(name, currentState);
        if (resultGroups.isEmpty() && name != null && !name.isEmpty()) {
            logger.warn("No Resource Limit Group exists with the provided name: {}", name);
            Exception e = new RuntimeException("No Resource Limit Group exists with the provided name: " + name);
            GetResourceLimitGroupResponse response = new GetResourceLimitGroupResponse();
            response.setRestStatus(RestStatus.NOT_FOUND);
            listener.onFailure(e);
            return;
        }
        GetResourceLimitGroupResponse response = new GetResourceLimitGroupResponse(resultGroups);
        response.setRestStatus(RestStatus.OK);
        listener.onResponse(response);
    }

    @Override
    public void delete(String name, ActionListener<DeleteResourceLimitGroupResponse> listener) {
        deleteInClusterStateMetadata(name, listener);
    }

    /**
     * Update cluster state to include the new Resource Limit Group
     * @param resourceLimitGroup {@link ResourceLimitGroup} - the resource limit group we're currently creating
     */
    void persistInClusterStateMetadata(ResourceLimitGroup resourceLimitGroup, ActionListener<CreateResourceLimitGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return saveResourceLimitGroupInClusterState(resourceLimitGroup, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return createResourceLimitGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to save Resource Limit Group object due to error: {}, for source: {}", e.getMessage(), source);
                CreateResourceLimitGroupResponse response = new CreateResourceLimitGroupResponse();
                response.setRestStatus(RestStatus.FAILED_DEPENDENCY);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                CreateResourceLimitGroupResponse response = new CreateResourceLimitGroupResponse(resourceLimitGroup);
                response.setRestStatus(RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * Get the JVM allocation value for the Resource Limit Group
     * @param resourceLimitGroup {@link ResourceLimitGroup} - the resource limit group from which to get the JVM allocation value
     */
    private double getResourceLimitValue(String resourceName, final ResourceLimitGroup resourceLimitGroup) {
        double value = 0.0;
        List<ResourceLimit> resourceLimits = resourceLimitGroup.getResourceLimits();
        for (ResourceLimit resourceLimit : resourceLimits) {
            if (resourceLimit.getResourceName().equals(resourceName)) {
                value = resourceLimit.getValue();
                break;
            }
        }
        return value;
    }

    /**
     * This method will be executed before we submit the new cluster state
     * @param resourceLimitGroup - the resource limit group we're currently creating
     * @param currentClusterState - the cluster state before the update
     */
    ClusterState saveResourceLimitGroupInClusterState(final ResourceLimitGroup resourceLimitGroup, final ClusterState currentClusterState) {
        final Metadata metadata = currentClusterState.metadata();
        String groupName = resourceLimitGroup.getName();
        if (metadata.resourceLimitGroups().containsKey(groupName)) {
            logger.warn("Resource Limit Group with name {} already exists. Not creating a new one.", groupName);
            throw new RuntimeException("Resource Limit Group with name " + groupName + " already exists. Not creating a new one.");
        }
        final List<ResourceLimitGroup> previousGroups = new ArrayList<>(metadata.resourceLimitGroups().values());

        // check if there's any resource allocation that exceed limit of 1.0
        String resourceNameWithThresholdExceeded = "";
        for (ResourceLimit resourceLimit : resourceLimitGroup.getResourceLimits()) {
            String resourceName = resourceLimit.getResourceName();
            double existingUsage = calculateExistingUsage(resourceName, previousGroups, groupName);
            double newGroupUsage = getResourceLimitValue(resourceName, resourceLimitGroup);
            inflightResourceLimitValues.computeIfAbsent(resourceName, k -> new DoubleAdder()).add(newGroupUsage);
            double totalUsage = existingUsage + inflightResourceLimitValues.get(resourceName).doubleValue();
            if (totalUsage > 1) {
                resourceNameWithThresholdExceeded = resourceName;
            }
        }
        restoreInflightValues(resourceLimitGroup);
        if (!resourceNameWithThresholdExceeded.isEmpty()) {
            logger.error("Total resource allocation for {} will go above the max limit of 1.0", resourceNameWithThresholdExceeded);
            throw new RuntimeException(
                "Total resource allocation for " + resourceNameWithThresholdExceeded + " will go above the max limit of 1.0"
            );
        }

        // check if group count exceed max
        boolean groupCountExceeded = inflightCreateResourceLimitGroupRequestCount.incrementAndGet() + previousGroups
            .size() > maxResourceLimitGroupCount;
        inflightCreateResourceLimitGroupRequestCount.decrementAndGet();
        if (groupCountExceeded) {
            logger.error("{} value exceeded its assigned limit of {}", RESOURCE_LIMIT_GROUP_COUNT_SETTING_NAME, maxResourceLimitGroupCount);
            throw new RuntimeException("Can't create more than " + maxResourceLimitGroupCount + " Resource Limit Groups in the system");
        }

        return ClusterState.builder(currentClusterState).metadata(Metadata.builder(metadata).put(resourceLimitGroup).build()).build();
    }

    /**
     * This method restores the inflight values to be before the resource limit group is processed
     * @param resourceLimitGroup - the resource limit group we're currently creating
     */
    void restoreInflightValues(ResourceLimitGroup resourceLimitGroup) {
        for (ResourceLimit rl : resourceLimitGroup.getResourceLimits()) {
            String currResourceName = rl.getResourceName();
            inflightResourceLimitValues.get(currResourceName).add(-getResourceLimitValue(currResourceName, resourceLimitGroup));
        }
    }

    /**
     * This method calculates the existing total usage of the resource (except the group that we're updating here)
     * @param resourceName - the resource name we're calculating
     * @param groupsList - existing resource limit groups
     * @param groupName - the resource limit group name we're updating
     */
    private double calculateExistingUsage(String resourceName, List<ResourceLimitGroup> groupsList, String groupName) {
        double existingUsage = 0;
        for (ResourceLimitGroup group : groupsList) {
            if (!group.getName().equals(groupName)) {
                existingUsage += getResourceLimitValue(resourceName, group);
            }
        }
        return existingUsage;
    }

    /**
     * Modify cluster state to update the Resource Limit Group
     * @param existingGroup {@link ResourceLimitGroup} - the existing resource limit group that we want to update
     * @param updatedGroup {@link ResourceLimitGroup} - the resource limit group we're updating to
     */
    void updateInClusterStateMetadata(
        ResourceLimitGroup existingGroup,
        ResourceLimitGroup toUpdateGroup,
        ResourceLimitGroup updatedGroup,
        ActionListener<UpdateResourceLimitGroupResponse> listener
    ) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateResourceLimitGroupInClusterState(existingGroup, updatedGroup, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return updateResourceLimitGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to update Resource Limit Group due to error: {}, for source: {}", e.getMessage(), source);
                UpdateResourceLimitGroupResponse response = new UpdateResourceLimitGroupResponse();
                response.setRestStatus(RestStatus.FAILED_DEPENDENCY);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (toUpdateGroup.getResourceLimits() != null) {
                    for (ResourceLimit rl : toUpdateGroup.getResourceLimits()) {
                        String currResourceName = rl.getResourceName();
                        inflightResourceLimitValues.get(currResourceName).add(-getResourceLimitValue(currResourceName, toUpdateGroup));
                    }
                }
                UpdateResourceLimitGroupResponse response = new UpdateResourceLimitGroupResponse(updatedGroup);
                response.setRestStatus(RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * Modify cluster state to update the existing Resource Limit Group
     * @param existingGroup {@link ResourceLimitGroup} - the existing resource limit group that we want to update
     * @param updatedGroup {@link ResourceLimitGroup} - the resource limit group we're updating to
     * @param currentState - current cluster state
     */
    public ClusterState updateResourceLimitGroupInClusterState(
        ResourceLimitGroup existingGroup,
        ResourceLimitGroup updatedGroup,
        ClusterState currentState
    ) {
        final Metadata metadata = currentState.metadata();
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(metadata).removeResourceLimitGroup(existingGroup.getName()).put(updatedGroup).build())
            .build();
    }

    /**
     * Modify cluster state to delete the Resource Limit Group
     * @param name - the name for resource limit group to be deleted
     */
    void deleteInClusterStateMetadata(String name, ActionListener<DeleteResourceLimitGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return deleteResourceLimitGroupInClusterState(name, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return deleteResourceLimitGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to delete Resource Limit Group due to error: {}, for source: {}", e.getMessage(), source);
                DeleteResourceLimitGroupResponse response = new DeleteResourceLimitGroupResponse();
                response.setRestStatus(RestStatus.NOT_FOUND);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                final Map<String, ResourceLimitGroup> oldGroupsMap = oldState.metadata().resourceLimitGroups();
                final Map<String, ResourceLimitGroup> newGroupssMap = newState.metadata().resourceLimitGroups();
                List<ResourceLimitGroup> deletedGroups = new ArrayList<>();
                for (String name : oldGroupsMap.keySet()) {
                    if (!newGroupssMap.containsKey(name)) {
                        deletedGroups.add(oldGroupsMap.get(name));
                    }
                }
                DeleteResourceLimitGroupResponse response = new DeleteResourceLimitGroupResponse(deletedGroups);
                response.setRestStatus(RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * Modify cluster state to delete the Resource Limit Group
     * @param name - the name for resource limit group to be deleted
     * @param currentClusterState - current cluster state
     */
    ClusterState deleteResourceLimitGroupInClusterState(final String name, final ClusterState currentClusterState) {
        final Metadata metadata = currentClusterState.metadata();
        final Map<String, ResourceLimitGroup> previousGroups = metadata.resourceLimitGroups();
        Map<String, ResourceLimitGroup> resultGroups = new HashMap<>(previousGroups);
        ;
        if (name == null || name.equals("")) {
            resultGroups = new HashMap<>();
        } else {
            boolean nameExists = previousGroups.containsKey(name);
            if (!nameExists) {
                logger.error("The Resource Limit Group with provided name {} doesn't exist", name);
                throw new RuntimeException("No Resource Limit Group exists with the provided name: " + name);
            }
            resultGroups.remove(name);
        }
        return ClusterState.builder(currentClusterState)
            .metadata(Metadata.builder(metadata).resourceLimitGroups(resultGroups).build())
            .build();
    }

    List<ResourceLimitGroup> getFromClusterStateMetadata(String name, ClusterState currentState) {
        Map<String, ResourceLimitGroup> currentGroupsMap = currentState.getMetadata().resourceLimitGroups();
        List<ResourceLimitGroup> currentGroups = new ArrayList<>(currentGroupsMap.values());
        List<ResourceLimitGroup> resultGroups = new ArrayList<>();
        if (name == null || name.equals("")) {
            resultGroups = currentGroups;
        } else if (currentGroupsMap.containsKey(name)) {
            resultGroups = List.of(currentGroupsMap.get(name));
        }
        return resultGroups;
    }
}

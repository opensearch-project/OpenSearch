/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group.service;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.MONITOR;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.SANDBOX_MAX_SETTING_NAME;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.TIMESTAMP_ONE;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.clusterState;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.compareResourceLimitGroups;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.prepareSandboxPersistenceService;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupList;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupMap;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupOne;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupPersistenceService;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupTwo;
import static org.mockito.Mockito.mock;

public class ResourceLimitGroupPersistenceServiceTests extends OpenSearchTestCase {

    public void testGetSingleResourceLimitGroup() {
        List<ResourceLimitGroup> groups = resourceLimitGroupPersistenceService.getFromClusterStateMetadata(NAME_ONE, clusterState);
        assertEquals(1, groups.size());
        ResourceLimitGroup resourceLimitGroup = groups.get(0);
        compareResourceLimitGroups(List.of(resourceLimitGroupOne), List.of(resourceLimitGroup));
    }

    public void testGetAllResourceLimitGroups() {
        List<ResourceLimitGroup> res = resourceLimitGroupPersistenceService.getFromClusterStateMetadata(null, clusterState);
        assertEquals(2, res.size());
        Set<String> currentNAME = res.stream().map(ResourceLimitGroup::getName).collect(Collectors.toSet());
        assertTrue(currentNAME.contains(NAME_ONE));
        assertTrue(currentNAME.contains(NAME_TWO));
        compareResourceLimitGroups(resourceLimitGroupList, res);
    }

    public void testGetZeroResourceLimitGroups() {
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ResourceLimitGroupPersistenceService sandboxPersistenceService = new ResourceLimitGroupPersistenceService(
            mock(ClusterService.class),
            settings,
            clusterSettings
        );
        List<ResourceLimitGroup> res = sandboxPersistenceService.getFromClusterStateMetadata(NAME_NONE_EXISTED, clusterState);
        assertEquals(0, res.size());
    }

    public void testDeleteSingleResourceLimitGroup() {
        ClusterState newClusterState = resourceLimitGroupPersistenceService.deleteResourceLimitGroupInClusterState(NAME_TWO, clusterState);
        Map<String, ResourceLimitGroup> afterDeletionGroups = newClusterState.getMetadata().resourceLimitGroups();
        assertEquals(1, afterDeletionGroups.size());
        List<ResourceLimitGroup> oldSandbox = List.of(resourceLimitGroupMap.get(NAME_ONE));
        compareResourceLimitGroups(new ArrayList<>(afterDeletionGroups.values()), oldSandbox);
    }

    public void testDeleteAllResourceLimitGroups() {
        ClusterState newClusterState = resourceLimitGroupPersistenceService.deleteResourceLimitGroupInClusterState(null, clusterState);
        Map<String, ResourceLimitGroup> sandboxes = newClusterState.getMetadata().resourceLimitGroups();
        assertEquals(0, sandboxes.size());
    }

    public void testDeleteNonExistedResourceLimitGroup() {
        assertThrows(
            RuntimeException.class,
            () -> resourceLimitGroupPersistenceService.deleteResourceLimitGroupInClusterState(NAME_NONE_EXISTED, clusterState)
        );
    }

    public void testUpdateResourceLimitGroupAllFields() {
        ResourceLimitGroup current = resourceLimitGroupOne;
        ResourceLimitGroup updated = resourceLimitGroupTwo;
        ClusterState newClusterState = resourceLimitGroupPersistenceService.updateResourceLimitGroupInClusterState(
            current,
            updated,
            clusterState
        );
        List<ResourceLimitGroup> updatedSandboxes = new ArrayList<>(newClusterState.getMetadata().resourceLimitGroups().values());
        assertEquals(1, updatedSandboxes.size());
        compareResourceLimitGroups(List.of(updated), updatedSandboxes);
    }

    public void testUpdateResourceLimitGroupResourceLimitOnly() {
        ResourceLimitGroup current = resourceLimitGroupOne;
        String updatedTime = "2024-04-28 23:02:21";
        ResourceLimitGroup updated = new ResourceLimitGroup(
            NAME_ONE,
            resourceLimitGroupOne.getUUID(),
            List.of(new ResourceLimitGroup.ResourceLimit("jvm", 0.1)),
            MONITOR,
            TIMESTAMP_ONE,
            updatedTime
        );
        ClusterState newClusterState = resourceLimitGroupPersistenceService.updateResourceLimitGroupInClusterState(
            current,
            updated,
            clusterState
        );
        Map<String, ResourceLimitGroup> updatedSandboxesMap = newClusterState.getMetadata().resourceLimitGroups();
        assertEquals(2, updatedSandboxesMap.size());
        assertTrue(updatedSandboxesMap.containsKey(NAME_ONE));
        assertTrue(updatedSandboxesMap.containsKey(NAME_TWO));
        compareResourceLimitGroups(List.of(updated), List.of(updatedSandboxesMap.get(NAME_ONE)));
    }

    public void testCreateResourceLimitGroup() {
        List<Object> setup = prepareSandboxPersistenceService(new ArrayList<>());
        ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService1 = (ResourceLimitGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ClusterState newClusterState = resourceLimitGroupPersistenceService1.saveResourceLimitGroupInClusterState(
            resourceLimitGroupOne,
            clusterState
        );
        Map<String, ResourceLimitGroup> updatedGroupsMap = newClusterState.getMetadata().resourceLimitGroups();
        assertEquals(1, updatedGroupsMap.size());
        assertTrue(updatedGroupsMap.containsKey(NAME_ONE));
        compareResourceLimitGroups(List.of(resourceLimitGroupOne), List.of(updatedGroupsMap.get(NAME_ONE)));
    }

    public void testCreateAnotherResourceLimitGroup() {
        List<Object> setup = prepareSandboxPersistenceService(List.of(resourceLimitGroupOne));
        ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService1 = (ResourceLimitGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ClusterState newClusterState = resourceLimitGroupPersistenceService1.saveResourceLimitGroupInClusterState(
            resourceLimitGroupTwo,
            clusterState
        );
        Map<String, ResourceLimitGroup> updatedGroupsMap = newClusterState.getMetadata().resourceLimitGroups();
        assertEquals(2, updatedGroupsMap.size());
        compareResourceLimitGroups(resourceLimitGroupList, new ArrayList<>(updatedGroupsMap.values()));
    }

    public void testCreateResourceLimitGroupDuplicateName() {
        List<Object> setup = prepareSandboxPersistenceService(List.of(resourceLimitGroupOne));
        ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService1 = (ResourceLimitGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ResourceLimitGroup toCreate = new ResourceLimitGroup(
            NAME_ONE,
            "W5iIqHyhgi4K1qIAAAAIHw==",
            List.of(new ResourceLimitGroup.ResourceLimit("jvm", 0.3)),
            MONITOR,
            null,
            null
        );
        assertThrows(
            RuntimeException.class,
            () -> resourceLimitGroupPersistenceService1.saveResourceLimitGroupInClusterState(toCreate, clusterState)
        );
    }

    public void testCreateResourceLimitGroupOverflowAllocation() {
        List<Object> setup = prepareSandboxPersistenceService(List.of(resourceLimitGroupTwo));
        ResourceLimitGroup toCreate = new ResourceLimitGroup(
            NAME_TWO,
            null,
            List.of(new ResourceLimitGroup.ResourceLimit("jvm", 0.5)),
            MONITOR,
            null,
            null
        );
        ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService1 = (ResourceLimitGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        assertThrows(
            RuntimeException.class,
            () -> resourceLimitGroupPersistenceService1.saveResourceLimitGroupInClusterState(toCreate, clusterState)
        );
    }

    public void testCreateResourceLimitGroupOverflowCount() {
        ResourceLimitGroup toCreate = new ResourceLimitGroup(
            NAME_NONE_EXISTED,
            null,
            List.of(new ResourceLimitGroup.ResourceLimit("jvm", 0.5)),
            MONITOR,
            null,
            null
        );
        Metadata metadata = Metadata.builder().resourceLimitGroups(resourceLimitGroupMap).build();
        Settings settings = Settings.builder().put(SANDBOX_MAX_SETTING_NAME, 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService1 = new ResourceLimitGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        assertThrows(
            RuntimeException.class,
            () -> resourceLimitGroupPersistenceService1.saveResourceLimitGroupInClusterState(toCreate, clusterState)
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.WorkloadGroupTestUtils;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupResponse;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;

import static org.opensearch.cluster.metadata.WorkloadGroup.builder;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils._ID_TWO;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.assertEqualWorkloadGroups;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.clusterSettings;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.clusterSettingsSet;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.clusterState;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.preparePersistenceServiceSetup;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.queryGroupPersistenceService;
import static org.opensearch.plugin.wlm.WorkloadGroupTestUtils.queryGroupTwo;
import static org.opensearch.plugin.wlm.action.WorkloadGroupActionTestUtils.updateWorkloadGroupRequest;
import static org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService.QUERY_GROUP_COUNT_SETTING_NAME;
import static org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService.SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WorkloadGroupPersistenceServiceTests extends OpenSearchTestCase {

    /**
     * Test case to validate the creation logic of a WorkloadGroup
     */
    public void testCreateWorkloadGroup() {
        Tuple<WorkloadGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(new HashMap<>());
        WorkloadGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        ClusterState newClusterState = queryGroupPersistenceService1.saveWorkloadGroupInClusterState(queryGroupOne, clusterState);
        Map<String, WorkloadGroup> updatedGroupsMap = newClusterState.getMetadata().queryGroups();
        assertEquals(1, updatedGroupsMap.size());
        assertTrue(updatedGroupsMap.containsKey(_ID_ONE));
        List<WorkloadGroup> listOne = new ArrayList<>();
        List<WorkloadGroup> listTwo = new ArrayList<>();
        listOne.add(queryGroupOne);
        listTwo.add(updatedGroupsMap.get(_ID_ONE));
        assertEqualWorkloadGroups(listOne, listTwo, false);
    }

    /**
     * Test case to validate the logic for adding a new WorkloadGroup to a cluster state that already contains
     * an existing WorkloadGroup
     */
    public void testCreateAnotherWorkloadGroup() {
        Tuple<WorkloadGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        WorkloadGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        ClusterState newClusterState = queryGroupPersistenceService1.saveWorkloadGroupInClusterState(queryGroupTwo, clusterState);
        Map<String, WorkloadGroup> updatedGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(2, updatedGroups.size());
        assertTrue(updatedGroups.containsKey(_ID_TWO));
        Collection<WorkloadGroup> values = updatedGroups.values();
        assertEqualWorkloadGroups(queryGroupList(), new ArrayList<>(values), false);
    }

    /**
     * Test case to ensure the error is thrown when we try to create another WorkloadGroup with duplicate name
     */
    public void testCreateWorkloadGroupDuplicateName() {
        Tuple<WorkloadGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        WorkloadGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        WorkloadGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mutableWorkloadGroupFragment(new MutableWorkloadGroupFragment(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.3)))
            .updatedAt(1690934400000L)
            .build();
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveWorkloadGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Test case to ensure the error is thrown when we try to create another WorkloadGroup that will make
     * the total resource limits go above 1
     */
    public void testCreateWorkloadGroupOverflowAllocation() {
        Tuple<WorkloadGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(Map.of(_ID_TWO, queryGroupTwo));
        WorkloadGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mutableWorkloadGroupFragment(new MutableWorkloadGroupFragment(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.41)))
            .updatedAt(1690934400000L)
            .build();

        WorkloadGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveWorkloadGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Test case to ensure the error is thrown when we already have the max allowed number of WorkloadGroups, but
     * we want to create another one
     */
    public void testCreateWorkloadGroupOverflowCount() {
        WorkloadGroup toCreate = builder().name(NAME_NONE_EXISTED)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mutableWorkloadGroupFragment(new MutableWorkloadGroupFragment(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.5)))
            .updatedAt(1690934400000L)
            .build();
        Metadata metadata = Metadata.builder().queryGroups(Map.of(_ID_ONE, queryGroupOne, _ID_TWO, queryGroupTwo)).build();
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingsSet());
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        WorkloadGroupPersistenceService queryGroupPersistenceService1 = new WorkloadGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveWorkloadGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Tests the invalid value of {@code node.workload_group.max_count}
     */
    public void testInvalidMaxWorkloadGroupCount() {
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingsSet());
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        assertThrows(IllegalArgumentException.class, () -> queryGroupPersistenceService.setMaxWorkloadGroupCount(-1));
    }

    /**
     * Tests the valid value of {@code node.workload_group.max_count}
     */
    public void testValidMaxSandboxCountSetting() {
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 100).build();
        ClusterService clusterService = new ClusterService(settings, clusterSettings(), mock(ThreadPool.class));
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings()
        );
        queryGroupPersistenceService.setMaxWorkloadGroupCount(50);
        assertEquals(50, queryGroupPersistenceService.getMaxWorkloadGroupCount());
    }

    /**
     * Tests PersistInClusterStateMetadata function
     */
    public void testPersistInClusterStateMetadata() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<CreateWorkloadGroupResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        queryGroupPersistenceService.persistInClusterStateMetadata(queryGroupOne, listener);
        verify(clusterService).submitStateUpdateTask(eq(SOURCE), any());
    }

    /**
     * Tests PersistInClusterStateMetadata function with inner functions
     */
    public void testPersistInClusterStateMetadataInner() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<CreateWorkloadGroupResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        queryGroupPersistenceService.persistInClusterStateMetadata(queryGroupOne, listener);
        verify(clusterService, times(1)).submitStateUpdateTask(eq(SOURCE), captor.capture());
        ClusterStateUpdateTask capturedTask = captor.getValue();
        assertEquals(queryGroupPersistenceService.createWorkloadGroupThrottlingKey, capturedTask.getClusterManagerThrottlingKey());

        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            task.clusterStateProcessed(SOURCE, mock(ClusterState.class), mock(ClusterState.class));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.persistInClusterStateMetadata(queryGroupOne, listener);
        verify(listener).onResponse(any(CreateWorkloadGroupResponse.class));
    }

    /**
     * Tests PersistInClusterStateMetadata function with failure
     */
    public void testPersistInClusterStateMetadataFailure() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<CreateWorkloadGroupResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            Exception exception = new RuntimeException("Test Exception");
            task.onFailure(SOURCE, exception);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.persistInClusterStateMetadata(queryGroupOne, listener);
        verify(listener).onFailure(any(RuntimeException.class));
    }

    /**
     * Tests getting a single WorkloadGroup
     */
    public void testGetSingleWorkloadGroup() {
        Collection<WorkloadGroup> groupsCollections = WorkloadGroupPersistenceService.getFromClusterStateMetadata(NAME_ONE, clusterState());
        List<WorkloadGroup> groups = new ArrayList<>(groupsCollections);
        assertEquals(1, groups.size());
        WorkloadGroup queryGroup = groups.get(0);
        List<WorkloadGroup> listOne = new ArrayList<>();
        List<WorkloadGroup> listTwo = new ArrayList<>();
        listOne.add(WorkloadGroupTestUtils.queryGroupOne);
        listTwo.add(queryGroup);
        WorkloadGroupTestUtils.assertEqualWorkloadGroups(listOne, listTwo, false);
    }

    /**
     * Tests getting all WorkloadGroups
     */
    public void testGetAllWorkloadGroups() {
        assertEquals(2, WorkloadGroupTestUtils.clusterState().metadata().queryGroups().size());
        Collection<WorkloadGroup> groupsCollections = WorkloadGroupPersistenceService.getFromClusterStateMetadata(null, clusterState());
        List<WorkloadGroup> res = new ArrayList<>(groupsCollections);
        assertEquals(2, res.size());
        Set<String> currentNAME = res.stream().map(WorkloadGroup::getName).collect(Collectors.toSet());
        assertTrue(currentNAME.contains(WorkloadGroupTestUtils.NAME_ONE));
        assertTrue(currentNAME.contains(WorkloadGroupTestUtils.NAME_TWO));
        WorkloadGroupTestUtils.assertEqualWorkloadGroups(WorkloadGroupTestUtils.queryGroupList(), res, false);
    }

    /**
     * Tests getting a WorkloadGroup with invalid name
     */
    public void testGetNonExistedWorkloadGroups() {
        Collection<WorkloadGroup> groupsCollections = WorkloadGroupPersistenceService.getFromClusterStateMetadata(
            NAME_NONE_EXISTED,
            clusterState()
        );
        List<WorkloadGroup> groups = new ArrayList<>(groupsCollections);
        assertEquals(0, groups.size());
    }

    /**
     * Tests setting maxWorkloadGroupCount
     */
    public void testMaxWorkloadGroupCount() {
        assertThrows(IllegalArgumentException.class, () -> WorkloadGroupTestUtils.queryGroupPersistenceService().setMaxWorkloadGroupCount(-1));
        WorkloadGroupPersistenceService queryGroupPersistenceService = WorkloadGroupTestUtils.queryGroupPersistenceService();
        queryGroupPersistenceService.setMaxWorkloadGroupCount(50);
        assertEquals(50, queryGroupPersistenceService.getMaxWorkloadGroupCount());
    }

    /**
     * Tests delete a single WorkloadGroup
     */
    public void testDeleteSingleWorkloadGroup() {
        ClusterState newClusterState = queryGroupPersistenceService().deleteWorkloadGroupInClusterState(NAME_TWO, clusterState());
        Map<String, WorkloadGroup> afterDeletionGroups = newClusterState.getMetadata().queryGroups();
        assertFalse(afterDeletionGroups.containsKey(_ID_TWO));
        assertEquals(1, afterDeletionGroups.size());
        List<WorkloadGroup> oldWorkloadGroups = new ArrayList<>();
        oldWorkloadGroups.add(queryGroupOne);
        assertEqualWorkloadGroups(new ArrayList<>(afterDeletionGroups.values()), oldWorkloadGroups, false);
    }

    /**
     * Tests delete a WorkloadGroup with invalid name
     */
    public void testDeleteNonExistedWorkloadGroup() {
        assertThrows(
            ResourceNotFoundException.class,
            () -> queryGroupPersistenceService().deleteWorkloadGroupInClusterState(NAME_NONE_EXISTED, clusterState())
        );
    }

    /**
     * Tests DeleteInClusterStateMetadata function
     */
    @SuppressWarnings("unchecked")
    public void testDeleteInClusterStateMetadata() throws Exception {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(NAME_ONE);
        ClusterService clusterService = mock(ClusterService.class);

        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        doAnswer(invocation -> {
            AckedClusterStateUpdateTask<?> task = invocation.getArgument(1);
            ClusterState initialState = clusterState();
            ClusterState newState = task.execute(initialState);
            assertNotNull(newState);
            assertEquals(queryGroupPersistenceService.deleteWorkloadGroupThrottlingKey, task.getClusterManagerThrottlingKey());
            task.onAllNodesAcked(null);
            verify(listener).onResponse(argThat(response -> response.isAcknowledged()));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.deleteInClusterStateMetadata(request, listener);
        verify(clusterService).submitStateUpdateTask(eq(SOURCE), any(AckedClusterStateUpdateTask.class));
    }

    /**
     * Tests updating a WorkloadGroup with all fields
     */
    public void testUpdateWorkloadGroupAllFields() {
        WorkloadGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .mutableWorkloadGroupFragment(new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.15)))
            .updatedAt(1690934400000L)
            .build();
        UpdateWorkloadGroupRequest updateWorkloadGroupRequest = updateWorkloadGroupRequest(NAME_ONE, updated.getMutableWorkloadGroupFragment());
        ClusterState newClusterState = queryGroupPersistenceService().updateWorkloadGroupInClusterState(
            updateWorkloadGroupRequest,
            clusterState()
        );
        List<WorkloadGroup> updatedWorkloadGroups = new ArrayList<>(newClusterState.getMetadata().queryGroups().values());
        assertEquals(2, updatedWorkloadGroups.size());
        List<WorkloadGroup> expectedList = new ArrayList<>();
        expectedList.add(queryGroupTwo);
        expectedList.add(updated);
        assertEqualWorkloadGroups(expectedList, updatedWorkloadGroups, true);
    }

    /**
     * Tests updating a WorkloadGroup with only updated resourceLimits
     */
    public void testUpdateWorkloadGroupResourceLimitsOnly() {
        WorkloadGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .mutableWorkloadGroupFragment(new MutableWorkloadGroupFragment(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.15)))
            .updatedAt(1690934400000L)
            .build();
        UpdateWorkloadGroupRequest updateWorkloadGroupRequest = updateWorkloadGroupRequest(NAME_ONE, updated.getMutableWorkloadGroupFragment());
        ClusterState newClusterState = queryGroupPersistenceService().updateWorkloadGroupInClusterState(
            updateWorkloadGroupRequest,
            clusterState()
        );
        List<WorkloadGroup> updatedWorkloadGroups = new ArrayList<>(newClusterState.getMetadata().queryGroups().values());
        assertEquals(2, updatedWorkloadGroups.size());
        Optional<WorkloadGroup> findUpdatedGroupOne = newClusterState.metadata()
            .queryGroups()
            .values()
            .stream()
            .filter(group -> group.getName().equals(NAME_ONE))
            .findFirst();
        Optional<WorkloadGroup> findUpdatedGroupTwo = newClusterState.metadata()
            .queryGroups()
            .values()
            .stream()
            .filter(group -> group.getName().equals(NAME_TWO))
            .findFirst();
        assertTrue(findUpdatedGroupOne.isPresent());
        assertTrue(findUpdatedGroupTwo.isPresent());
        List<WorkloadGroup> list1 = new ArrayList<>();
        list1.add(updated);
        List<WorkloadGroup> list2 = new ArrayList<>();
        list2.add(findUpdatedGroupOne.get());
        assertEqualWorkloadGroups(list1, list2, true);
    }

    /**
     * Tests updating a WorkloadGroup with invalid name
     */
    public void testUpdateWorkloadGroupNonExistedName() {
        WorkloadGroupPersistenceService queryGroupPersistenceService = queryGroupPersistenceService();
        UpdateWorkloadGroupRequest updateWorkloadGroupRequest = updateWorkloadGroupRequest(
            NAME_NONE_EXISTED,
            new MutableWorkloadGroupFragment(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.15))
        );
        assertThrows(
            RuntimeException.class,
            () -> queryGroupPersistenceService.updateWorkloadGroupInClusterState(updateWorkloadGroupRequest, clusterState())
        );
        List<WorkloadGroup> updatedWorkloadGroups = new ArrayList<>(
            queryGroupPersistenceService.getClusterService().state().metadata().queryGroups().values()
        );
        assertEquals(2, updatedWorkloadGroups.size());
        List<WorkloadGroup> expectedList = new ArrayList<>();
        expectedList.add(queryGroupTwo);
        expectedList.add(queryGroupOne);
        assertEqualWorkloadGroups(expectedList, updatedWorkloadGroups, true);
    }

    /**
     * Tests UpdateInClusterStateMetadata function
     */
    public void testUpdateInClusterStateMetadata() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<UpdateWorkloadGroupResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        queryGroupPersistenceService.updateInClusterStateMetadata(null, listener);
        verify(clusterService).submitStateUpdateTask(eq(SOURCE), any());
    }

    /**
     * Tests UpdateInClusterStateMetadata function with inner functions
     */
    public void testUpdateInClusterStateMetadataInner() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<UpdateWorkloadGroupResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        UpdateWorkloadGroupRequest updateWorkloadGroupRequest = updateWorkloadGroupRequest(
            NAME_TWO,
            new MutableWorkloadGroupFragment(ResiliencyMode.SOFT, new HashMap<>())
        );
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        queryGroupPersistenceService.updateInClusterStateMetadata(updateWorkloadGroupRequest, listener);
        verify(clusterService, times(1)).submitStateUpdateTask(eq(SOURCE), captor.capture());
        ClusterStateUpdateTask capturedTask = captor.getValue();
        assertEquals(queryGroupPersistenceService.updateWorkloadGroupThrottlingKey, capturedTask.getClusterManagerThrottlingKey());

        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            task.clusterStateProcessed(SOURCE, clusterState(), clusterState());
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.updateInClusterStateMetadata(updateWorkloadGroupRequest, listener);
        verify(listener).onResponse(any(UpdateWorkloadGroupResponse.class));
    }

    /**
     * Tests UpdateInClusterStateMetadata function with failure
     */
    public void testUpdateInClusterStateMetadataFailure() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<UpdateWorkloadGroupResponse> listener = mock(ActionListener.class);
        WorkloadGroupPersistenceService queryGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            WorkloadGroupTestUtils.settings(),
            clusterSettings()
        );
        UpdateWorkloadGroupRequest updateWorkloadGroupRequest = updateWorkloadGroupRequest(
            NAME_TWO,
            new MutableWorkloadGroupFragment(ResiliencyMode.SOFT, new HashMap<>())
        );
        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            Exception exception = new RuntimeException("Test Exception");
            task.onFailure(SOURCE, exception);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.updateInClusterStateMetadata(updateWorkloadGroupRequest, listener);
        verify(listener).onFailure(any(RuntimeException.class));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.QueryGroupTestUtils;
import org.opensearch.plugin.wlm.action.CreateQueryGroupResponse;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateQueryGroupRequest;
import org.opensearch.plugin.wlm.action.UpdateQueryGroupResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.ChangeableQueryGroup;
import org.opensearch.wlm.ChangeableQueryGroup.ResiliencyMode;
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

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertEqualQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterSettings;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterSettingsSet;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterState;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.preparePersistenceServiceSetup;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupPersistenceService;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupTwo;
import static org.opensearch.plugin.wlm.service.QueryGroupPersistenceService.QUERY_GROUP_COUNT_SETTING_NAME;
import static org.opensearch.plugin.wlm.service.QueryGroupPersistenceService.SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

    /**
     * Test case to validate the creation logic of a QueryGroup
     */
    public void testCreateQueryGroup() {
        Tuple<QueryGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(new HashMap<>());
        QueryGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        ClusterState newClusterState = queryGroupPersistenceService1.saveQueryGroupInClusterState(queryGroupOne, clusterState);
        Map<String, QueryGroup> updatedGroupsMap = newClusterState.getMetadata().queryGroups();
        assertEquals(1, updatedGroupsMap.size());
        assertTrue(updatedGroupsMap.containsKey(_ID_ONE));
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(queryGroupOne);
        listTwo.add(updatedGroupsMap.get(_ID_ONE));
        assertEqualQueryGroups(listOne, listTwo, false);
    }

    /**
     * Test case to validate the logic for adding a new QueryGroup to a cluster state that already contains
     * an existing QueryGroup
     */
    public void testCreateAnotherQueryGroup() {
        Tuple<QueryGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        ClusterState newClusterState = queryGroupPersistenceService1.saveQueryGroupInClusterState(queryGroupTwo, clusterState);
        Map<String, QueryGroup> updatedGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(2, updatedGroups.size());
        assertTrue(updatedGroups.containsKey(_ID_TWO));
        Collection<QueryGroup> values = updatedGroups.values();
        assertEqualQueryGroups(queryGroupList(), new ArrayList<>(values), false);
    }

    /**
     * Test case to ensure the error is thrown when we try to create another QueryGroup with duplicate name
     */
    public void testCreateQueryGroupDuplicateName() {
        Tuple<QueryGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        QueryGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .changeableQueryGroup(new ChangeableQueryGroup(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.3)))
            .updatedAt(1690934400000L)
            .build();
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Test case to ensure the error is thrown when we try to create another QueryGroup that will make
     * the total resource limits go above 1
     */
    public void testCreateQueryGroupOverflowAllocation() {
        Tuple<QueryGroupPersistenceService, ClusterState> setup = preparePersistenceServiceSetup(Map.of(_ID_TWO, queryGroupTwo));
        QueryGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .changeableQueryGroup(new ChangeableQueryGroup(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.41)))
            .updatedAt(1690934400000L)
            .build();

        QueryGroupPersistenceService queryGroupPersistenceService1 = setup.v1();
        ClusterState clusterState = setup.v2();
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Test case to ensure the error is thrown when we already have the max allowed number of QueryGroups, but
     * we want to create another one
     */
    public void testCreateQueryGroupOverflowCount() {
        QueryGroup toCreate = builder().name(NAME_NONE_EXISTED)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .changeableQueryGroup(new ChangeableQueryGroup(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.5)))
            .updatedAt(1690934400000L)
            .build();
        Metadata metadata = Metadata.builder().queryGroups(Map.of(_ID_ONE, queryGroupOne, _ID_TWO, queryGroupTwo)).build();
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingsSet());
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        QueryGroupPersistenceService queryGroupPersistenceService1 = new QueryGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Tests the invalid value of {@code node.query_group.max_count}
     */
    public void testInvalidMaxQueryGroupCount() {
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingsSet());
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        assertThrows(IllegalArgumentException.class, () -> queryGroupPersistenceService.setMaxQueryGroupCount(-1));
    }

    /**
     * Tests the valid value of {@code node.query_group.max_count}
     */
    public void testValidMaxSandboxCountSetting() {
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 100).build();
        ClusterService clusterService = new ClusterService(settings, clusterSettings(), mock(ThreadPool.class));
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings()
        );
        queryGroupPersistenceService.setMaxQueryGroupCount(50);
        assertEquals(50, queryGroupPersistenceService.getMaxQueryGroupCount());
    }

    /**
     * Tests PersistInClusterStateMetadata function
     */
    public void testPersistInClusterStateMetadata() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<CreateQueryGroupResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
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
        ActionListener<CreateQueryGroupResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
            clusterSettings()
        );
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        queryGroupPersistenceService.persistInClusterStateMetadata(queryGroupOne, listener);
        verify(clusterService, times(1)).submitStateUpdateTask(eq(SOURCE), captor.capture());
        ClusterStateUpdateTask capturedTask = captor.getValue();
        assertEquals(queryGroupPersistenceService.createQueryGroupThrottlingKey, capturedTask.getClusterManagerThrottlingKey());

        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            task.clusterStateProcessed(SOURCE, mock(ClusterState.class), mock(ClusterState.class));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.persistInClusterStateMetadata(queryGroupOne, listener);
        verify(listener).onResponse(any(CreateQueryGroupResponse.class));
    }

    /**
     * Tests PersistInClusterStateMetadata function with failure
     */
    public void testPersistInClusterStateMetadataFailure() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<CreateQueryGroupResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
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
     * Tests getting a single QueryGroup
     */
    public void testGetSingleQueryGroup() {
        Collection<QueryGroup> groupsCollections = QueryGroupPersistenceService.getFromClusterStateMetadata(NAME_ONE, clusterState());
        List<QueryGroup> groups = new ArrayList<>(groupsCollections);
        assertEquals(1, groups.size());
        QueryGroup queryGroup = groups.get(0);
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(QueryGroupTestUtils.queryGroupOne);
        listTwo.add(queryGroup);
        QueryGroupTestUtils.assertEqualQueryGroups(listOne, listTwo, false);
    }

    /**
     * Tests getting all QueryGroups
     */
    public void testGetAllQueryGroups() {
        assertEquals(2, QueryGroupTestUtils.clusterState().metadata().queryGroups().size());
        Collection<QueryGroup> groupsCollections = QueryGroupPersistenceService.getFromClusterStateMetadata(null, clusterState());
        List<QueryGroup> res = new ArrayList<>(groupsCollections);
        assertEquals(2, res.size());
        Set<String> currentNAME = res.stream().map(QueryGroup::getName).collect(Collectors.toSet());
        assertTrue(currentNAME.contains(QueryGroupTestUtils.NAME_ONE));
        assertTrue(currentNAME.contains(QueryGroupTestUtils.NAME_TWO));
        QueryGroupTestUtils.assertEqualQueryGroups(QueryGroupTestUtils.queryGroupList(), res, false);
    }

    /**
     * Tests getting a QueryGroup with invalid name
     */
    public void testGetNonExistedQueryGroups() {
        Collection<QueryGroup> groupsCollections = QueryGroupPersistenceService.getFromClusterStateMetadata(
            NAME_NONE_EXISTED,
            clusterState()
        );
        List<QueryGroup> groups = new ArrayList<>(groupsCollections);
        assertEquals(0, groups.size());
    }

    /**
     * Tests setting maxQueryGroupCount
     */
    public void testMaxQueryGroupCount() {
        assertThrows(IllegalArgumentException.class, () -> QueryGroupTestUtils.queryGroupPersistenceService().setMaxQueryGroupCount(-1));
        QueryGroupPersistenceService queryGroupPersistenceService = QueryGroupTestUtils.queryGroupPersistenceService();
        queryGroupPersistenceService.setMaxQueryGroupCount(50);
        assertEquals(50, queryGroupPersistenceService.getMaxQueryGroupCount());
    }

    /**
     * Tests delete a single QueryGroup
     */
    public void testDeleteSingleQueryGroup() {
        ClusterState newClusterState = queryGroupPersistenceService().deleteQueryGroupInClusterState(NAME_TWO, clusterState());
        Map<String, QueryGroup> afterDeletionGroups = newClusterState.getMetadata().queryGroups();
        assertFalse(afterDeletionGroups.containsKey(_ID_TWO));
        assertEquals(1, afterDeletionGroups.size());
        List<QueryGroup> oldQueryGroups = new ArrayList<>();
        oldQueryGroups.add(queryGroupOne);
        assertEqualQueryGroups(new ArrayList<>(afterDeletionGroups.values()), oldQueryGroups, false);
    }

    /**
     * Tests delete a QueryGroup with invalid name
     */
    public void testDeleteNonExistedQueryGroup() {
        assertThrows(
            ResourceNotFoundException.class,
            () -> queryGroupPersistenceService().deleteQueryGroupInClusterState(NAME_NONE_EXISTED, clusterState())
        );
    }

    /**
     * Tests DeleteInClusterStateMetadata function
     */
    @SuppressWarnings("unchecked")
    public void testDeleteInClusterStateMetadata() throws Exception {
        DeleteQueryGroupRequest request = new DeleteQueryGroupRequest(NAME_ONE);
        ClusterService clusterService = mock(ClusterService.class);

        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
            clusterSettings()
        );
        doAnswer(invocation -> {
            AckedClusterStateUpdateTask<?> task = invocation.getArgument(1);
            ClusterState initialState = clusterState();
            ClusterState newState = task.execute(initialState);
            assertNotNull(newState);
            assertEquals(queryGroupPersistenceService.deleteQueryGroupThrottlingKey, task.getClusterManagerThrottlingKey());
            task.onAllNodesAcked(null);
            verify(listener).onResponse(argThat(response -> response.isAcknowledged()));
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.deleteInClusterStateMetadata(request, listener);
        verify(clusterService).submitStateUpdateTask(eq(SOURCE), any(AckedClusterStateUpdateTask.class));
    }

    /**
     * Tests updating a QueryGroup with all fields
     */
    public void testUpdateQueryGroupAllFields() {
        QueryGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .changeableQueryGroup(new ChangeableQueryGroup(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.15)))
            .updatedAt(1690934400000L)
            .build();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(NAME_ONE, updated.getChangeableQueryGroup());
        ClusterState newClusterState = queryGroupPersistenceService().updateQueryGroupInClusterState(
            updateQueryGroupRequest,
            clusterState()
        );
        List<QueryGroup> updatedQueryGroups = new ArrayList<>(newClusterState.getMetadata().queryGroups().values());
        assertEquals(2, updatedQueryGroups.size());
        List<QueryGroup> expectedList = new ArrayList<>();
        expectedList.add(queryGroupTwo);
        expectedList.add(updated);
        assertEqualQueryGroups(expectedList, updatedQueryGroups, true);
    }

    /**
     * Tests updating a QueryGroup with only updated resourceLimits
     */
    public void testUpdateQueryGroupResourceLimitsOnly() {
        QueryGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .changeableQueryGroup(new ChangeableQueryGroup(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.15)))
            .updatedAt(1690934400000L)
            .build();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(NAME_ONE, updated.getChangeableQueryGroup());
        ClusterState newClusterState = queryGroupPersistenceService().updateQueryGroupInClusterState(
            updateQueryGroupRequest,
            clusterState()
        );
        List<QueryGroup> updatedQueryGroups = new ArrayList<>(newClusterState.getMetadata().queryGroups().values());
        assertEquals(2, updatedQueryGroups.size());
        Optional<QueryGroup> findUpdatedGroupOne = newClusterState.metadata()
            .queryGroups()
            .values()
            .stream()
            .filter(group -> group.getName().equals(NAME_ONE))
            .findFirst();
        Optional<QueryGroup> findUpdatedGroupTwo = newClusterState.metadata()
            .queryGroups()
            .values()
            .stream()
            .filter(group -> group.getName().equals(NAME_TWO))
            .findFirst();
        assertTrue(findUpdatedGroupOne.isPresent());
        assertTrue(findUpdatedGroupTwo.isPresent());
        List<QueryGroup> list1 = new ArrayList<>();
        list1.add(updated);
        List<QueryGroup> list2 = new ArrayList<>();
        list2.add(findUpdatedGroupOne.get());
        assertEqualQueryGroups(list1, list2, true);
    }

    /**
     * Tests updating a QueryGroup with invalid name
     */
    public void testUpdateQueryGroupNonExistedName() {
        QueryGroupPersistenceService queryGroupPersistenceService = queryGroupPersistenceService();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_NONE_EXISTED,
            new ChangeableQueryGroup(ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.15))
        );
        assertThrows(
            RuntimeException.class,
            () -> queryGroupPersistenceService.updateQueryGroupInClusterState(updateQueryGroupRequest, clusterState())
        );
        List<QueryGroup> updatedQueryGroups = new ArrayList<>(
            queryGroupPersistenceService.getClusterService().state().metadata().queryGroups().values()
        );
        assertEquals(2, updatedQueryGroups.size());
        List<QueryGroup> expectedList = new ArrayList<>();
        expectedList.add(queryGroupTwo);
        expectedList.add(queryGroupOne);
        assertEqualQueryGroups(expectedList, updatedQueryGroups, true);
    }

    /**
     * Tests UpdateInClusterStateMetadata function
     */
    public void testUpdateInClusterStateMetadata() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryGroupResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
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
        ActionListener<UpdateQueryGroupResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
            clusterSettings()
        );
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_TWO,
            new ChangeableQueryGroup(ResiliencyMode.SOFT, new HashMap<>())
        );
        ArgumentCaptor<ClusterStateUpdateTask> captor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        queryGroupPersistenceService.updateInClusterStateMetadata(updateQueryGroupRequest, listener);
        verify(clusterService, times(1)).submitStateUpdateTask(eq(SOURCE), captor.capture());
        ClusterStateUpdateTask capturedTask = captor.getValue();
        assertEquals(queryGroupPersistenceService.updateQueryGroupThrottlingKey, capturedTask.getClusterManagerThrottlingKey());

        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            task.clusterStateProcessed(SOURCE, clusterState(), clusterState());
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.updateInClusterStateMetadata(updateQueryGroupRequest, listener);
        verify(listener).onResponse(any(UpdateQueryGroupResponse.class));
    }

    /**
     * Tests UpdateInClusterStateMetadata function with failure
     */
    public void testUpdateInClusterStateMetadataFailure() {
        ClusterService clusterService = mock(ClusterService.class);
        @SuppressWarnings("unchecked")
        ActionListener<UpdateQueryGroupResponse> listener = mock(ActionListener.class);
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            QueryGroupTestUtils.settings(),
            clusterSettings()
        );
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_TWO,
            new ChangeableQueryGroup(ResiliencyMode.SOFT, new HashMap<>())
        );
        doAnswer(invocation -> {
            ClusterStateUpdateTask task = invocation.getArgument(1);
            Exception exception = new RuntimeException("Test Exception");
            task.onFailure(SOURCE, exception);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
        queryGroupPersistenceService.updateInClusterStateMetadata(updateQueryGroupRequest, listener);
        verify(listener).onFailure(any(RuntimeException.class));
    }
}

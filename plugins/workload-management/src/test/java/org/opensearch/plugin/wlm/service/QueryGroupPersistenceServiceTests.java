/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.QueryGroup.QueryGroupMode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.wlm.UpdateQueryGroupRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MONITOR;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.SANDBOX_MAX_SETTING_NAME;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertInflightValuesAreZero;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterState;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.prepareSandboxPersistenceService;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupMap;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupPersistenceService;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupTwo;
import static org.mockito.Mockito.mock;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

    public void testGetSingleQueryGroup() {
        List<QueryGroup> groups = queryGroupPersistenceService().getFromClusterStateMetadata(NAME_ONE, clusterState());
        assertEquals(1, groups.size());
        QueryGroup queryGroup = groups.get(0);
        compareQueryGroups(List.of(queryGroupOne), List.of(queryGroup));
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testGetAllQueryGroups() {
        assertEquals(2, clusterState().metadata().queryGroups().size());
        List<QueryGroup> res = queryGroupPersistenceService().getFromClusterStateMetadata(null, clusterState());
        assertEquals(2, res.size());
        Set<String> currentNAME = res.stream().map(QueryGroup::getName).collect(Collectors.toSet());
        assertTrue(currentNAME.contains(NAME_ONE));
        assertTrue(currentNAME.contains(NAME_TWO));
        compareQueryGroups(queryGroupList(), res);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testGetZeroQueryGroups() {
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryGroupPersistenceService sandboxPersistenceService = new QueryGroupPersistenceService(
            mock(ClusterService.class),
            settings,
            clusterSettings
        );
        List<QueryGroup> res = sandboxPersistenceService.getFromClusterStateMetadata(NAME_NONE_EXISTED, clusterState());
        assertEquals(0, res.size());
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testDeleteSingleQueryGroup() {
        ClusterState newClusterState = queryGroupPersistenceService().deleteQueryGroupInClusterState(NAME_TWO, clusterState());
        Set<QueryGroup> afterDeletionGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(1, afterDeletionGroups.size());
        List<QueryGroup> oldSandbox = List.of(queryGroupMap.get(NAME_ONE));
        compareQueryGroups(new ArrayList<>(afterDeletionGroups), oldSandbox);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testDeleteAllQueryGroups() {
        ClusterState newClusterState = queryGroupPersistenceService().deleteQueryGroupInClusterState(null, clusterState());
        Set<QueryGroup> afterDeletionGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(0, afterDeletionGroups.size());
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testDeleteNonExistedQueryGroup() {
        assertThrows(
            RuntimeException.class,
            () -> queryGroupPersistenceService().deleteQueryGroupInClusterState(NAME_NONE_EXISTED, clusterState())
        );
    }

    public void testUpdateQueryGroupAllFields() {
        QueryGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .mode(MONITOR)
            .resourceLimits(Map.of("jvm", 0.15))
            .updatedAt(1690934400000L)
            .build();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_ONE,
            QueryGroupMode.fromName(MONITOR),
            Map.of("jvm", 0.15),
            1690934400000L
        );
        ClusterState newClusterState = queryGroupPersistenceService().updateQueryGroupInClusterState(
            updateQueryGroupRequest,
            clusterState()
        );
        List<QueryGroup> updatedSandboxes = new ArrayList<>(newClusterState.getMetadata().queryGroups());
        assertEquals(2, updatedSandboxes.size());
        compareQueryGroups(List.of(queryGroupTwo, updated), updatedSandboxes);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testUpdateQueryGroupResourceLimitsOnly() {
        QueryGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .mode(MONITOR)
            .resourceLimits(Map.of("jvm", 0.15))
            .updatedAt(1690934400000L)
            .build();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_ONE,
            QueryGroupMode.fromName(MONITOR),
            Map.of("jvm", 0.15),
            1690934400000L
        );
        ClusterState newClusterState = queryGroupPersistenceService().updateQueryGroupInClusterState(
            updateQueryGroupRequest,
            clusterState()
        );
        Set<QueryGroup> updatedSandboxesMap = newClusterState.getMetadata().queryGroups();
        assertEquals(2, updatedSandboxesMap.size());
        Optional<QueryGroup> findUpdatedGroupOne = newClusterState.metadata()
            .queryGroups()
            .stream()
            .filter(group -> group.getName().equals(NAME_ONE))
            .findFirst();
        Optional<QueryGroup> findUpdatedGroupTwo = newClusterState.metadata()
            .queryGroups()
            .stream()
            .filter(group -> group.getName().equals(NAME_TWO))
            .findFirst();
        assertTrue(findUpdatedGroupOne.isPresent());
        assertTrue(findUpdatedGroupTwo.isPresent());
        compareQueryGroups(List.of(updated), List.of(findUpdatedGroupOne.get()));
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testCreateQueryGroup() {
        List<Object> setup = prepareSandboxPersistenceService(new ArrayList<>());
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ClusterState newClusterState = queryGroupPersistenceService1.saveQueryGroupInClusterState(queryGroupOne, clusterState);
        Set<QueryGroup> updatedGroupsMap = newClusterState.getMetadata().queryGroups();
        assertEquals(1, updatedGroupsMap.size());
        Optional<QueryGroup> findUpdatedGroupOne = newClusterState.metadata()
            .queryGroups()
            .stream()
            .filter(group -> group.getName().equals(NAME_ONE))
            .findFirst();
        assertTrue(findUpdatedGroupOne.isPresent());
        compareQueryGroups(List.of(queryGroupOne), List.of(findUpdatedGroupOne.get()));
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testCreateAnotherQueryGroup() {
        List<Object> setup = prepareSandboxPersistenceService(List.of(queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ClusterState newClusterState = queryGroupPersistenceService1.saveQueryGroupInClusterState(queryGroupTwo, clusterState);
        Set<QueryGroup> updatedGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(2, updatedGroups.size());
        compareQueryGroups(queryGroupList(), new ArrayList<>(updatedGroups));
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testCreateQueryGroupDuplicateName() {
        List<Object> setup = prepareSandboxPersistenceService(List.of(queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        QueryGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR)
            .resourceLimits(Map.of("jvm", 0.3))
            .updatedAt(1690934400000L)
            .build();
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    public void testCreateQueryGroupOverflowAllocation() {
        List<Object> setup = prepareSandboxPersistenceService(List.of(queryGroupTwo));
        QueryGroup toCreate = builder().name(NAME_TWO)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR)
            .resourceLimits(Map.of("jvm", 0.5))
            .updatedAt(1690934400000L)
            .build();
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    public void testCreateQueryGroupOverflowCount() {
        QueryGroup toCreate = builder().name(NAME_NONE_EXISTED)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR)
            .resourceLimits(Map.of("jvm", 0.5))
            .updatedAt(1690934400000L)
            .build();
        Metadata metadata = Metadata.builder().queryGroups(new HashSet<>(queryGroupList())).build();
        Settings settings = Settings.builder().put(SANDBOX_MAX_SETTING_NAME, 2).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        QueryGroupPersistenceService queryGroupPersistenceService1 = new QueryGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }
}

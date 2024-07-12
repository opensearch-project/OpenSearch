/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action.service;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.action.CreateQueryGroupResponse;
import org.opensearch.search.ResourceType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.MEMORY_STRING;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.MONITOR_STRING;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils._ID_TWO;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.assertInflightValuesAreZero;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.preparePersistenceServiceSetup;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupPersistenceService;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupTwo;
import static org.opensearch.search.query_group.QueryGroupServiceSettings.QUERY_GROUP_COUNT_SETTING_NAME;
import static org.mockito.Mockito.mock;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

    public void testCreateQueryGroup() {
        List<Object> setup = preparePersistenceServiceSetup(new HashMap<>());
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ClusterState newClusterState = queryGroupPersistenceService1.saveQueryGroupInClusterState(queryGroupOne, clusterState);
        Map<String, QueryGroup> updatedGroupsMap = newClusterState.getMetadata().queryGroups();
        assertEquals(1, updatedGroupsMap.size());
        assertTrue(updatedGroupsMap.containsKey(_ID_ONE));
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(queryGroupOne);
        listTwo.add(updatedGroupsMap.get(_ID_ONE));
        compareQueryGroups(listOne, listTwo);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testCreateAnotherQueryGroup() {
        List<Object> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        ClusterState newClusterState = queryGroupPersistenceService1.saveQueryGroupInClusterState(queryGroupTwo, clusterState);
        Map<String, QueryGroup> updatedGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(2, updatedGroups.size());
        assertTrue(updatedGroups.containsKey(_ID_TWO));
        Collection<QueryGroup> values = updatedGroups.values();
        compareQueryGroups(queryGroupList(), new ArrayList<>(values));
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testCreateQueryGroupDuplicateName() {
        List<Object> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        QueryGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR_STRING)
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.3))
            .updatedAt(1690934400000L)
            .build();
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    public void testCreateQueryGroupOverflowAllocation() {
        List<Object> setup = preparePersistenceServiceSetup(Map.of(_ID_TWO, queryGroupOne));
        QueryGroup toCreate = builder().name(NAME_TWO)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR_STRING)
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.5))
            .updatedAt(1690934400000L)
            .build();
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    public void testCreateQueryGroupOverflowCount() {
        QueryGroup toCreate = builder().name(NAME_NONE_EXISTED)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR_STRING)
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.5))
            .updatedAt(1690934400000L)
            .build();
        Metadata metadata = Metadata.builder().queryGroups(Map.of(_ID_ONE, queryGroupOne, _ID_TWO, queryGroupTwo)).build();
        Settings settings = Settings.builder().put(QUERY_GROUP_COUNT_SETTING_NAME, 2).build();
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

    @SuppressWarnings("unchecked")
    public void testPersist() {
        List<Object> setup = preparePersistenceServiceSetup(Map.of(_ID_ONE, queryGroupOne));
        QueryGroupPersistenceService queryGroupPersistenceService = (QueryGroupPersistenceService) setup.get(0);
        ActionListener<CreateQueryGroupResponse> mockListener = mock(ActionListener.class);
        queryGroupPersistenceService.persist(queryGroupTwo, mockListener);
        Map<String, QueryGroup> newQueryGroups = queryGroupPersistenceService.getClusterService().state().metadata().queryGroups();
        assertEquals(2, newQueryGroups.size());
        assertTrue(newQueryGroups.containsKey(_ID_TWO));
    }
}

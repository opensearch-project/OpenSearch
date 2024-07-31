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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.ResourceType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MEMORY_STRING;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MONITOR_STRING;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterSettingsSet;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.preparePersistenceServiceSetup;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupTwo;
import static org.opensearch.plugin.wlm.service.QueryGroupPersistenceService.QUERY_GROUP_COUNT_SETTING_NAME;
import static org.mockito.Mockito.mock;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

    /**
     * Test case to validate the creation logic of a single QueryGroup
     */
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
    }

    /**
     * Test case to validate the logic for adding a new QueryGroup to a cluster state that already contains
     * an existing QueryGroup
     */
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
    }

    /**
     * Test case to ensure the error is thrown when we try to create another QueryGroup with duplicate name
     */
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

    /**
     * Test case to ensure the error is thrown when we try to create another QueryGroup that will make
     * the total resource limits go above 1
     */
    public void testCreateQueryGroupOverflowAllocation() {
        List<Object> setup = preparePersistenceServiceSetup(Map.of(_ID_TWO, queryGroupTwo));
        QueryGroup toCreate = builder().name(NAME_ONE)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR_STRING)
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.41))
            .updatedAt(1690934400000L)
            .build();
        QueryGroupPersistenceService queryGroupPersistenceService1 = (QueryGroupPersistenceService) setup.get(0);
        ClusterState clusterState = (ClusterState) setup.get(1);
        assertThrows(RuntimeException.class, () -> queryGroupPersistenceService1.saveQueryGroupInClusterState(toCreate, clusterState));
    }

    /**
     * Test case to ensure the error is thrown when we already have the max allowed number of QueryGroups, but
     * we want to create another one
     */
    public void testCreateQueryGroupOverflowCount() {
        QueryGroup toCreate = builder().name(NAME_NONE_EXISTED)
            ._id("W5iIqHyhgi4K1qIAAAAIHw==")
            .mode(MONITOR_STRING)
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.5))
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
}

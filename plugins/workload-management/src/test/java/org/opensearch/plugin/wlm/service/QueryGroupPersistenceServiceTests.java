/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.QueryGroup.ResiliencyMode;
import org.opensearch.plugin.wlm.UpdateQueryGroupRequest;
import org.opensearch.search.ResourceType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MEMORY_STRING;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.MONITOR_STRING;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertInflightValuesAreZero;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterState;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupPersistenceService;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupTwo;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {
    public void testUpdateQueryGroupAllFields() {
        QueryGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .mode("enforced")
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.15))
            .updatedAt(1690934400000L)
            .build();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_ONE,
            ResiliencyMode.fromName("enforced"),
            Map.of(ResourceType.fromName(MEMORY_STRING), 0.15),
            1690934400000L
        );
        ClusterState newClusterState = queryGroupPersistenceService().updateQueryGroupInClusterState(
            updateQueryGroupRequest,
            clusterState()
        );
        List<QueryGroup> updatedQueryGroups = new ArrayList<>(newClusterState.getMetadata().queryGroups().values());
        assertEquals(2, updatedQueryGroups.size());
        List<QueryGroup> expectedList = new ArrayList<>();
        expectedList.add(queryGroupTwo);
        expectedList.add(updated);
        compareQueryGroups(expectedList, updatedQueryGroups);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testUpdateQueryGroupResourceLimitsOnly() {
        QueryGroup updated = builder().name(NAME_ONE)
            ._id(_ID_ONE)
            .mode(MONITOR_STRING)
            .resourceLimits(Map.of(ResourceType.fromName(MEMORY_STRING), 0.15))
            .updatedAt(1690934400000L)
            .build();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_ONE,
            ResiliencyMode.fromName(MONITOR_STRING),
            Map.of(ResourceType.fromName(MEMORY_STRING), 0.15),
            1690934400000L
        );
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
        compareQueryGroups(list1, list2);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testUpdateQueryGroupNonExistedName() {
        QueryGroupPersistenceService queryGroupPersistenceService = queryGroupPersistenceService();
        UpdateQueryGroupRequest updateQueryGroupRequest = new UpdateQueryGroupRequest(
            NAME_NONE_EXISTED,
            ResiliencyMode.fromName(MONITOR_STRING),
            Map.of(ResourceType.fromName(MEMORY_STRING), 0.15),
            1690934400000L
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
        compareQueryGroups(expectedList, updatedQueryGroups);
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.plugin.wlm.QueryGroupTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

    public void testGetSingleQueryGroup() {
        List<QueryGroup> groups = QueryGroupPersistenceService.getFromClusterStateMetadata(
            QueryGroupTestUtils.NAME_ONE,
            QueryGroupTestUtils.clusterState()
        );
        assertEquals(1, groups.size());
        QueryGroup queryGroup = groups.get(0);
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(QueryGroupTestUtils.queryGroupOne);
        listTwo.add(queryGroup);
        QueryGroupTestUtils.compareQueryGroups(listOne, listTwo);
    }

    public void testGetAllQueryGroups() {
        assertEquals(2, QueryGroupTestUtils.clusterState().metadata().queryGroups().size());
        List<QueryGroup> res = QueryGroupPersistenceService.getFromClusterStateMetadata(null, QueryGroupTestUtils.clusterState());
        assertEquals(2, res.size());
        Set<String> currentNAME = res.stream().map(QueryGroup::getName).collect(Collectors.toSet());
        assertTrue(currentNAME.contains(QueryGroupTestUtils.NAME_ONE));
        assertTrue(currentNAME.contains(QueryGroupTestUtils.NAME_TWO));
        QueryGroupTestUtils.compareQueryGroups(QueryGroupTestUtils.queryGroupList(), res);
    }

    public void testGetZeroQueryGroups() {
        List<QueryGroup> res = QueryGroupPersistenceService.getFromClusterStateMetadata(
            QueryGroupTestUtils.NAME_NONE_EXISTED,
            QueryGroupTestUtils.clusterState()
        );
        assertEquals(0, res.size());
    }

    public void testGetNonExistedQueryGroups() {
        List<QueryGroup> groups = QueryGroupPersistenceService.getFromClusterStateMetadata(
            QueryGroupTestUtils.NAME_NONE_EXISTED,
            QueryGroupTestUtils.clusterState()
        );
        assertEquals(0, groups.size());
    }

    public void testMaxQueryGroupCount() {
        assertThrows(IllegalArgumentException.class, () -> QueryGroupTestUtils.queryGroupPersistenceService().setMaxQueryGroupCount(-1));
        QueryGroupPersistenceService queryGroupPersistenceService = QueryGroupTestUtils.queryGroupPersistenceService();
        queryGroupPersistenceService.setMaxQueryGroupCount(50);
        assertEquals(50, queryGroupPersistenceService.getMaxQueryGroupCount());

    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action.service;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.assertInflightValuesAreZero;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.clusterState;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupPersistenceService;
import static org.mockito.Mockito.mock;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

    public void testGetSingleQueryGroup() {
        List<QueryGroup> groups = queryGroupPersistenceService().getFromClusterStateMetadata(NAME_ONE, clusterState());
        assertEquals(1, groups.size());
        QueryGroup queryGroup = groups.get(0);
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(queryGroupOne);
        listTwo.add(queryGroup);
        compareQueryGroups(listOne, listTwo);
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
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            mock(ClusterService.class),
            settings,
            clusterSettings
        );
        List<QueryGroup> res = queryGroupPersistenceService.getFromClusterStateMetadata(NAME_NONE_EXISTED, clusterState());
        assertEquals(0, res.size());
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }

    public void testGetNonExistedQueryGroups() {
        List<QueryGroup> groups = queryGroupPersistenceService().getFromClusterStateMetadata(NAME_NONE_EXISTED, clusterState());
        assertEquals(0, groups.size());
        assertInflightValuesAreZero(queryGroupPersistenceService());
    }
}

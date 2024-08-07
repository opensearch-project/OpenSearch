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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_NONE_EXISTED;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.NAME_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils._ID_TWO;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.clusterState;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.compareQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupPersistenceService;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {
    public void testDeleteSingleQueryGroup() {
        ClusterState newClusterState = queryGroupPersistenceService().deleteQueryGroupInClusterState(NAME_TWO, clusterState());
        Map<String, QueryGroup> afterDeletionGroups = newClusterState.getMetadata().queryGroups();
        assertFalse(afterDeletionGroups.containsKey(_ID_TWO));
        assertEquals(1, afterDeletionGroups.size());
        List<QueryGroup> oldQueryGroups = new ArrayList<>();
        oldQueryGroups.add(queryGroupOne);
        compareQueryGroups(new ArrayList<>(afterDeletionGroups.values()), oldQueryGroups);
    }

    public void testDeleteAllQueryGroups() {
        ClusterState newClusterState = queryGroupPersistenceService().deleteQueryGroupInClusterState(null, clusterState());
        Map<String, QueryGroup> afterDeletionGroups = newClusterState.getMetadata().queryGroups();
        assertEquals(0, afterDeletionGroups.size());
    }

    public void testDeleteNonExistedQueryGroup() {
        assertThrows(
            RuntimeException.class,
            () -> queryGroupPersistenceService().deleteQueryGroupInClusterState(NAME_NONE_EXISTED, clusterState())
        );
    }
}

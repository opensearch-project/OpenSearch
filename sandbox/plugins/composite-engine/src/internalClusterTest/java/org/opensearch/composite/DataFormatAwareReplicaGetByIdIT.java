/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.get.GetResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * End-to-end get-by-id coverage for {@link org.opensearch.index.engine.DataFormatAwareNRTReplicationEngine}:
 * a doc indexed on the primary is resolvable by id from a replica shard via the row path (the replica
 * engine has no live version map) once segment replication has propagated it.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicaGetByIdIT extends DataFormatAwareReplicationBaseIT {

    public void testGetByIdFromReplica() throws Exception {
        createDfaIndex(1); // 1 replica, 2 data nodes (from base)
        indexDocs(20);     // ids 0..19, RefreshPolicy.NONE
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        // Ensure the replica's catalog has converged with the primary (segments replicated).
        assertCatalogSnapshotsConverged(INDEX_NAME);

        String replicaNode = replicaNodeNames().get(0);
        // Route the GET to the replica copy so DataFormatAwareNRTReplicationEngine#getById serves it.
        GetResponse resp = client().prepareGet(INDEX_NAME, "5").setPreference("_only_nodes:" + replicaNode).setRealtime(false).get();

        assertTrue("replica get-by-id must find the replicated doc via rows", resp.isExists());
        assertEquals(1L, resp.getVersion());
        assertEquals(5L, ((Number) resp.getSourceAsMap().get("field_number")).longValue());
        assertNotNull(resp.getSourceAsMap().get("field_text"));
        assertNotNull(resp.getSourceAsMap().get("field_keyword"));
    }
}

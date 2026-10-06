/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Verifies that primary-side updates become visible on replicas through segment replication.
 * Companion to {@link DataFormatAwareUpdateIT}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicaUpdateIT extends DataFormatAwareReplicationBaseIT {

    public void testUpdatedDocVisibleOnReplica() throws Exception {
        // Disable the final append-only setting at index creation to allow updates.
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        Settings settings = Settings.builder()
            .put(dfaIndexSettings(1)) // 1 replica, segment replication, composite parquet+lucene
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .build();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        IndexResponse created = client().prepareIndex(INDEX_NAME)
            .setId("k1")
            .setSource("field_text", "old_text", "field_keyword", "old_kw", "field_number", 1L)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        assertEquals(1L, created.getVersion());

        IndexResponse updated = client().prepareIndex(INDEX_NAME)
            .setId("k1")
            .setSource("field_text", "new_text", "field_keyword", "new_kw", "field_number", 2L)
            .get();
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        // Refresh, then wait for segment replication to carry the updated segments to the replica.
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertCatalogSnapshotsConverged(INDEX_NAME);

        // Verify the replica resolves the updated row.
        String replicaNode = replicaNodeNames().get(0);
        GetResponse resp = client().prepareGet(INDEX_NAME, "k1").setPreference("_only_nodes:" + replicaNode).setRealtime(false).get();
        assertTrue("replica must resolve the updated doc via rows", resp.isExists());
        assertEquals("new_text", resp.getSourceAsMap().get("field_text"));
        assertEquals("new_kw", resp.getSourceAsMap().get("field_keyword"));
        assertEquals(2L, resp.getVersion());
    }
}

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
 * Replica-visibility coverage for updates on {@link org.opensearch.index.engine.DataFormatAwareEngine}.
 *
 * <p>An update ("delete the prior copy, add the new one") is applied on the <b>primary only</b>. The
 * replica runs {@link org.opensearch.index.engine.DataFormatAwareNRTReplicationEngine}, whose
 * {@code index()}/{@code delete()} are translog + checkpoint bookkeeping only — it has no update
 * logic. It receives the already-deduplicated segments via segment replication. This test proves
 * that an updated doc becomes visible with its <b>new</b> content on the replica purely through
 * replication, which is why update support lives only in the primary DFAE and not in the NRT
 * replica (or the read-only) engine.
 *
 * <p>Companion to {@link DataFormatAwareUpdateIT} (primary-side scenarios); split out because
 * replica ITs require the multi-node segment-replication harness in
 * {@link DataFormatAwareReplicationBaseIT}, mirroring the existing
 * {@link DataFormatAwareGetByIdIT} / {@link DataFormatAwareReplicaGetByIdIT} split.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicaUpdateIT extends DataFormatAwareReplicationBaseIT {

    public void testUpdatedDocVisibleOnReplica() throws Exception {
        // Composite indices are append-only by default, which makes the transport layer reject
        // custom-_id INDEX / update ops before they reach the engine. Disable it (a Final setting, so
        // it must be set at creation) to exercise the update path. Mirrors
        // DataFormatAwareReplicationBaseIT.createDfaIndex(1) with the extra setting layered on.
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        Settings settings = Settings.builder()
            .put(dfaIndexSettings(1)) // 1 replica, segment replication, composite parquet+lucene
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .build();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Create k1 on the primary.
        IndexResponse created = client().prepareIndex(INDEX_NAME)
            .setId("k1")
            .setSource("field_text", "old_text", "field_keyword", "old_kw", "field_number", 1L)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, created.getResult());
        assertEquals(1L, created.getVersion());

        // Update k1 on the primary (delete-old + add-new happens on the primary only).
        IndexResponse updated = client().prepareIndex(INDEX_NAME)
            .setId("k1")
            .setSource("field_text", "new_text", "field_keyword", "new_kw", "field_number", 2L)
            .get();
        assertEquals(DocWriteResponse.Result.UPDATED, updated.getResult());
        assertEquals(2L, updated.getVersion());

        // Refresh, then wait for segment replication to carry the updated segments to the replica.
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertCatalogSnapshotsConverged(INDEX_NAME);

        // Route GET to the replica: DataFormatAwareNRTReplicationEngine#getById resolves via the row
        // path (no version map). It must return the UPDATED content — proving the delete-old+add-new
        // propagated purely via segment replication, with no replica-side update handling.
        String replicaNode = replicaNodeNames().get(0);
        GetResponse resp = client().prepareGet(INDEX_NAME, "k1").setPreference("_only_nodes:" + replicaNode).setRealtime(false).get();
        assertTrue("replica must resolve the updated doc via rows", resp.isExists());
        assertEquals("new_text", resp.getSourceAsMap().get("field_text"));
        assertEquals("new_kw", resp.getSourceAsMap().get("field_keyword"));
        assertEquals(2L, resp.getVersion());
    }
}

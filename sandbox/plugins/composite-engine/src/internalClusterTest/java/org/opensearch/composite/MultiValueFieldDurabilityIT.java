/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

/**
 * Durability lifecycle for multi-valued ({@code multi_value: true}) keyword fields on a composite
 * parquet+lucene index: refresh, flush, force-merge, replication to a peer, and recovery.
 *
 * <p>A multi-valued field is stored as an Arrow/Parquet {@code LIST<element>} column rather than a
 * flat column, so every stage that moves or rewrites those files is a place the list encoding can
 * break independently of the read path: the VSR flush writes offsets, force-merge re-encodes each
 * column through {@code compute_leaves}, segment replication ships the files to a replica, and
 * recovery replays or re-fetches them. Each assertion below re-reads the array and requires the
 * values to be byte-identical, in document order, duplicates intact — the column is the source of
 * truth for derived {@code _source}, so any loss is silent data corruption rather than a query bug.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MultiValueFieldDurabilityIT extends DataFormatAwareReplicationBaseIT {

    private static final String MV_INDEX = "mv-durability-idx";

    /** Document id → the exact array it was indexed with. */
    private static final Map<String, List<String>> EXPECTED = Map.of(
        "d1",
        List.of("beta", "alpha", "beta"),
        "d2",
        List.of("solo"),
        "d3",
        List.of(),
        "d4",
        List.of("x", "y", "z", "x")
    );

    private Settings mvIndexSettings(int replicaCount) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicaCount, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, org.opensearch.indices.replication.common.ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    private static final String MAPPING = "{\"properties\":{"
        + "\"id\":{\"type\":\"keyword\"},"
        + "\"tags\":{\"type\":\"keyword\",\"multi_value\":true}"
        + "}}";

    private void createMvIndex(int replicaCount) {
        client().admin().indices().prepareCreate(MV_INDEX).setSettings(mvIndexSettings(replicaCount)).setMapping(MAPPING).get();
        ensureYellowAndNoInitializingShards(MV_INDEX);
    }

    /** Generated _id → fixture key, captured at index time (append-only index rejects custom _ids). */
    private final Map<String, String> idToFixture = new java.util.HashMap<>();

    /** Indexes the four fixture docs with the given refresh policy, recording their generated ids. */
    private void indexFixtures(WriteRequest.RefreshPolicy policy) {
        for (Map.Entry<String, List<String>> e : EXPECTED.entrySet()) {
            StringBuilder json = new StringBuilder("{\"id\":\"").append(e.getKey()).append("\",\"tags\":[");
            for (int i = 0; i < e.getValue().size(); i++) {
                if (i > 0) json.append(",");
                json.append("\"").append(e.getValue().get(i)).append("\"");
            }
            json.append("]}");
            org.opensearch.action.index.IndexResponse resp = client().prepareIndex(MV_INDEX)
                .setRefreshPolicy(policy)
                .setSource(json.toString(), XContentType.JSON)
                .get();
            idToFixture.put(resp.getId(), e.getKey());
        }
    }

    /**
     * Asserts every indexed document still carries exactly its own values.
     *
     * <p>Reads via get-by-id rather than {@code _search}: on a composite index
     * {@code IndexShard.applyOnEngine} rejects {@code DataFormatAwareEngine}, so there is no
     * searcher. The get path reconstructs {@code _source} from the Parquet columns (parquet-owned
     * fields have no Lucene stored fields), making this a direct test of the LIST column's
     * integrity rather than of a cached copy.
     */
    @SuppressWarnings("unchecked")
    private void assertArraysIntact(String stage) {
        assertFalse(stage + ": no documents were indexed", idToFixture.isEmpty());
        for (Map.Entry<String, String> entry : idToFixture.entrySet()) {
            org.opensearch.action.get.GetResponse resp = client().prepareGet(MV_INDEX, entry.getKey()).setRealtime(false).get();
            assertTrue(stage + ": document [" + entry.getKey() + "] must exist", resp.isExists());
            Map<String, Object> source = resp.getSourceAsMap();
            List<String> want = EXPECTED.get(entry.getValue());
            Object actual = source.get("tags");
            List<String> got = actual == null ? List.of() : ((List<Object>) actual).stream().map(String::valueOf).toList();
            assertEquals(stage + ": fixture [" + entry.getValue() + "] lost or reordered values", want, got);
        }
    }

    /**
     * refresh → flush → force-merge on a single primary. Each stage either makes the in-memory VSR
     * durable or rewrites the Parquet files; the array must survive all three unchanged.
     */
    public void testArraysSurviveRefreshFlushAndForceMerge() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createMvIndex(0);
        ensureGreen(MV_INDEX);

        // Refresh: flushes the active VSR into a readable Parquet generation.
        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        assertArraysIntact("after refresh");

        // Flush: commits the generation.
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();
        assertArraysIntact("after flush");

        // A second generation, so force-merge has more than one file to combine.
        indexFixtures(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();

        ForceMergeResponse merge = client().admin().indices().prepareForceMerge(MV_INDEX).setMaxNumSegments(1).get();
        assertEquals("force-merge must not fail any shard", 0, merge.getFailedShards());
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();

        assertArraysIntact("after force-merge");
    }

    /**
     * Segment replication of a LIST column to a peer: the replica reads the primary's Parquet files
     * verbatim, so a replica-side read failure would mean the file or its catalog entry is not
     * self-describing.
     */
    public void testArraysReplicateToPeer() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createMvIndex(1);
        ensureGreen(MV_INDEX);

        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();

        // Segment replication is asynchronous. Wait for the replica's catalog to carry the same
        // parquet files as the primary — the base helper's node lookups are bound to its own
        // INDEX_NAME, so resolve this index's shards here.
        assertBusy(() -> {
            org.opensearch.cluster.routing.IndexShardRoutingTable routing = getClusterState().routingTable().index(MV_INDEX).shard(0);
            String primaryNode = getClusterState().nodes().get(routing.primaryShard().currentNodeId()).getName();
            org.opensearch.index.shard.IndexShard primary = getIndexShard(primaryNode, MV_INDEX);
            java.util.Set<String> primaryFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
            assertFalse("primary must have parquet files in its catalog", primaryFiles.isEmpty());
            for (org.opensearch.cluster.routing.ShardRouting r : routing.replicaShards()) {
                assertTrue("replica must be started", r.started());
                String replicaNode = getClusterState().nodes().get(r.currentNodeId()).getName();
                org.opensearch.index.shard.IndexShard replica = getIndexShard(replicaNode, MV_INDEX);
                assertEquals(
                    "primary/replica catalog files must converge on " + replicaNode,
                    primaryFiles,
                    DataFormatAwareITUtils.catalogFilesExcludingSegments(replica)
                );
            }
        }, 60, java.util.concurrent.TimeUnit.SECONDS);

        // Values must still read back intact once the replica holds the same files.
        assertArraysIntact("after replication to peer");
    }

    /**
     * Recovery: restart the data node holding the primary and re-read. Exercises the commit +
     * catalog replay path over a LIST column — a lost or mis-sized offsets buffer would surface as
     * a read failure or altered values after recovery rather than at write time.
     */
    public void testArraysSurviveNodeRestartRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createMvIndex(0);
        ensureGreen(MV_INDEX);

        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();
        assertArraysIntact("before restart");

        internalCluster().restartRandomDataNode();
        ensureGreen(MV_INDEX);

        assertArraysIntact("after node restart recovery");
    }
}

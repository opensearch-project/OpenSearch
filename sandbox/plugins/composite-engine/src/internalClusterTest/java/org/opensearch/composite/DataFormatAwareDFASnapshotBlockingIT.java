/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.Node;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.transport.client.Client;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests covering the snapshot guardrails for pluggable-data-format-aware (DFA) indexes:
 * <ul>
 *   <li><b>Block A</b> ({@code SnapshotsService.createSnapshotV2}): warm-tiered DFA indexes are
 *       silently filtered out of V2 snapshots; the rest of the cluster is captured normally.</li>
 * </ul>
 *
 * Together these guarantee: hot DFA + non-DFA flow normally through V2 snapshots; warm DFA never
 * appears in any snapshot.
 */
public class DataFormatAwareDFASnapshotBlockingIT extends DataFormatAwareReadonlyEngineBaseIT {

    private static final String V2_REPO = "v2-block-test-repo";

    private Path v2RepoPath;

    @Before
    public void setupSnapshotRepos() throws java.io.IOException {
        v2RepoPath = randomRepoPath().toAbsolutePath();
        Files.createDirectories(v2RepoPath);
    }

    @After
    public void tearDownFileCache() {
        for (String nodeName : internalCluster().getNodeNames()) {
            try {
                Node node = internalCluster().getInstance(Node.class, nodeName);
                var fc = node.fileCache();
                if (fc != null) {
                    fc.clear();
                }
            } catch (Exception ignored) {
                // Node may have already been stopped; skip it.
            }
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(2, ByteSizeUnit.GB).toString())
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
    }

    private Settings.Builder v2RepoSettings(Path location) {
        return Settings.builder()
            .put("location", location)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
    }

    /** Create a hot DFA index with the given name, index docs, and flush. */
    private void createHotDFAIndex(String indexName, int docs) {
        Settings hot = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
        client().admin().indices().prepareCreate(indexName).setSettings(hot).get();
        ensureGreen(indexName);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("n", (long) i).get();
        }
        client().admin().indices().prepareFlush(indexName).setForce(true).get();
    }

    /** Create a hot DFA index, ingest docs, then tier to warm. */
    private void createWarmDFAIndex(String indexName, int docs) {
        createHotDFAIndex(indexName, docs);
        client().admin().indices().prepareClose(indexName).get();
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true))
            .get();
        client().admin().indices().prepareOpen(indexName).get();
        ensureGreen(indexName);
    }

    /** Create a plain non-DFA, remote-store-backed index. */
    private void createNonDFAIndex(String indexName, int docs) {
        Settings nonDfa = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        client().admin().indices().prepareCreate(indexName).setSettings(nonDfa).get();
        ensureGreen(indexName);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("text", "n" + i).get();
        }
        client().admin().indices().prepareFlush(indexName).setForce(true).get();
        client().admin().indices().prepareRefresh(indexName).get();
    }

    /** Create a writable-warm (non-DFA) index directly and ingest docs. */
    private void createWarmNonDFAIndex(String indexName, int docs) {
        // Writable warm: the index is created warm from the start (OpenSearch does not support
        // tiering a hot non-DFA index to warm). Remote store is enabled cluster-wide by the base.
        Settings warmNonDfa = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .build();
        client().admin().indices().prepareCreate(indexName).setSettings(warmNonDfa).get();
        ensureGreen(indexName);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("text", "n" + i).get();
        }
        client().admin().indices().prepareFlush(indexName).setForce(true).get();
        client().admin().indices().prepareRefresh(indexName).get();
    }

    /**
     * Block A coverage — the headline V2 partial-filter test for a mixed cluster:
     * <ul>
     *   <li>Take a V2 snapshot of a cluster with hot DFA + warm DFA + non-DFA indexes.</li>
     *   <li>Verify the snapshot manifest contains hot DFA + non-DFA, NOT warm DFA.</li>
     *   <li>Delete all indexes, restore all (*), and verify from cluster state that hot DFA +
     *       non-DFA come back while the filtered warm DFA index does not.</li>
     *   <li>Phase 3: explicitly request restore of the warm DFA index from the snapshot — fails
     *       because it's not in the snapshot manifest (Block A filtered it at create time).</li>
     * </ul>
     */
    public void testV2SnapshotMixedClusterFiltersWarmDFAFullLifecycle() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);

        final String hotDFA = "hot-dfa-mix";
        final String warmDFA = "warm-dfa-mix";
        final String nonDFA = "non-dfa-mix";

        createRepository(V2_REPO, "fs", v2RepoSettings(v2RepoPath));

        createHotDFAIndex(hotDFA, 30);
        createWarmDFAIndex(warmDFA, 20);
        createNonDFAIndex(nonDFA, 15);

        // Phase 1: take V2 snapshot, verify warm DFA is filtered.
        CreateSnapshotResponse snapResp = client().admin()
            .cluster()
            .prepareCreateSnapshot(V2_REPO, "v2-mixed-snap")
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo info = snapResp.getSnapshotInfo();
        assertEquals(SnapshotState.SUCCESS, info.state());

        List<String> snapshotIndices = info.indices();
        assertTrue("snapshot must contain hot DFA index, got: " + snapshotIndices, snapshotIndices.contains(hotDFA));
        assertTrue("snapshot must contain non-DFA index, got: " + snapshotIndices, snapshotIndices.contains(nonDFA));
        assertFalse("warm DFA index must be filtered out, got: " + snapshotIndices, snapshotIndices.contains(warmDFA));

        // Phase 2: delete every index, then restore all (*). Validate from cluster state that only
        // the snapshotted indices (hot DFA + non-DFA) come back; the filtered warm DFA does not.
        Client client = client();
        assertAcked(client.admin().indices().delete(new DeleteIndexRequest(hotDFA, nonDFA, warmDFA)).get());
        assertFalse(indexExists(hotDFA));
        assertFalse(indexExists(nonDFA));
        assertFalse(indexExists(warmDFA));

        RestoreSnapshotResponse restoreResp = client.admin()
            .cluster()
            .prepareRestoreSnapshot(V2_REPO, "v2-mixed-snap")
            .setIndices("*")
            .setWaitForCompletion(true)
            .get();
        assertEquals(RestStatus.OK, restoreResp.status());
        ensureGreen(hotDFA, nonDFA);

        // Validate the restored set from cluster state.
        assertTrue("hot DFA must be restored", indexExists(hotDFA));
        assertTrue("non-DFA must be restored", indexExists(nonDFA));
        assertFalse("warm DFA must NOT be restored (filtered from snapshot)", indexExists(warmDFA));

        // Phase 3: explicitly try to restore warm DFA from the snapshot → must fail (not in manifest).
        Exception ex = expectThrows(
            Exception.class,
            () -> client.admin()
                .cluster()
                .prepareRestoreSnapshot(V2_REPO, "v2-mixed-snap")
                .setWaitForCompletion(true)
                .setIndices(warmDFA)
                .get()
        );
        // Standard "no indices in snapshot match" / "index does not exist in snapshot" type error.
        assertNotNull("explicit restore of filtered warm DFA must fail", ex.getMessage());
    }

    /**
     * Block A coverage — only-warm-DFA cluster. Snapshot succeeds with empty index list; explicit
     * restore of the warm DFA index fails because it's not in the manifest.
     */
    public void testV2SnapshotWhenClusterHasOnlyWarmDFA() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);

        final String warmDFA = "only-warm-dfa";

        createRepository(V2_REPO, "fs", v2RepoSettings(v2RepoPath));
        createWarmDFAIndex(warmDFA, 10);

        CreateSnapshotResponse snapResp = client().admin()
            .cluster()
            .prepareCreateSnapshot(V2_REPO, "v2-only-warm")
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo info = snapResp.getSnapshotInfo();
        assertEquals(SnapshotState.SUCCESS, info.state());
        assertTrue("snapshot must be empty when cluster has only warm DFA, got: " + info.indices(), info.indices().isEmpty());

        // Explicit restore of warm DFA → fails (not in snapshot).
        Exception ex = expectThrows(
            Exception.class,
            () -> client().admin()
                .cluster()
                .prepareRestoreSnapshot(V2_REPO, "v2-only-warm")
                .setWaitForCompletion(true)
                .setIndices(warmDFA)
                .get()
        );
        assertNotNull("explicit restore of warm DFA must fail; snapshot has no such index", ex.getMessage());
    }

    /**
     * Block A guard — warm tiering alone must NOT trigger exclusion; only the combination of
     * warm tier + pluggable data format is filtered. This verifies that a writable-warm
     * <em>non-DFA</em> index is captured by a V2 snapshot (present in the manifest with a
     * successful shard), while a warm DFA index in the same cluster is still filtered out.
     */
    public void testV2SnapshotIncludesWarmNonDFAIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1);

        final String warmNonDFA = "warm-non-dfa";
        final String warmDFA = "warm-dfa";

        createRepository(V2_REPO, "fs", v2RepoSettings(v2RepoPath));
        createWarmNonDFAIndex(warmNonDFA, 15);
        createWarmDFAIndex(warmDFA, 10);

        // Take V2 snapshot — warm non-DFA must be included, warm DFA must be filtered.
        CreateSnapshotResponse snapResp = client().admin()
            .cluster()
            .prepareCreateSnapshot(V2_REPO, "v2-warm-mix")
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo info = snapResp.getSnapshotInfo();
        assertEquals(SnapshotState.SUCCESS, info.state());

        List<String> snapshotIndices = info.indices();
        assertTrue("warm non-DFA index must be included in snapshot, got: " + snapshotIndices, snapshotIndices.contains(warmNonDFA));
        assertFalse("warm DFA index must be filtered out, got: " + snapshotIndices, snapshotIndices.contains(warmDFA));
        // The warm non-DFA shard must actually be captured, not just listed.
        assertTrue("warm non-DFA shard must be snapshotted, successfulShards=" + info.successfulShards(), info.successfulShards() >= 1);

        // Restore all (*): delete both indices, restore, and validate from cluster state that only
        // the warm non-DFA index comes back; the warm DFA index (filtered at create time) does not.
        Client client = client();
        assertAcked(client.admin().indices().delete(new DeleteIndexRequest(warmNonDFA, warmDFA)).get());
        assertFalse(indexExists(warmNonDFA));
        assertFalse(indexExists(warmDFA));

        RestoreSnapshotResponse restoreResp = client.admin()
            .cluster()
            .prepareRestoreSnapshot(V2_REPO, "v2-warm-mix")
            .setIndices("*")
            .setWaitForCompletion(true)
            .get();
        assertEquals(RestStatus.OK, restoreResp.status());
        ensureGreen(warmNonDFA);

        // Validate the restored set from cluster state.
        assertTrue("warm non-DFA must be restored", indexExists(warmNonDFA));
        assertFalse("warm DFA must NOT be restored (filtered from snapshot)", indexExists(warmDFA));
    }
}

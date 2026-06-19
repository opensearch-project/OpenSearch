/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration tests demonstrating that fsync-on-commit enables reliable local recovery
 * for the primary shard on node restart, while replicas recover via remote store +
 * incremental translog replay.
 *
 * <p>Without fsync-on-commit, committed Parquet/Lucene files could be lost on crash
 * (data only in OS page cache), requiring a full remote store restore. With fsync,
 * the primary can recover locally from its committed files — only uncommitted data
 * (in translog) needs replay.
 *
 * <p>Replica recovery combines:
 * <ol>
 *   <li>Downloading committed segments from remote segment store</li>
 *   <li>Replaying incremental translog operations for unflushed data</li>
 * </ol>
 */
@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/actions/runs/27134969547/job/80085120367?pr=21817")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeRemoteStoreFsyncRecoveryIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "fsync-recovery-test";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ArrowBasePlugin.class,
                ParquetDataFormatPlugin.class,
                CompositeDataFormatPlugin.class,
                LucenePlugin.class,
                DataFusionPlugin.class
            )
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings compositeIndexSettings(int replicas) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicas, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    private void indexDocs(int count, int offset) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(offset + i))
                .setSource("name", "doc_" + (offset + i), "value", offset + i)
                .get();
        }
    }

    private long getDocCount() {
        IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX_NAME).clear().setDocs(true).get();
        return stats.getTotal().getDocs().getCount();
    }

    private String primaryNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return getClusterState().nodes().get(nodeId).getName();
    }

    /**
     * Primary local recovery after node restart.
     *
     * <p>Demonstrates that fsync-on-commit makes committed data durable on local disk:
     * <ol>
     *   <li>Index batch 1 → flush (commits + fsyncs Parquet and Lucene files)</li>
     *   <li>Index batch 2 → refresh only (in translog, NOT committed)</li>
     *   <li>Restart the node (simulates crash)</li>
     *   <li>After restart: committed files survive on local disk (fsync guarantee),
     *       translog replays batch 2 → total doc count = batch1 + batch2</li>
     * </ol>
     *
     * <p>Without fsync, committed Parquet files could be lost on restart (only in page cache),
     * requiring a full remote store download instead of fast local recovery.
     */
    public void testPrimaryLocalRecoveryWithFsync() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(compositeIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        // Batch 1: committed (fsync'd to local disk + uploaded to remote store)
        int committedDocs = randomIntBetween(15, 30);
        indexDocs(committedDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Verify committed files exist on local disk (both parquet + lucene)
        assertBusy(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
            assertTrue("Parquet dir must exist after commit", Files.isDirectory(parquetDir));
            try (Stream<Path> files = Files.list(parquetDir)) {
                assertTrue("Committed parquet files must be on disk", files.findAny().isPresent());
            }
            Path indexDir = shard.shardPath().resolveIndex();
            assertTrue("Lucene index dir must exist after commit", Files.isDirectory(indexDir));
        }, 30, TimeUnit.SECONDS);

        // Batch 2: in translog only (refreshed but NOT committed)
        int uncommittedDocs = randomIntBetween(5, 15);
        indexDocs(uncommittedDocs, committedDocs);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        long expectedTotal = committedDocs + uncommittedDocs;
        assertEquals("Pre-restart doc count must be correct", expectedTotal, getDocCount());

        // Restart the primary node — simulates a crash
        String nodeName = primaryNodeName();
        internalCluster().restartNode(nodeName);
        ensureGreen(INDEX_NAME);

        // After restart: local recovery uses fsync'd committed files + translog replay
        long actualTotal = getDocCount();
        assertEquals(
            "After restart, local recovery (fsync'd files) + translog replay must restore all docs. "
                + "committed="
                + committedDocs
                + " uncommitted="
                + uncommittedDocs,
            expectedTotal,
            actualTotal
        );

        // Verify recovery type: primary must recover from EXISTING_STORE (local committed files)
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(INDEX_NAME).get();
        RecoveryState primaryRecovery = recoveryResponse.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(rs -> rs.getPrimary())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No primary recovery state found"));

        assertEquals(
            "Primary must recover from EXISTING_STORE (local fsync'd files), not REMOTE_STORE",
            RecoverySource.Type.EXISTING_STORE,
            primaryRecovery.getRecoverySource().getType()
        );

        // Verify both format files exist on the recovered primary
        assertBusy(() -> {
            IndexShard recoveredShard = getIndexShard(primaryNodeName(), INDEX_NAME);
            Path parquetDir = recoveredShard.shardPath().getDataPath().resolve("parquet");
            assertTrue("Parquet files must survive restart (fsync guarantee)", Files.isDirectory(parquetDir));
            try (Stream<Path> files = Files.list(parquetDir)) {
                assertTrue("Parquet files must be present after local recovery", files.findAny().isPresent());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Replica recovery via remote store segments + incremental translog replay.
     *
     * <p>Demonstrates the replica recovery path:
     * <ol>
     *   <li>Primary indexes batch 1 → flush (commits, fsyncs, uploads to remote store)</li>
     *   <li>Primary indexes batch 2 → refresh only (in remote translog, NOT committed)</li>
     *   <li>Start a new data node with replica allocation</li>
     *   <li>Replica recovers by:
     *       <ul>
     *         <li>Downloading committed segments from remote segment store</li>
     *         <li>Replaying incremental translog operations for batch 2</li>
     *       </ul>
     *   </li>
     *   <li>Both primary and replica end up with identical doc counts</li>
     * </ol>
     */
    public void testReplicaRecoveryFromRemoteStoreWithIncrementalCheckpoints() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        // Create index with 0 replicas initially
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(compositeIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        // Batch 1: committed on primary (uploaded to remote segment store)
        int committedDocs = randomIntBetween(15, 30);
        indexDocs(committedDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Wait for remote store upload to complete
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = primary.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("Remote metadata must exist after flush", meta);
            Set<String> formats = meta.getMetadata()
                .keySet()
                .stream()
                .map(f -> new org.opensearch.index.store.FileMetadata(f).dataFormat())
                .collect(Collectors.toSet());
            assertTrue("Parquet must be uploaded to remote store", formats.contains("parquet"));
        }, 30, TimeUnit.SECONDS);

        // Batch 2: in translog only (NOT committed, but in remote translog)
        int uncommittedDocs = randomIntBetween(5, 15);
        indexDocs(uncommittedDocs, committedDocs);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        long expectedTotal = committedDocs + uncommittedDocs;

        // Increase replica count and start a new data node — triggers replica recovery
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        // Replica must have all docs: committed segments from remote store + translog replay
        assertBusy(() -> {
            IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX_NAME).clear().setDocs(true).get();
            long primaryDocs = stats.getIndex(INDEX_NAME).getShards()[0].getStats().getDocs().getCount();
            assertEquals("Primary doc count must match expected", expectedTotal, primaryDocs);
        }, 60, TimeUnit.SECONDS);

        // Verify recovery source type: replica recovers via PEER (RemoteStorePeerRecoverySourceHandler
        // downloads segments from remote store on behalf of the replica)
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(INDEX_NAME).get();
        RecoveryState replicaRecovery = recoveryResponse.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No replica recovery state found"));

        assertEquals(
            "Replica must recover via PEER (peer recovery downloads segments from remote store)",
            RecoverySource.Type.PEER,
            replicaRecovery.getRecoverySource().getType()
        );

        // Verify the replica has format files on disk (downloaded from remote store)
        assertBusy(() -> {
            String replicaNode = null;
            String primaryNode = primaryNodeName();
            for (String n : internalCluster().getNodeNames()) {
                if (n.equals(primaryNode) == false && n.equals(internalCluster().getClusterManagerName()) == false) {
                    replicaNode = n;
                    break;
                }
            }
            assertNotNull("Must find replica node", replicaNode);
            IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);
            assertNotNull("Replica shard must exist", replica);

            Path parquetDir = replica.shardPath().getDataPath().resolve("parquet");
            assertTrue("Replica must have parquet directory (downloaded from remote store)", Files.isDirectory(parquetDir));
            try (Stream<Path> files = Files.list(parquetDir)) {
                assertTrue("Replica must have parquet files from remote store", files.findAny().isPresent());
            }
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Full cluster restart with primary + replica: demonstrates that primary recovers
     * locally (fsync'd files) while the replica re-syncs from remote store.
     *
     * <p>After full restart:
     * <ol>
     *   <li>Primary: local recovery from fsync'd committed files + translog replay</li>
     *   <li>Replica: downloads segments from remote store + replays translog</li>
     *   <li>Both shards converge to the same doc count</li>
     * </ol>
     */
    public void testFullRestartPrimaryLocalRecoveryReplicaRemoteStore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(compositeIndexSettings(1)).get();
        ensureGreen(INDEX_NAME);

        // Index and commit data on primary (replicates to replica via segment replication)
        int committedDocs = randomIntBetween(20, 40);
        indexDocs(committedDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Wait for remote store upload
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = primary.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("Remote metadata must exist", meta);
        }, 30, TimeUnit.SECONDS);

        // Index more WITHOUT flush (translog only)
        int uncommittedDocs = randomIntBetween(5, 10);
        indexDocs(uncommittedDocs, committedDocs);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        long expectedTotal = committedDocs + uncommittedDocs;

        // Full cluster restart — both nodes go down simultaneously
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // After restart:
        // - Primary: recovered from local fsync'd files + translog replay
        // - Replica: recovered from remote store segments + translog replay
        long actualDocs = getDocCount();
        // getDocCount returns total across primary + replica; each should have expectedTotal
        // Use per-shard stats to verify
        IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX_NAME).clear().setDocs(true).get();
        for (var shardStats : stats.getIndex(INDEX_NAME).getShards()) {
            assertEquals(
                "Each shard (primary or replica) must have all docs after full restart. "
                    + "Shard routing: "
                    + shardStats.getShardRouting(),
                expectedTotal,
                shardStats.getStats().getDocs().getCount()
            );
        }

        // Verify primary recovery type: must be EXISTING_STORE (local fsync'd committed files)
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(INDEX_NAME).get();
        RecoveryState primaryRecovery = recoveryResponse.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(RecoveryState::getPrimary)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No primary recovery state found"));
        assertEquals(
            "Primary must recover from EXISTING_STORE (local fsync'd files) after full restart",
            RecoverySource.Type.EXISTING_STORE,
            primaryRecovery.getRecoverySource().getType()
        );

        // Verify format files exist on the primary after local recovery
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            Path parquetDir = primary.shardPath().getDataPath().resolve("parquet");
            assertTrue("Primary must have parquet files after local recovery", Files.isDirectory(parquetDir));
            try (Stream<Path> files = Files.list(parquetDir)) {
                assertTrue("Primary parquet files must survive full restart (fsync)", files.findAny().isPresent());
            }
        }, 30, TimeUnit.SECONDS);

        // Verify replica: must be on a different node, have format files, and matching doc count
        RecoveryState replicaRecovery = recoveryResponse.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No replica recovery state found"));

        // Replica recovers via PEER (segment replication from the already-recovered primary)
        assertEquals(
            "Replica must recover via PEER after full restart (segment replication from primary)",
            RecoverySource.Type.PEER,
            replicaRecovery.getRecoverySource().getType()
        );

        // Replica must have format files on disk (received from primary via segment replication)
        String replicaNode = null;
        String primaryNode = primaryNodeName();
        for (String n : internalCluster().getNodeNames()) {
            if (n.equals(primaryNode) == false && n.equals(internalCluster().getClusterManagerName()) == false) {
                replicaNode = n;
                break;
            }
        }
        assertNotNull("Must find replica node", replicaNode);
        assertNotEquals("Replica must be on a different node from primary", primaryNode, replicaNode);

        final String replicaNodeFinal = replicaNode;
        assertBusy(() -> {
            IndexShard replica = getIndexShard(replicaNodeFinal, INDEX_NAME);
            Path parquetDir = replica.shardPath().getDataPath().resolve("parquet");
            assertTrue("Replica must have parquet directory after full restart", Files.isDirectory(parquetDir));
            try (Stream<Path> files = Files.list(parquetDir)) {
                assertTrue("Replica must have parquet files (received via segment replication from primary)", files.findAny().isPresent());
            }
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Verifies that merge-on-refresh produces files that are also fsync'd on commit and
     * survive a node restart. The merged segment must be recoverable locally.
     */
    public void testMergeOnRefreshFilesSurviveRestart() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        Settings settings = Settings.builder()
            .put(compositeIndexSettings(0))
            .put("index.composite.merge_on_refresh_max_size", "10mb")
            .build();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get();
        ensureGreen(INDEX_NAME);

        // Index with multiple threads to trigger merge-on-refresh
        int numThreads = 3;
        int docsPerThread = 10;
        java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(numThreads);
        Thread[] threads = new Thread[numThreads];
        java.util.concurrent.atomic.AtomicInteger failures = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int d = 0; d < docsPerThread; d++) {
                        client().prepareIndex(INDEX_NAME).setSource("name", "t" + threadId + "_d" + d, "value", threadId * 100 + d).get();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, "merge-fsync-indexer-" + t);
            threads[t].start();
        }
        for (Thread t : threads) {
            t.join(30_000);
        }
        assertEquals(0, failures.get());

        int totalDocs = numThreads * docsPerThread;

        // Flush commits + fsyncs all format files (including merged segments)
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        assertEquals("Doc count before restart", totalDocs, getDocCount());

        // Count parquet files before restart
        long parquetFilesBefore;
        IndexShard shardBefore = getIndexShard(primaryNodeName(), INDEX_NAME);
        Path parquetDirBefore = shardBefore.shardPath().getDataPath().resolve("parquet");
        try (Stream<Path> files = Files.list(parquetDirBefore)) {
            parquetFilesBefore = files.filter(p -> p.toString().endsWith(".parquet")).count();
        }
        assertTrue("Must have parquet files after merge-on-refresh + commit", parquetFilesBefore > 0);

        // Restart — local recovery must find the fsync'd merged files
        String nodeName = primaryNodeName();
        internalCluster().restartNode(nodeName);
        ensureGreen(INDEX_NAME);

        assertEquals("Doc count must survive restart", totalDocs, getDocCount());

        // Verify merged parquet files survived restart
        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            Path parquetDir = recovered.shardPath().getDataPath().resolve("parquet");
            assertTrue("Parquet dir must exist after restart", Files.isDirectory(parquetDir));
            try (Stream<Path> files = Files.list(parquetDir)) {
                long count = files.filter(p -> p.toString().endsWith(".parquet")).count();
                assertTrue("Merged parquet files must survive restart (fsync guarantee), found " + count, count > 0);
            }
        }, 30, TimeUnit.SECONDS);
    }
}

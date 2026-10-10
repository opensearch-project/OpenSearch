/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_MODE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Tests a cluster running with {@code node.attr.remote_store.mode: segments_only}, where segments are uploaded to a
 * remote store but the translog stays on local disk and is replicated node-to-node.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentsOnlyRemoteStoreIT extends OpenSearchIntegTestCase {

    private static final String SEGMENT_REPOSITORY_NAME = "test-segment-repo";
    private static final String INDEX_NAME = "segments-only-idx";

    private Path segmentRepoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
        }
        String typeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            SEGMENT_REPOSITORY_NAME
        );
        String settingsPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            SEGMENT_REPOSITORY_NAME
        );
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.attr." + REMOTE_STORE_MODE_KEY, "segments_only")
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, SEGMENT_REPOSITORY_NAME)
            .put(typeKey, ReloadableFsRepository.TYPE)
            .put(settingsPrefix + "location", segmentRepoPath)
            .build();
    }

    private void createSegmentsOnlyIndex(int numReplicas) {
        createSegmentsOnlyIndex(numReplicas, 0);
    }

    private void createSegmentsOnlyIndex(int numReplicas, int numSearchReplicas) {
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, numSearchReplicas)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            )
        );
        ensureGreen(INDEX_NAME);
    }

    /**
     * The segment repository must reach cluster state even though no cluster state repository is configured, otherwise
     * nothing downstream can resolve it by name.
     */
    public void testSegmentRepositoryIsRegisteredInClusterState() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE);
        assertNotNull("segments-only node should publish its repository to cluster state", repositories);
        assertTrue(
            "cluster state should contain " + SEGMENT_REPOSITORY_NAME + " but had " + repositories.repositories(),
            repositories.repositories().stream().anyMatch(r -> SEGMENT_REPOSITORY_NAME.equals(r.name()))
        );
    }

    /**
     * Index creation must stamp the segment repository settings, and must not stamp a translog repository.
     */
    public void testIndexIsCreatedWithSegmentRepositoryOnly() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        createSegmentsOnlyIndex(0);

        GetSettingsResponse settings = client().admin().indices().prepareGetSettings(INDEX_NAME).get();
        assertEquals("true", settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_STORE_ENABLED));
        assertEquals(SEGMENT_REPOSITORY_NAME, settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(
            "segments-only indices must not be given a translog repository",
            settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY)
        );
    }

    /**
     * A write replica must be allocatable, must keep a local translog and must stay part of the replication group so
     * that the global checkpoint reflects it.
     */
    public void testWriteReplicaReceivesOperationsAndBoundsGlobalCheckpoint() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        createSegmentsOnlyIndex(1);

        int docs = randomIntBetween(10, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);
        // Segment replication is asynchronous, so a search served by the replica may briefly lag the primary.
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs), 60, TimeUnit.SECONDS);

        assertBusy(() -> {
            IndexShard primaryShard = null;
            for (String node : dataNodes) {
                IndexService indexService = internalCluster().getInstance(IndicesService.class, node)
                    .indexService(resolveIndex(INDEX_NAME));
                assertNotNull("index should exist on " + node, indexService);
                IndexShard shard = indexService.getShard(0);

                assertFalse("translog must stay local in segments_only mode", shard.indexSettings().isRemoteTranslogStoreEnabled());
                assertTrue("segments must be remote backed", shard.indexSettings().isRemoteStoreEnabled());
                // Every operation reached this shard, so either copy can act as the durable one.
                assertEquals("operations must reach " + node, docs - 1, shard.seqNoStats().getMaxSeqNo());

                if (shard.routingEntry().primary()) {
                    primaryShard = shard;
                }
            }

            assertNotNull(primaryShard);
            assertEquals(
                "the replica must remain in the replication group",
                2,
                primaryShard.getReplicationGroup().getRoutingTable().size()
            );
            assertEquals(
                "a peer recovery retention lease must be retained for the replica",
                2,
                primaryShard.getRetentionLeases().leases().size()
            );
            assertEquals(docs - 1, primaryShard.seqNoStats().getGlobalCheckpoint());
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * The global checkpoint keeps advancing on a replica after its last write, and that advance is only durable once
     * the replica syncs its translog. A remote backed translog can skip the sync because the uploaded copy already
     * covers those operations, but a segments_only replica holds the only copy locally and has to perform it.
     */
    public void testReplicaPersistsTheAdvancingGlobalCheckpoint() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            )
        );
        ensureGreen(INDEX_NAME);

        int docs = randomIntBetween(10, 20);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting replicaRouting = state.routingTable().index(INDEX_NAME).shard(0).replicaShards().get(0);
        String replicaNode = state.nodes().get(replicaRouting.currentNodeId()).getName();
        IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaNode)
            .indexService(resolveIndex(INDEX_NAME))
            .getShard(0);
        assertFalse("precondition: the replica translog is local", replica.indexSettings().hasRemoteTranslog());

        // Writes have stopped, so nothing other than the periodic sync can close the gap.
        assertBusy(
            () -> assertEquals(
                "the replica must persist the global checkpoint it has learned",
                replica.getLastKnownGlobalCheckpoint(),
                replica.getLastSyncedGlobalCheckpoint()
            ),
            30,
            TimeUnit.SECONDS
        );
    }

    /**
     * Killing the primary must not lose acknowledged writes, since the replica holds them in its own translog.
     */
    public void testReplicaPromotionRetainsAcknowledgedWrites() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        createSegmentsOnlyIndex(1);

        int docs = randomIntBetween(10, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs), 60, TimeUnit.SECONDS);

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String primaryNode = state.nodes().get(state.routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId()).getName();
        internalCluster().stopRandomNode(settings -> primaryNode.equals(settings.get("node.name")));

        ensureYellowAndNoInitializingShards(INDEX_NAME);

        // The promoted replica replays its local translog into Lucene, so the writes survive even though some of them
        // may only become searchable after the next refresh.
        assertBusy(() -> {
            refresh(INDEX_NAME);
            assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Balancing primaries only requires the segments to be remote backed, so segments_only clusters must be allowed to
     * use the primary shard limits at both the cluster and the index level.
     */
    public void testTotalPrimaryShardsPerNodeIsAllowed() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        createSegmentsOnlyIndex(1);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2))
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2))
        );

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey()))
        );
    }

    /**
     * A search replica only ever needs segments, which segments_only mode does upload, so it must allocate onto a
     * search node and serve what it pulls from the remote store without holding a translog.
     */
    public void testSearchReplicaServesDocumentsFromRemoteSegmentStore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        String searchNode = internalCluster().startSearchOnlyNode();
        ensureStableCluster(3);
        createSegmentsOnlyIndex(0, 1);

        int docs = randomIntBetween(10, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);

        // Restricting the search to the search node proves the search replica itself served the documents.
        assertBusy(
            () -> assertHitCount(client(searchNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs),
            60,
            TimeUnit.SECONDS
        );

        IndexShard searchShard = internalCluster().getInstance(IndicesService.class, searchNode)
            .indexService(resolveIndex(INDEX_NAME))
            .getShard(0);
        assertTrue(searchShard.routingEntry().isSearchOnly());
        assertTrue("segments must be remote backed", searchShard.indexSettings().isRemoteStoreEnabled());
        assertFalse("translog must stay local in segments_only mode", searchShard.indexSettings().isRemoteTranslogStoreEnabled());
        // This is what makes the shard pull from RemoteStoreReplicationSource rather than from the primary.
        assertTrue(searchShard.indexSettings().isAssignedOnRemoteNode());
        assertEquals("a search replica never receives operations", 0, searchShard.translogStats().estimatedNumberOfOperations());
    }

    /**
     * The buffered translog processor defers each fsync onto the {@code translog_sync} pool so that remote translog
     * uploads can be batched. A segments_only translog is never uploaded, so that deferral buys nothing and only
     * delays the write response by the upload interval. Writes here must sync inline instead, leaving the pool idle.
     */
    public void testLocalTranslogIsSyncedWithoutTheRemoteUploadBuffer() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        createSegmentsOnlyIndex(0);

        IndexShard shard = internalCluster().getInstance(IndicesService.class, dataNode).indexService(resolveIndex(INDEX_NAME)).getShard(0);
        assertTrue("segments must be remote backed", shard.indexSettings().isRemoteStoreEnabled());
        assertFalse("translog must stay local in segments_only mode", shard.indexSettings().isRemoteTranslogStoreEnabled());

        // Writes are issued one at a time with the default REQUEST durability, so each one is separately fsynced and
        // would separately schedule onto the buffer pool.
        int docs = randomIntBetween(10, 20);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);

        // The buffered processor is the only thing in the server that submits to this pool, so a non-zero count means
        // the local fsync was routed through the remote upload buffer.
        assertEquals(
            "a local translog must be synced inline, not deferred onto the remote upload buffer",
            0L,
            completedTranslogSyncTasks(dataNode)
        );
        assertFalse("shard was built with the remote upload buffer", shard.getTranslogSyncProcessor() instanceof BufferedAsyncIOProcessor);
    }

    /**
     * Operations that have been acknowledged but not yet flushed exist only in the local translog, because the remote
     * segment store holds segments and never the operation stream. A primary must therefore recover them from its own
     * translog across a restart rather than rebuilding its commit from the remote store, which would silently drop
     * them. A replica is present so that the primary is restored into a replication group.
     */
    public void testUnflushedWritesSurviveAFullClusterRestart() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        createSegmentsOnlyIndex(1);

        // No refresh or flush, so nothing reaches a Lucene commit and the translog is the only record of these writes.
        int docs = randomIntBetween(20, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // The primary is the copy that had to replay the translog, so pinning the search to it is what proves no
        // acknowledged operation was dropped. The replica reaches the same count only once the primary's recovered
        // segments have been uploaded and a replication checkpoint processed, which is why it is polled separately.
        String primaryNode = primaryNodeName(INDEX_NAME);
        refresh(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs), 60, TimeUnit.SECONDS);
    }

    private long completedTranslogSyncTasks(String node) {
        ThreadPoolStats stats = client().admin()
            .cluster()
            .prepareNodesStats(node)
            .clear()
            .addMetric(NodesStatsRequest.Metric.THREAD_POOL.metricName())
            .get()
            .getNodes()
            .get(0)
            .getThreadPool();
        return StreamSupport.stream(stats.spliterator(), false)
            .filter(s -> ThreadPool.Names.TRANSLOG_SYNC.equals(s.getName()))
            .mapToLong(ThreadPoolStats.Stats::getCompleted)
            .sum();
    }
}

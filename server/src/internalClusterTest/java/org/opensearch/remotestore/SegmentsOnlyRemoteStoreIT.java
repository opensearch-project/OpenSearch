/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

/**
 * The mechanics of a cluster running with {@code node.attr.remote_store.mode: segments_only}: how such a node and
 * index are set up, and how a shard whose segments are remote while its translog is not behaves while it is
 * running. Recovery, snapshots, the translog and the stats APIs each have their own test class alongside this one.
 */
public class SegmentsOnlyRemoteStoreIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX_NAME = "segments-only-idx";

    /**
     * The segment repository must reach cluster state even though no cluster state repository is configured, otherwise
     * nothing downstream can resolve it by name.
     */
    public void testSegmentRepositoryIsRegisteredInClusterState() {
        startCluster(1);

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
        startCluster(1);
        createSegmentsOnlyIndex(INDEX_NAME, 0);

        GetSettingsResponse settings = client().admin().indices().prepareGetSettings(INDEX_NAME).get();
        assertEquals("true", settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_STORE_ENABLED));
        assertEquals(SEGMENT_REPOSITORY_NAME, settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(
            "segments-only indices must not be given a translog repository",
            settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY)
        );
        assertNull(
            "fencing has nothing to write its fence to without a translog repository",
            settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED)
        );
    }

    /**
     * Fencing works by having every translog upload compare-and-swap a per-shard blob in the translog repository.
     * This mode has neither, so asking for it is refused at creation rather than accepted and never acted on. The
     * refusal is what keeps {@code index.remote_store.auto_restore.enabled} out of reach too, since all that
     * setting checks is whether fencing is enabled.
     */
    public void testFencingIsRejectedForASegmentsOnlyIndex() {
        startCluster(1);

        // Not IndexCreationException: that is an OpenSearchWrapperException, so the caller gets the
        // ValidationException it wraps, which is an IllegalArgumentException and, being absent from the
        // exception registry, degrades to exactly that over the transport.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
            ).get()
        );
        assertThat(e.getMessage(), containsString("requires a remote translog repository"));
    }

    /**
     * Auto-restore turns a primary the cluster manager believes unrecoverable into a remote store recovery with no
     * operator action, while the departed primary may still be alive behind a partition. Only the fence stops it
     * acknowledging writes the restored copy will never see, so neither way of asking for it may succeed here.
     */
    public void testAutoRestoreIsUnreachableForASegmentsOnlyIndex() {
        startCluster(1);

        final Settings.Builder base = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REMOTE_STORE_AUTO_RESTORE_ENABLED, true);

        // Without fencing the setting's own validator refuses it.
        IllegalArgumentException withoutFencing = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(INDEX_NAME).setSettings(base).get()
        );
        assertThat(withoutFencing.getMessage(), containsString("can only be enabled when"));

        // With fencing that validator is satisfied, and the fencing refusal catches it instead.
        IllegalArgumentException withFencing = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(base.build()).put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
            ).get()
        );
        assertThat(withFencing.getMessage(), containsString("requires a remote translog repository"));
    }

    /**
     * A write replica must be allocatable, must keep a local translog and must stay part of the replication group so
     * that the global checkpoint reflects it.
     */
    public void testWriteReplicaBoundsGlobalCheckpoint() throws Exception {
        List<String> dataNodes = startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = randomIntBetween(10, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        refresh(INDEX_NAME);
        // Segment replication is asynchronous, so a search served by the replica may briefly lag the primary.
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));

        assertBusy(() -> {
            IndexShard primaryShard = null;
            for (String node : dataNodes) {
                IndexService indexService = internalCluster().getInstance(IndicesService.class, node)
                    .indexService(resolveIndex(INDEX_NAME));
                assertNotNull("index should exist on " + node, indexService);
                IndexShard shard = indexService.getShard(0);

                assertSegmentsOnly(shard);
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
        });
    }

    /**
     * The global checkpoint keeps advancing on a replica after its last write, and that advance is only durable once
     * the replica syncs its translog. A remote backed translog can skip the sync because the uploaded copy already
     * covers those operations, but a segments_only replica holds the only copy locally and has to perform it.
     */
    public void testReplicaPersistsTheAdvancingGlobalCheckpoint() throws Exception {
        startCluster(2);
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
        assertFalse("precondition: the replica translog is local", replica.indexSettings().isTranslogDurableInRemoteStore());

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
     * Balancing primaries only requires the segments to be remote backed, so segments_only clusters must be allowed to
     * use the primary shard limits at both the cluster and the index level.
     */
    public void testTotalPrimaryShardsPerNodeIsAllowed() {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        try {
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
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().putNull(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey()))
            );
        }
    }

    /**
     * Segment replication itself. The search is pinned to the replica copy, because a search without a preference
     * can be answered entirely by the primary and would pass even if the replica received nothing.
     */
    public void testSegmentsAreReplicatedToTheReplicaCopy() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 40);
        refresh(INDEX_NAME);

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting primaryRouting = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
        ShardRouting replicaRouting = state.routingTable().index(INDEX_NAME).shard(0).replicaShards().get(0);
        String primaryNode = state.nodes().get(primaryRouting.currentNodeId()).getName();
        String replicaNode = state.nodes().get(replicaRouting.currentNodeId()).getName();

        // A segment replication replica never builds its own Lucene index out of the operations it was sent, so it
        // can only answer this from segments it received.
        assertBusy(
            () -> assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs),
            60,
            TimeUnit.SECONDS
        );

        IndexShard primary = shardOn(primaryNode, INDEX_NAME);
        IndexShard replica = shardOn(replicaNode, INDEX_NAME);
        assertBusy(() -> {
            Store.RecoveryDiff diff = Store.segmentReplicationDiff(primary.getSegmentMetadataMap(), replica.getSegmentMetadataMap());
            assertTrue("the replica is missing segment files the primary has: " + diff.missing, diff.missing.isEmpty());
            assertTrue("the replica and the primary disagree on segment files: " + diff.different, diff.different.isEmpty());
        });

        // A second round, so this covers steady state replication and not only the initial recovery.
        int total = indexDocs(INDEX_NAME, docs, 20);
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), total),
            60,
            TimeUnit.SECONDS
        );
    }

    /** Shrink, which builds a single-shard index out of an existing one's segments. */
    public void testShrink() throws Exception {
        int docs = createWriteBlockedSourceIndex(2);

        Settings target = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(client().admin().indices().prepareResizeIndex(INDEX_NAME, "shrunk-idx").setSettings(target).get());
        ensureGreen("shrunk-idx");
        assertHitCount(client().prepareSearch("shrunk-idx").setSize(0).get(), docs);
    }

    /** Clone, which copies an existing index's segments into a new index of the same shape. */
    public void testClone() throws Exception {
        int docs = createWriteBlockedSourceIndex(1);

        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(INDEX_NAME, "cloned-idx")
                .setResizeType(ResizeType.CLONE)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build())
                .get()
        );
        ensureGreen("cloned-idx");
        assertHitCount(client().prepareSearch("cloned-idx").setSize(0).get(), docs);
    }

    /** A flushed, write blocked index, which is what a resize operation needs as its source. Returns the doc count. */
    private int createWriteBlockedSourceIndex(int shards) {
        startCluster(1);
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            )
        );
        ensureGreen(INDEX_NAME);

        int docs = indexDocs(INDEX_NAME, 0, 40);
        flush(INDEX_NAME);
        refresh(INDEX_NAME);

        assertAcked(
            client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put("index.blocks.write", true))
        );
        ensureGreen(INDEX_NAME);
        return docs;
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
        createSegmentsOnlyIndex(INDEX_NAME, 0, 1, Settings.EMPTY);

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
        assertSegmentsOnly(searchShard);
        // This is what makes the shard pull from RemoteStoreReplicationSource rather than from the primary.
        assertTrue(searchShard.indexSettings().isAssignedOnRemoteNode());
        assertEquals("a search replica never receives operations", 0, searchShard.translogStats().estimatedNumberOfOperations());
    }
}

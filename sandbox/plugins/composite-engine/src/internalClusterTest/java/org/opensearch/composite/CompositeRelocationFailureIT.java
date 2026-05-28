/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Tests relocation failure scenarios and concurrent indexing with relocation
 * for the composite engine (Parquet + Lucene) with remote store.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeRelocationFailureIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-relocation-failure";

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

    private void createCompositeIndex(int replicas) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(
                    Settings.builder()
                        .put(remoteStoreIndexSettings(replicas, 1))
                        .put("index.pluggable.dataformat.enabled", true)
                        .put("index.pluggable.dataformat", "composite")
                        .put("index.composite.primary_data_format", "parquet")
                        .putList("index.composite.secondary_data_formats", "lucene")
                )
                .setMapping("name", "type=keyword", "value", "type=integer")
        );
        ensureGreen(INDEX_NAME);
    }

    private void indexDocs(int count, int startId) {
        for (int i = startId; i < startId + count; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("name", "doc_" + i, "value", i).get().status()
            );
        }
    }

    /**
     * Concurrent indexing during relocation. All docs acknowledged before relocation
     * starts must survive. Docs during relocation are best-effort.
     */
    public void testConcurrentIndexingDuringRelocation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();

        createCompositeIndex(0);

        // Seed initial data (committed — must survive)
        int initialDocs = randomIntBetween(20, 40);
        indexDocs(initialDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Determine actual allocation
        String sourceNode = primaryNodeName();
        String targetNode = sourceNode.equals(node1) ? node2 : node1;

        long committedRows = getRowCount(sourceNode);

        // Start concurrent indexing in background
        AtomicBoolean stopIndexing = new AtomicBoolean(false);
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch started = new CountDownLatch(1);

        Thread indexer = new Thread(() -> {
            started.countDown();
            int id = 10000;
            while (!stopIndexing.get()) {
                try {
                    client().prepareIndex(INDEX_NAME).setId("concurrent_" + id).setSource("name", "concurrent_" + id, "value", id).get();
                    successCount.incrementAndGet();
                    id++;
                    Thread.sleep(10); // Pace writes to avoid overwhelming during relocation
                } catch (Exception e) {
                    // Rejections during relocation are expected
                    if (stopIndexing.get()) break;
                }
            }
        });
        indexer.setDaemon(true);
        indexer.start();
        assertTrue("Indexer must start", started.await(10, TimeUnit.SECONDS));

        // Small delay to let some concurrent docs through
        Thread.sleep(200);

        // Trigger relocation
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX_NAME, 0, sourceNode, targetNode)).get();

        // Wait for relocation to finish
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
            assertFalse("Should not be relocating", primary.relocating());
            assertTrue("Should be started", primary.started());
        }, 60, TimeUnit.SECONDS);

        // Stop indexing and wait for thread
        stopIndexing.set(true);
        indexer.join(15000);
        assertFalse("Indexer thread did not stop in time", indexer.isAlive());

        // Flush everything
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Verify: at minimum, all pre-relocation committed data survives
        long totalRows = getRowCount(null);
        assertTrue("Must have at least committed rows (" + committedRows + "), got " + totalRows, totalRows >= committedRows);
    }

    /**
     * Multiple sequential relocations — shard moves node1 → node2 → node1.
     * Generation must be monotonically increasing across all hops.
     */
    public void testMultipleSequentialRelocations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();

        createCompositeIndex(0);

        int numDocs = randomIntBetween(20, 40);
        indexDocs(numDocs, 0);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        long maxGenBefore = getMaxGeneration(null);

        // Relocate: current primary → the other node
        String currentPrimary = primaryNodeName();
        String otherNode = currentPrimary.equals(node1) ? node2 : node1;
        relocateAndWait(currentPrimary, otherNode);

        // Index more + flush
        indexDocs(10, 1000);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        long genAfterFirst = getMaxGeneration(null);
        assertTrue(
            "Generation must advance after first relocation + writes (" + genAfterFirst + " > " + maxGenBefore + ")",
            genAfterFirst > maxGenBefore
        );

        // Relocate back
        currentPrimary = primaryNodeName();
        otherNode = currentPrimary.equals(node1) ? node2 : node1;
        relocateAndWait(currentPrimary, otherNode);

        // Index more + flush
        indexDocs(10, 2000);
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        long genAfterSecond = getMaxGeneration(null);
        assertTrue(
            "Generation must advance after second relocation + writes (" + genAfterSecond + " > " + genAfterFirst + ")",
            genAfterSecond > genAfterFirst
        );

        // All data present
        long totalRows = getRowCount(null);
        assertEquals("All docs must survive two relocations", numDocs + 20, totalRows);
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private String primaryNodeName() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
        return state.nodes().get(primary.currentNodeId()).getName();
    }

    private void relocateAndWait(String source, String target) throws Exception {
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX_NAME, 0, source, target)).get();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            ShardRouting primary = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
            assertFalse("Should not be relocating", primary.relocating());
            assertTrue("Should be started", primary.started());
            assertEquals("Primary should be on target", target, state.nodes().get(primary.currentNodeId()).getName());
        }, 60, TimeUnit.SECONDS);
        ensureGreen(INDEX_NAME);
    }

    private long getRowCount(String nodeName) throws Exception {
        IndexShard shard = getShard(nodeName);
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get()
                .getSegments()
                .stream()
                .map(seg -> seg.dfGroupedSearchableFiles().get("parquet"))
                .filter(java.util.Objects::nonNull)
                .mapToLong(WriterFileSet::numRows)
                .sum();
        }
    }

    private long getMaxGeneration(String nodeName) throws Exception {
        IndexShard shard = getShard(nodeName);
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return ref.get().getSegments().stream().mapToLong(Segment::generation).max().orElse(0L);
        }
    }

    private IndexShard getShard(String nodeName) {
        if (nodeName == null) {
            nodeName = primaryNodeName();
        }
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        var indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }
}

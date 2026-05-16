/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Integration test for concurrent indexing across multiple indices and shards
 * with the composite engine (Parquet primary format).
 *
 * <p>Validates that the {@link org.opensearch.index.engine.DataFormatAwareEngine}
 * correctly handles concurrent document writes, refresh, flush, and merge across
 * multiple indices with multiple shards, ensuring:
 * <ul>
 *   <li>No documents are lost under concurrent writes</li>
 *   <li>Segment generations are unique within each shard</li>
 *   <li>Row counts in catalog snapshots match the number of indexed documents</li>
 *   <li>Flush produces valid commit data with catalog snapshots</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeConcurrentIndexingIT" -Dsandbox.enabled=true
 * </pre>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class CompositeConcurrentIndexingIT extends OpenSearchIntegTestCase {

    private static final String[] INDEX_NAMES = { "concurrent-idx-1", "concurrent-idx-2", "concurrent-idx-3" };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            for (String idx : INDEX_NAMES) {
                try {
                    client().admin().indices().prepareDelete(idx).get();
                } catch (Exception e) {
                    // index may not exist
                }
            }
        } finally {
            super.tearDown();
        }
    }

    /**
     * Indexes documents concurrently across multiple indices with multiple shards,
     * then verifies row counts, segment generation uniqueness, and commit integrity.
     */
    public void testConcurrentIndexingAcrossMultipleIndicesAndShards() throws Exception {
        int numShards = randomIntBetween(2, 4);
        int docsPerThread = randomIntBetween(20, 50);
        int threadsPerIndex = randomIntBetween(2, 4);

        // Create indices with multiple shards
        for (String idx : INDEX_NAMES) {
            client().admin()
                .indices()
                .prepareCreate(idx)
                .setSettings(indexSettings(numShards))
                .setMapping("name", "type=keyword", "value", "type=integer")
                .get();
        }
        for (String idx : INDEX_NAMES) {
            ensureGreen(idx);
        }

        // Concurrent indexing across all indices
        int totalThreads = INDEX_NAMES.length * threadsPerIndex;
        CyclicBarrier barrier = new CyclicBarrier(totalThreads);
        AtomicInteger failures = new AtomicInteger(0);
        Thread[] threads = new Thread[totalThreads];

        for (int idxOrd = 0; idxOrd < INDEX_NAMES.length; idxOrd++) {
            String indexName = INDEX_NAMES[idxOrd];
            for (int t = 0; t < threadsPerIndex; t++) {
                int threadId = idxOrd * threadsPerIndex + t;
                threads[threadId] = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int d = 0; d < docsPerThread; d++) {
                            IndexResponse response = client().prepareIndex()
                                .setIndex(indexName)
                                .setSource("name", "thread_" + threadId + "_doc_" + d, "value", randomIntBetween(1, 10000))
                                .get();
                            assertEquals(RestStatus.CREATED, response.status());
                        }
                    } catch (Exception e) {
                        logger.error("Indexing failed in thread " + threadId, e);
                        failures.incrementAndGet();
                    }
                }, "indexer-" + threadId);
                threads[threadId].start();
            }
        }

        for (Thread t : threads) {
            t.join(60_000);
            assertFalse("Thread did not finish in time: " + t.getName(), t.isAlive());
        }
        assertEquals("No indexing threads should have failed", 0, failures.get());

        // Refresh and flush all indices
        for (String idx : INDEX_NAMES) {
            client().admin().indices().prepareRefresh(idx).get();
            client().admin().indices().prepareFlush(idx).setForce(true).setWaitIfOngoing(true).get();
        }

        // Verify each index
        int expectedDocsPerIndex = threadsPerIndex * docsPerThread;
        for (String idx : INDEX_NAMES) {
            verifyIndex(idx, numShards, expectedDocsPerIndex);
        }
    }

    /**
     * Interleaves indexing with refresh and flush operations concurrently.
     */
    public void testInterleavedIndexRefreshFlush() throws Exception {
        String indexName = INDEX_NAMES[0];
        int numShards = 2;
        int totalDocs = randomIntBetween(50, 100);

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(numShards))
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);

        AtomicInteger failures = new AtomicInteger(0);
        AtomicInteger indexedCount = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(4);

        // 2 indexing threads
        Thread indexer1 = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < totalDocs / 2; i++) {
                    client().prepareIndex().setIndex(indexName).setSource("name", "idx1_" + i, "value", i).get();
                    indexedCount.incrementAndGet();
                    if (i % 10 == 0) {
                        Thread.yield();
                    }
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "indexer-1");

        Thread indexer2 = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < totalDocs / 2; i++) {
                    client().prepareIndex().setIndex(indexName).setSource("name", "idx2_" + i, "value", i + 1000).get();
                    indexedCount.incrementAndGet();
                    if (i % 10 == 0) {
                        Thread.yield();
                    }
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "indexer-2");

        // 1 refresh thread
        Thread refresher = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 5; i++) {
                    Thread.sleep(randomIntBetween(50, 200));
                    client().admin().indices().prepareRefresh(indexName).get();
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "refresher");

        // 1 flush thread
        Thread flusher = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 3; i++) {
                    Thread.sleep(randomIntBetween(100, 300));
                    client().admin().indices().prepareFlush(indexName).setForce(false).setWaitIfOngoing(true).get();
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }, "flusher");

        indexer1.start();
        indexer2.start();
        refresher.start();
        flusher.start();

        indexer1.join(60_000);
        indexer2.join(60_000);
        refresher.join(60_000);
        flusher.join(60_000);

        assertEquals("No threads should have failed", 0, failures.get());

        // Final refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        verifyIndex(indexName, numShards, indexedCount.get());
    }

    // ── Helpers ──

    private Settings indexSettings(int numShards) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private void verifyIndex(String indexName, int numShards, int expectedTotalDocs) throws IOException {
        long totalRows = 0;
        Set<Long> allGenerations = new HashSet<>();

        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).clear().setStore(true).get();
        ShardStats[] shardStats = statsResponse.getIndex(indexName).getShards();

        for (ShardStats ss : shardStats) {
            if (ss.getShardRouting().primary() == false) {
                continue;
            }
            CommitStats commitStats = ss.getCommitStats();
            assertNotNull("Commit stats should exist for shard " + ss.getShardRouting().shardId(), commitStats);

            String snapshotKey = DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY;
            assertTrue(
                "Commit data should contain catalog snapshot for shard " + ss.getShardRouting().shardId(),
                commitStats.getUserData().containsKey(snapshotKey)
            );

            DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
                commitStats.getUserData().get(snapshotKey),
                Function.identity()
            );

            // Verify segment generation uniqueness within this shard
            Set<Long> shardGenerations = new HashSet<>();
            for (Segment seg : snapshot.getSegments()) {
                assertTrue(
                    "Duplicate generation " + seg.generation() + " in shard " + ss.getShardRouting().shardId(),
                    shardGenerations.add(seg.generation())
                );
                // Every segment must have files
                assertFalse("Segment " + seg.generation() + " has no files", seg.dfGroupedSearchableFiles().isEmpty());
                for (WriterFileSet wfs : seg.dfGroupedSearchableFiles().values()) {
                    assertTrue("WriterFileSet must have positive row count", wfs.numRows() > 0);
                    assertFalse("WriterFileSet must have files", wfs.files().isEmpty());
                    totalRows += wfs.numRows();
                }
            }
            allGenerations.addAll(shardGenerations);
        }

        assertEquals("Total rows across all shards of " + indexName + " must match indexed docs", expectedTotalDocs, totalRows);
    }
}

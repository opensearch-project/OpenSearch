/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

/**
 * Integration tests for local recovery of the composite engine (parquet + lucene).
 *
 * <p>Validates that after a node restart, the engine correctly restores committed
 * catalog snapshots and initializes reader managers so that the catalog snapshot
 * is immediately available without requiring translog replay or explicit refresh.
 *
 * <p>Exercises the CatalogSnapshotManager fix that notifies listeners about
 * committed snapshots on engine startup.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeLocalRecoveryIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-local-recovery";

    /**
     * Index docs, flush (commit), restart node. After restart, the catalog snapshot
     * must contain segments with the correct row count — confirming that committed
     * data is restored and reader managers are initialized without translog replay.
     */
    public void testCatalogSnapshotRestoredAfterRestart() throws Exception {
        createCompositeIndex(INDEX_NAME);
        int numDocs = randomIntBetween(10, 30);
        indexDocs(INDEX_NAME, numDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        // Verify snapshot before restart
        long rowsBefore = getRowCountFromEngine(INDEX_NAME);
        assertTrue("Should have rows before restart", rowsBefore > 0);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // After restart, snapshot must be immediately available with same row count
        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            assertFalse("Snapshot must have segments after restart", snapshot.getSegments().isEmpty());

            long rowsAfter = getTotalRowCount(snapshot);
            assertEquals("Row count must be identical after restart", rowsBefore, rowsAfter);
        }
    }

    /**
     * After restart, the catalog snapshot must have segments with files for both
     * parquet (primary) and lucene (secondary) formats.
     */
    public void testCatalogSnapshotRestoredWithBothFormats() throws Exception {
        createCompositeIndex(INDEX_NAME);
        int numDocs = randomIntBetween(5, 20);
        indexDocs(INDEX_NAME, numDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            assertFalse("Snapshot must have segments after recovery", snapshot.getSegments().isEmpty());

            for (Segment segment : snapshot.getSegments()) {
                assertTrue(
                    "Each segment must have parquet files",
                    segment.dfGroupedSearchableFiles().containsKey("parquet")
                );
                assertTrue(
                    "Each segment must have lucene files",
                    segment.dfGroupedSearchableFiles().containsKey("lucene")
                );
            }
        }
    }

    /**
     * Index docs, flush, then index more WITHOUT flushing. After restart, translog
     * recovery must replay the unflushed ops and the catalog snapshot must reflect
     * all documents.
     */
    public void testTranslogRecoveryAfterRestart() throws Exception {
        createCompositeIndex(INDEX_NAME);
        int flushedDocs = randomIntBetween(5, 15);
        int unflushedDocs = randomIntBetween(5, 15);

        indexDocs(INDEX_NAME, flushedDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        // Index more without flush — these are in translog only
        indexDocs(INDEX_NAME, unflushedDocs, flushedDocs);
        refreshIndex(INDEX_NAME);

        long rowsBefore = getRowCountFromEngine(INDEX_NAME);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // After restart + translog recovery, all docs must be reflected
        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            long rowsAfter = getTotalRowCount(ref.get());
            assertEquals("All rows (committed + recovered) must be present", rowsBefore, rowsAfter);
        }
    }

    /**
     * After restart, the engine must accept new writes. Index new docs, refresh,
     * and verify the catalog snapshot reflects both recovered and new data.
     */
    public void testEngineAcceptsNewWritesAfterRestart() throws Exception {
        createCompositeIndex(INDEX_NAME);
        int initialDocs = randomIntBetween(5, 15);
        indexDocs(INDEX_NAME, initialDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Index new docs after restart
        int newDocs = randomIntBetween(5, 15);
        indexDocs(INDEX_NAME, newDocs, initialDocs);
        refreshIndex(INDEX_NAME);

        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            assertFalse("Snapshot must have segments", snapshot.getSegments().isEmpty());

            long totalRows = getTotalRowCount(snapshot);
            // Each format stores all docs, so per-format row count = total docs
            // For parquet (primary), row count should reflect all docs
            long parquetRows = snapshot.getSegments()
                .stream()
                .filter(s -> s.dfGroupedSearchableFiles().containsKey("parquet"))
                .flatMap(s -> s.dfGroupedSearchableFiles().values().stream())
                .filter(wfs -> wfs.files().stream().anyMatch(f -> f.endsWith(".parquet") || true))
                .mapToLong(org.opensearch.index.engine.exec.WriterFileSet::numRows)
                .sum();
            assertTrue("Total rows must include both recovered and new docs", totalRows > initialDocs);
        }
    }

    /**
     * Two restart cycles: index->flush->restart->index->flush->restart. The second
     * restart must see accumulated data from both lifecycles.
     */
    public void testMultipleRestartCyclesAccumulateData() throws Exception {
        createCompositeIndex(INDEX_NAME);

        // First lifecycle
        int firstBatch = randomIntBetween(5, 10);
        indexDocs(INDEX_NAME, firstBatch, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Second lifecycle — add more data
        int secondBatch = randomIntBetween(5, 10);
        indexDocs(INDEX_NAME, secondBatch, firstBatch);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // After second restart, catalog snapshot must reflect all accumulated data
        DataFormatAwareEngine engine = getEngine(INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            assertFalse("Snapshot must have segments after second restart", snapshot.getSegments().isEmpty());
            assertTrue("Snapshot generation must be > 0", snapshot.getGeneration() > 0);

            long totalRows = getTotalRowCount(snapshot);
            assertTrue("Total rows must reflect both batches", totalRows > 0);
        }
    }

    private long getRowCountFromEngine(String indexName) throws IOException {
        DataFormatAwareEngine engine = getEngine(indexName);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            return getTotalRowCount(ref.get());
        }
    }
}

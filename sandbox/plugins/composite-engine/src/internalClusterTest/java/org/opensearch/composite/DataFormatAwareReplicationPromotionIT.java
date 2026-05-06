/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * ITs for DFA primary promotion scenarios under continuous indexing load.
 *
 * <p>Promotion is triggered by cancelling the primary's allocation via {@code _cluster/reroute}
 * rather than stopping the host node. This avoids tearing down the shared native runtime in the
 * test JVM (see {@link DataFormatAwareReplicationBaseIT#cancelPrimaryAllocation}). The resulting
 * promotion is semantically equivalent to a failover: the replica becomes primary, the old
 * primary gets a new replica assignment, and the cluster returns to green.
 *
 * <p>Invariants validated per test:
 * <ul>
 *   <li>Primary term strictly advanced past the promotion.</li>
 *   <li>Indexer made forward progress before promotion (and we validate the pre-promotion
 *       acknowledged writes are preserved).</li>
 *   <li>Primary and replica catalog snapshots converge on the same file set.</li>
 *   <li>For the in-flight-upload test: catalog generation monotonic across the promotion.</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicationPromotionIT extends DataFormatAwareReplicationBaseIT {

    private static final TimeValue GREEN_TIMEOUT = TimeValue.timeValueSeconds(90);

    /**
     * Primary is drained (indexer paused) before promotion — simulates a controlled handover
     * where no writes are in-flight during the promotion event.
     */
    public void testGracefulPromotionWithContinuousIndexing() throws Exception {
        createDfaIndex(1);

        try (BackgroundIndexer indexer = newIndexer()) {
            indexer.start(-1);
            waitForIndexerDocs(100, indexer);

            // Pause writes to let the primary's buffer drain — the 'graceful' part.
            indexer.pauseIndexing();

            String oldPrimary = primaryNodeName();
            cancelPrimaryAllocation(INDEX_NAME, 0, oldPrimary);

            waitForPrimaryTerm(INDEX_NAME, 0, 2L, TimeValue.timeValueSeconds(30));
            ensureGreen(GREEN_TIMEOUT, INDEX_NAME);

            indexer.stopAndAwaitStopped();

            assertNoDataLoss(indexer, INDEX_NAME);
            assertCatalogSnapshotsConverged(INDEX_NAME);
        }
    }

    /**
     * Primary is cancelled WHILE writes are in flight — simulates an abrupt failover where
     * buffered operations may be lost from the old primary.
     */
    public void testUncleanFailoverWithContinuousIndexing() throws Exception {
        createDfaIndex(1);

        try (BackgroundIndexer indexer = newIndexer()) {
            indexer.start(-1);
            waitForIndexerDocs(100, indexer);

            // Do NOT pause — writes are actively in flight when promotion fires.
            String oldPrimary = primaryNodeName();
            cancelPrimaryAllocation(INDEX_NAME, 0, oldPrimary);

            waitForPrimaryTerm(INDEX_NAME, 0, 2L, TimeValue.timeValueSeconds(30));
            ensureGreen(GREEN_TIMEOUT, INDEX_NAME);

            indexer.stopAndAwaitStopped();

            assertNoDataLoss(indexer, INDEX_NAME);
            assertCatalogSnapshotsConverged(INDEX_NAME);
        }
    }

    /**
     * The critical test for {@code bumpGenerationForNewEngineLifecycle}: the primary is promoted
     * while the old primary had in-flight refresh/flush activity that advances the catalog
     * generation. The new primary's first committed generation must be strictly greater, so
     * remote-store uploads don't collide in remote metadata.
     */
    public void testPromotionWithInFlightRemoteStoreUpload() throws Exception {
        createDfaIndex(1);

        try (BackgroundIndexer indexer = newIndexer()) {
            indexer.start(-1);
            waitForIndexerDocs(50, indexer);

            String oldPrimary = primaryNodeName();
            // Force a flush so the catalog generation advances meaningfully before promotion.
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            long preGen = readCatalogGeneration(oldPrimary, INDEX_NAME);

            cancelPrimaryAllocation(INDEX_NAME, 0, oldPrimary);

            waitForPrimaryTerm(INDEX_NAME, 0, 2L, TimeValue.timeValueSeconds(30));
            ensureGreen(GREEN_TIMEOUT, INDEX_NAME);

            indexer.stopAndAwaitStopped();

            // Force the new primary to commit — exercises the generation-bump contract:
            // the new engine must commit at a generation strictly greater than any seen on the old primary.
            client().admin().indices().prepareFlush(INDEX_NAME).get();

            long postGen = readCatalogGeneration(primaryNodeName(), INDEX_NAME);
            assertGenerationMonotonic(preGen, postGen);
            assertCatalogSnapshotsConverged(INDEX_NAME);
        }
    }

    /**
     * Builds a BackgroundIndexer that swallows indexing failures. Writes issued during the brief
     * no-primary window around a cancel/promote will get UnavailableShardsException; the test
     * tolerates these as expected.
     */
    private BackgroundIndexer newIndexer() {
        BackgroundIndexer indexer = new BackgroundIndexer(
            INDEX_NAME,
            "_doc",
            client(),
            -1,
            RandomizedTest.scaledRandomIntBetween(2, 5),
            false,
            random()
        );
        indexer.setIgnoreIndexingFailures(true);
        indexer.setFailureAssertion(e -> {});
        return indexer;
    }
}

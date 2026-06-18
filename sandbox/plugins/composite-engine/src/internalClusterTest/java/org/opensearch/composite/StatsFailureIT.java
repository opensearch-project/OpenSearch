/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for stats tracker lifecycle around DFA engine failure.
 *
 * <p>Uses {@link FailableLuceneDataFormatPlugin} to inject a Directory-level fault that makes
 * the IndexWriter tragic, fails the engine, and forces a shard recovery. The test then verifies
 * that the per-format provider registry properly unregisters the dead tracker and registers a
 * fresh one for the new engine, with all failure counters at zero on the recovered tracker and
 * subsequent indexing tracked correctly.
 *
 * <p>Note: this test does NOT assert that {@code docs_indexed_failures} ticks during the failed
 * indexing operations. The Directory-level fault makes the engine tragic before all per-doc
 * failures necessarily flow through the {@code LuceneWriter.addDoc()} catch block, so a
 * deterministic assertion on the failure-counter increment is racy. The lifecycle invariants
 * tested here are the higher-value safety net: orphan trackers and double-counting bugs would
 * surface as non-zero counters on the post-recovery tracker.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class StatsFailureIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Swap LucenePlugin out for FailableLuceneDataFormatPlugin so we can inject failures.
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            FailableLuceneDataFormatPlugin.class,
            DataFusionPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FailableLuceneDataFormatPlugin.clearFailure();
    }

    @Override
    public void tearDown() throws Exception {
        FailableLuceneDataFormatPlugin.clearFailure();
        super.tearDown();
    }

    /**
     * Verifies stats tracker lifecycle around engine failure and recovery:
     * 1. Happy path: all *_failures counters are 0 after successful indexing.
     * 2. Inject a Directory-level fault that causes the engine to fail.
     * 3. After shard recovery, stats endpoint is functional and failure counters are 0
     *    (proving the old tracker was properly unregistered and a fresh one registered).
     * 4. New indexing after recovery is tracked correctly.
     */
    @AwaitsFix(bugUrl = "Intermittent deadlock: after the injected engine failure, a scheduled refresh thread parks "
        + "permanently in LockablePool.checkoutAll (blocking ReentrantLock.lock with no timeout) because a "
        + "writer lock is left held on the engine-failure path. The shard never recovers, so ensureGreen times "
        + "out. Root cause is in DataFormatAwareEngine.refresh / LockablePool (server), not in stats. "
        + "Re-enable once checkoutAll uses a bounded tryLock or the failure path releases writer locks.")
    public void testTrackerLifecycleAroundEngineFailure() throws Exception {
        // Start a single node to minimize interference with the JVM-wide failure countdown.
        internalCluster().startNode();
        String idx = "failure-idx";
        createCompositeIndex(idx, true);

        // Stage 1 — happy path: index 50 docs, refresh, assert all *_failures == 0.
        indexDocs(idx, 50, 0);
        refreshIndex(idx);
        Map<String, Object> happyP = StatsITHelpers.parquetIndexStats(getRestClient(), idx);
        Map<String, Object> happyL = StatsITHelpers.luceneIndexStats(getRestClient(), idx);
        StatsITHelpers.assertCounter(
            "happy parquet native_write_failures",
            happyP,
            "indices." + idx + ".native_write.native_write_failures",
            0L
        );
        StatsITHelpers.assertCounter("happy parquet merge_failures", happyP, "indices." + idx + ".merge.merge_failures", 0L);
        StatsITHelpers.assertCounter(
            "happy lucene docs_indexed_failures",
            happyL,
            "indices." + idx + ".indexing.docs_indexed_failures",
            0L
        );
        StatsITHelpers.assertCounter("happy lucene merge_failures", happyL, "indices." + idx + ".merge.merge_failures", 0L);

        // Stage 2 — inject a Directory-level fault. The IOException during flush makes the
        // IndexWriter tragic, which fails the engine. The shard is then reallocated and
        // recovered on the same node.
        FailableLuceneDataFormatPlugin.failOnNthWrite(3);
        for (int i = 50; i < 70; i++) {
            try {
                client().prepareIndex(idx).setId(String.valueOf(i)).setSource("name", "injection_" + i, "value", i).get();
            } catch (Exception expected) {
                // Engine failure causes indexing exceptions — expected.
                break;
            }
        }
        FailableLuceneDataFormatPlugin.clearFailure();

        // Stage 3 — wait for shard recovery. After the engine fails, the shard is reallocated.
        // A new engine is created with a fresh stats tracker.
        assertBusy(() -> ensureGreen(idx), 30, TimeUnit.SECONDS);

        // Stage 4 — after recovery, stats endpoint must be functional with clean counters.
        // The old tracker (which may have seen the failure) was unregistered on engine close;
        // the new tracker starts fresh.
        Map<String, Object> postRecoveryL = StatsITHelpers.luceneIndexStats(getRestClient(), idx);
        StatsITHelpers.assertCounter(
            "post-recovery lucene docs_indexed_failures must be 0",
            postRecoveryL,
            "indices." + idx + ".indexing.docs_indexed_failures",
            0L
        );
        StatsITHelpers.assertCounter(
            "post-recovery lucene merge_failures must be 0",
            postRecoveryL,
            "indices." + idx + ".merge.merge_failures",
            0L
        );

        // Stage 5 — new indexing after recovery is tracked correctly.
        indexDocs(idx, 20, 100);
        refreshIndex(idx);
        Map<String, Object> postIndexL = StatsITHelpers.luceneIndexStats(getRestClient(), idx);
        StatsITHelpers.assertCounterAtLeast(
            "post-recovery new docs tracked",
            postIndexL,
            "indices." + idx + ".indexing.docs_indexed_total",
            20L
        );
        Map<String, Object> postIndexP = StatsITHelpers.parquetIndexStats(getRestClient(), idx);
        StatsITHelpers.assertCounterAtLeast(
            "post-recovery parquet new docs tracked",
            postIndexP,
            "indices." + idx + ".indexing.docs_indexed_total",
            20L
        );
    }
}

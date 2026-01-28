/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.opensearch.cluster.metadata.IndexMetadata;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

public class MergeSchedulerTests extends OpenSearchTestCase {

    private MergeScheduler mergeScheduler;
    private MergeHandler mergeHandler;
    private CompositeEngine compositeEngine;
    private ShardId shardId;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        shardId = new ShardId(new Index("test", "uuid"), 0);
        mergeHandler = mock(MergeHandler.class);
        compositeEngine = mock(CompositeEngine.class);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 2)
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 5)
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test")
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            settings
        );

        mergeScheduler = new MergeScheduler(mergeHandler, compositeEngine, shardId, indexSettings);
    }

    public void testRefreshConfig() {
        Settings newSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), 4)
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), 10)
            .build();

        IndexSettings newIndexSettings = new IndexSettings(
            IndexMetadata.builder("test")
                .settings(newSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            newSettings
        );

        MergeScheduler newMergeScheduler = new MergeScheduler(mergeHandler, compositeEngine, shardId, newIndexSettings);

        assertEquals(4, newMergeScheduler.getMaxConcurrentMerges());
        assertEquals(10, newMergeScheduler.getMaxMergeCount());
    }

    public void testTriggerMergesWithNoPendingMerges() {
        when(mergeHandler.hasPendingMerges()).thenReturn(false);

        mergeScheduler.triggerMerges();

        verify(mergeHandler, times(1)).updatePendingMerges();
        verify(mergeHandler, never()).getNextMerge();
    }

    public void testTriggerMergesWithPendingMerges() throws Exception {
        OneMerge merge = mock(OneMerge.class);
        MergeResult mergeResult = mock(MergeResult.class);

        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch allowMergeToFinish = new CountDownLatch(1);

        when(mergeHandler.hasPendingMerges()).thenReturn(true, false);
        when(mergeHandler.getNextMerge()).thenReturn(merge);

        when(mergeHandler.doMerge(merge)).thenAnswer(invocation -> {
            // Signal that merge thread has started
            mergeStarted.countDown();

            // Block merge thread until test allows it to proceed
            assertTrue("merge did not start in time",
                    allowMergeToFinish.await(5, TimeUnit.SECONDS));

            return mergeResult;
        });

        // Trigger merge
        mergeScheduler.triggerMerges();

        // Wait until merge thread is definitely running
        assertTrue("merge thread never started",
                mergeStarted.await(5, TimeUnit.SECONDS));

        // Now the merge must be active
        assertEquals(1, mergeScheduler.getActiveMergeCount());

        // Validate interactions so far
        verify(mergeHandler, times(1)).updatePendingMerges();
        verify(mergeHandler, times(1)).getNextMerge();

        // Allow merge thread to complete
        allowMergeToFinish.countDown();

        // Wait until merge completes
        assertBusy(() -> assertEquals(0, mergeScheduler.getActiveMergeCount()));

        // Verify completion callbacks
        verify(mergeHandler, times(1)).onMergeFinished(merge);
        verify(compositeEngine, times(1)).applyMergeChanges(mergeResult, merge);
    }


    public void testForceMergeSuccess() throws IOException {
        OneMerge merge = mock(OneMerge.class);
        MergeResult mergeResult = mock(MergeResult.class);

        when(mergeHandler.findForceMerges(1)).thenReturn(Arrays.asList(merge));
        when(mergeHandler.doMerge(merge)).thenReturn(mergeResult);

        mergeScheduler.forceMerge(1);

        verify(mergeHandler, times(1)).findForceMerges(1);
        verify(mergeHandler, times(1)).doMerge(merge);
        verify(compositeEngine, times(1)).applyMergeChanges(mergeResult, merge);
    }

    public void testForceMergeWithActiveMerges() throws Exception {
        OneMerge merge = mock(OneMerge.class);
        MergeResult mergeResult = mock(MergeResult.class);

        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch allowMergeToFinish = new CountDownLatch(1);

        when(mergeHandler.hasPendingMerges()).thenReturn(true, false);
        when(mergeHandler.getNextMerge()).thenReturn(merge);

        when(mergeHandler.doMerge(merge)).thenAnswer(invocation -> {
            // Signal that merge has started
            mergeStarted.countDown();

            // Block merge so it stays active while forceMerge is called
            assertTrue("merge did not start in time",
                    allowMergeToFinish.await(5, TimeUnit.SECONDS));

            return mergeResult;
        });

        // Start background merge
        mergeScheduler.triggerMerges();

        // Wait until merge thread is guaranteed running
        assertTrue("merge thread never started",
                mergeStarted.await(5, TimeUnit.SECONDS));

        // Sanity check: merge is active
        assertEquals(1, mergeScheduler.getActiveMergeCount());

        // Now forceMerge must fail because a merge is active
        IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> mergeScheduler.forceMerge(1)
        );

        assertEquals("Cannot force merge while background merges are active",
                exception.getMessage());

        // Cleanup: allow merge thread to finish
        allowMergeToFinish.countDown();

        // Wait until merge finishes
        assertBusy(() -> assertEquals(0, mergeScheduler.getActiveMergeCount()));
    }


    public void testShutdown() throws Exception {
        OneMerge merge = mock(OneMerge.class);
        MergeResult mergeResult = mock(MergeResult.class);

        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch allowMergeToFinish = new CountDownLatch(1);

        when(mergeHandler.hasPendingMerges()).thenReturn(true, false);
        when(mergeHandler.getNextMerge()).thenReturn(merge);

        when(mergeHandler.doMerge(merge)).thenAnswer(invocation -> {
            // Signal merge thread has started
            mergeStarted.countDown();

            // Block until shutdown allows completion
            assertTrue("merge did not start in time",
                    allowMergeToFinish.await(5, TimeUnit.SECONDS));

            return mergeResult;
        });

        // Start merge
        mergeScheduler.triggerMerges();

        // Ensure merge is actually running
        assertTrue("merge thread never started",
                mergeStarted.await(5, TimeUnit.SECONDS));
        assertEquals(1, mergeScheduler.getActiveMergeCount());
        assertFalse(mergeScheduler.isShutdown());

        // Call shutdown while merge is still active
        // Shutdown should wait for merge thread to finish
        Thread shutdownThread = new Thread(() -> mergeScheduler.shutdown());
        shutdownThread.start();

        // Give shutdown a moment to block on merge thread
        assertBusy(() -> assertTrue(mergeScheduler.isShutdown()));

        // Merge should still be active because we haven't released it yet
        assertEquals(1, mergeScheduler.getActiveMergeCount());

        // Allow merge to finish so shutdown can complete
        allowMergeToFinish.countDown();

        // Wait until shutdown completes and merge finishes
        shutdownThread.join(5_000);

        assertEquals(0, mergeScheduler.getActiveMergeCount());
        assertTrue(mergeScheduler.isShutdown());
    }


    public void testTriggerMergesAfterShutdown() {
        mergeScheduler.shutdown();

        mergeScheduler.triggerMerges();

        verify(mergeHandler, never()).updatePendingMerges();
    }

    public void testGetActiveMergeCount() {
        assertEquals(0, mergeScheduler.getActiveMergeCount());
    }

    public void testGetMaxConcurrentMerges() {
        assertEquals(2, mergeScheduler.getMaxConcurrentMerges());
    }

    public void testGetMaxMergeCount() {
        assertEquals(5, mergeScheduler.getMaxMergeCount());
    }
}

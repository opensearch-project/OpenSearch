/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the tiering freeze semantics of {@link MergeScheduler}: when frozen, no new merges
 * may be triggered or force-merged, and the frozen state is derived from both an explicit
 * {@link MergeScheduler#freeze()} and the index's {@code INDEX_TIERING_STATE} setting.
 */
public class MergeSchedulerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private IndexSettings indexSettings(IndexModule.TieringState tieringState) {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexModule.INDEX_TIERING_STATE.getKey(), tieringState.name())
                .build()
        );
    }

    private MergeScheduler newScheduler(MergeHandler mergeHandler, IndexModule.TieringState tieringState) {
        return new MergeScheduler(
            mergeHandler,
            (result, merge) -> {},
            () -> {},
            () -> {},
            () -> {},
            shardId,
            indexSettings(tieringState),
            threadPool
        );
    }

    public void testFreezeBlocksTriggerMerges() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();
        assertTrue("scheduler must report frozen after freeze()", scheduler.isFrozen());

        scheduler.triggerMerges();
        // Frozen: the scheduler must not even ask the handler to find/register merges.
        verify(mergeHandler, never()).findAndRegisterMerges();
    }

    public void testTriggerMergesWhenNotFrozenInvokesHandler() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        assertFalse("scheduler must not be frozen for a HOT index", scheduler.isFrozen());
        scheduler.triggerMerges();
        verify(mergeHandler, times(1)).findAndRegisterMerges();
    }

    public void testUnfreezeAllowsMergesAgain() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();
        scheduler.triggerMerges();
        verify(mergeHandler, never()).findAndRegisterMerges();

        scheduler.unfreeze();
        assertFalse("scheduler must report unfrozen after unfreeze()", scheduler.isFrozen());
        scheduler.triggerMerges();
        // unfreeze() itself fires triggerMerges() on a real frozen→unfrozen transition (resumes merges
        // without needing a follow-up call), and the explicit triggerMerges() above adds a second
        // invocation. Hence findAndRegisterMerges is invoked twice.
        verify(mergeHandler, times(2)).findAndRegisterMerges();
    }

    public void testIsFrozenReflectsHotToWarmTieringState() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT_TO_WARM);
        assertTrue("HOT_TO_WARM tiering state must report frozen", scheduler.isFrozen());
    }

    public void testIsNotFrozenForHotTieringState() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);
        assertFalse("HOT tiering state must not report frozen", scheduler.isFrozen());
    }

    public void testForceMergeSkipsAfterShutdown() throws IOException {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.shutdown();

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        verify(mergeHandler, never()).findForceMerges(anyInt());
    }

    public void testForceMergeAbortsRemainingMergesOnShutdown() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);

        Segment s1 = new Segment(1L, Map.of());
        Segment s2 = new Segment(2L, Map.of());
        OneMerge merge1 = new OneMerge(List.of(s1));
        OneMerge merge2 = new OneMerge(List.of(s2));

        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge1, merge2));
        when(mergeHandler.doMerge(merge1)).thenReturn(new MergeResult(Map.of()));

        final java.util.concurrent.atomic.AtomicReference<MergeScheduler> schedulerRef =
            new java.util.concurrent.atomic.AtomicReference<>();

        MergeScheduler scheduler = new MergeScheduler(
            mergeHandler,
            (result, merge) -> { schedulerRef.get().shutdown(); },
            () -> {},
            () -> {},
            () -> {},
            shardId,
            indexSettings(IndexModule.TieringState.HOT),
            threadPool
        );
        schedulerRef.set(scheduler);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        verify(mergeHandler).doMerge(merge1);
        verify(mergeHandler, never()).doMerge(merge2);
    }

    public void testThrottlingActivatesWhenMergesExceedMaxCount() throws Exception {
        AtomicInteger activateCount = new AtomicInteger();
        AtomicInteger deactivateCount = new AtomicInteger();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "2")
                .build()
        );

        MergeHandler mergeHandler = mock(MergeHandler.class);
        OneMerge merge1 = mock(OneMerge.class);
        OneMerge merge2 = mock(OneMerge.class);
        OneMerge merge3 = mock(OneMerge.class);
        when(merge1.getSegmentsToMerge()).thenReturn(List.of());
        when(merge2.getSegmentsToMerge()).thenReturn(List.of());
        when(merge3.getSegmentsToMerge()).thenReturn(List.of());

        when(mergeHandler.getPendingMergeCount()).thenReturn(3, 3, 2, 1, 0);
        when(mergeHandler.hasPendingMerges()).thenReturn(true, true, true, false);
        when(mergeHandler.getNextMerge()).thenReturn(merge1).thenReturn(merge2).thenReturn(merge3).thenReturn(null);
        when(mergeHandler.doMerge(any())).thenReturn(new MergeResult(Map.of()));

        MergeScheduler scheduler = new MergeScheduler(
            mergeHandler,
            (result, merge) -> {},
            () -> {},
            activateCount::incrementAndGet,
            deactivateCount::incrementAndGet,
            shardId,
            idxSettings,
            threadPool
        );

        scheduler.triggerMerges();
        assertBusy(() -> assertTrue("throttle should have activated", activateCount.get() > 0));
        assertBusy(() -> assertTrue("throttle should have deactivated", deactivateCount.get() > 0));
    }

    public void testThrottlingNotActivatedWhenMergesWithinLimit() throws Exception {
        AtomicInteger activateCount = new AtomicInteger();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
                .build()
        );

        MergeHandler mergeHandler = mock(MergeHandler.class);
        OneMerge merge1 = mock(OneMerge.class);
        when(merge1.getSegmentsToMerge()).thenReturn(List.of());
        when(mergeHandler.getPendingMergeCount()).thenReturn(1, 0);
        when(mergeHandler.hasPendingMerges()).thenReturn(true, false);
        when(mergeHandler.getNextMerge()).thenReturn(merge1).thenReturn(null);
        when(mergeHandler.doMerge(any())).thenReturn(new MergeResult(Map.of()));

        MergeScheduler scheduler = new MergeScheduler(
            mergeHandler,
            (result, merge) -> {},
            () -> {},
            activateCount::incrementAndGet,
            () -> {},
            shardId,
            idxSettings,
            threadPool
        );

        scheduler.triggerMerges();
        Thread.sleep(200);
        assertEquals("throttle should not activate when merges within limit", 0, activateCount.get());
    }
}

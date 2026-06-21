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
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        return new MergeScheduler(mergeHandler, (result, merge) -> {}, () -> {}, shardId, indexSettings(tieringState), threadPool);
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

    public void testForceMergeUnfreezesOnSuccess() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        Segment s1 = new Segment(1L, Map.of());
        OneMerge merge = new OneMerge(List.of(s1));

        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge));
        when(mergeHandler.doMerge(merge)).thenReturn(new MergeResult(Map.of()));

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);
        assertFalse("Should not be frozen before force merge", scheduler.isFrozen());

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        assertFalse("Must be unfrozen after successful force merge", scheduler.isFrozen());
    }

    public void testForceMergeUnfreezesOnFailure() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        Segment s1 = new Segment(1L, Map.of());
        OneMerge merge = new OneMerge(List.of(s1));

        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge));
        when(mergeHandler.doMerge(merge)).thenThrow(new IOException("simulated merge failure"));

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            expectThrows(IOException.class, () -> scheduler.forceMerge(1));
        } finally {
            Thread.currentThread().setName(oldName);
        }

        assertFalse("Must be unfrozen even after force merge failure", scheduler.isFrozen());
    }

    public void testForceMergePreservesExternalFreezeState() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.findForceMerges(1)).thenReturn(List.of());

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        // Externally frozen (e.g., by tiering)
        scheduler.freeze();
        assertTrue("Should be frozen", scheduler.isFrozen());

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        // Must remain frozen — force merge should NOT unfreeze externally-frozen state
        assertTrue("Must remain frozen when externally frozen before force merge", scheduler.isFrozen());
    }

    public void testForceMergeCleansUpRemainingMergesOnException() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        Segment s1 = new Segment(1L, Map.of());
        Segment s2 = new Segment(2L, Map.of());
        Segment s3 = new Segment(3L, Map.of());
        OneMerge merge1 = new OneMerge(List.of(s1));
        OneMerge merge2 = new OneMerge(List.of(s2));
        OneMerge merge3 = new OneMerge(List.of(s3));

        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge1, merge2, merge3));
        when(mergeHandler.doMerge(merge1)).thenReturn(new MergeResult(Map.of()));
        when(mergeHandler.doMerge(merge2)).thenThrow(new IOException("merge2 failed"));

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            expectThrows(IOException.class, () -> scheduler.forceMerge(1));
        } finally {
            Thread.currentThread().setName(oldName);
        }

        // merge1 executed, merge2 failed (onMergeFailure called by runMerge),
        // merge3 should be cleaned up via onMergeFailure
        verify(mergeHandler).doMerge(merge1);
        verify(mergeHandler).doMerge(merge2);
        verify(mergeHandler, never()).doMerge(merge3);
        verify(mergeHandler).onMergeFailure(merge3); // cleanup of unexecuted merge
        assertFalse("Must be unfrozen after failed force merge", scheduler.isFrozen());
    }

    public void testForceMergeReleasesReservationAfterCompletion() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.findForceMerges(1)).thenReturn(List.of());
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        when(mergeHandler.hasOverlappingMerges(any())).thenReturn(false);
        when(mergeHandler.reserveSegmentsForForceMerge()).thenReturn(Set.of());

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        // Reservation must be released (at least twice: start cleanup + finally)
        verify(mergeHandler, times(3)).releaseReservation();
    }

    public void testForceMergeThrowsOnInvalidMaxSegments() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            expectThrows(IllegalArgumentException.class, () -> scheduler.forceMerge(0));
            expectThrows(IllegalArgumentException.class, () -> scheduler.forceMerge(-1));
        } finally {
            Thread.currentThread().setName(oldName);
        }
    }

    public void testForceMergeWaitsForActiveMergesToDrain() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        Segment s1 = new Segment(1L, Map.of());
        OneMerge bgMerge = new OneMerge(List.of(s1));

        // Background merge is pending
        when(mergeHandler.hasPendingMerges()).thenReturn(true, true, false);
        when(mergeHandler.getNextMerge()).thenReturn(bgMerge, (OneMerge) null);
        when(mergeHandler.doMerge(bgMerge)).thenAnswer(inv -> {
            Thread.sleep(200);
            return new MergeResult(Map.of());
        });
        when(mergeHandler.findForceMerges(1)).thenReturn(List.of());
        when(mergeHandler.reserveSegmentsForForceMerge()).thenReturn(Set.of(s1));
        // Overlapping while bg merge runs, then not overlapping after it completes
        when(mergeHandler.hasOverlappingMerges(Set.of(s1))).thenReturn(true, true, false);

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        // Start a background merge
        scheduler.triggerMerges();

        // Force merge should wait for the overlapping background merge to finish
        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        // The background merge should have completed
        verify(mergeHandler).doMerge(bgMerge);
        assertFalse("Should be unfrozen after force merge", scheduler.isFrozen());
    }

    public void testForceMergeSkipsWhenAlreadyFrozen() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        // Should not even attempt to find merges
        verify(mergeHandler, never()).findForceMerges(anyInt());
        assertTrue("Should remain frozen", scheduler.isFrozen());
    }
}

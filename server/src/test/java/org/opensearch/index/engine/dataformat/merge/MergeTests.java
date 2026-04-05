/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the merge flow: {@link OneMerge}, {@link MergeHandler}, and {@link MergeScheduler}.
 */
public class MergeTests extends OpenSearchTestCase {

    // ---- Helpers ----

    private static class TestMergeHandler extends MergeHandler {
        private final List<OneMerge> merges;

        TestMergeHandler(Indexer indexer, ShardId shardId, List<OneMerge> merges) {
            super(indexer, shardId);
            this.merges = merges;
        }

        TestMergeHandler(Indexer indexer, ShardId shardId) {
            this(indexer, shardId, Collections.emptyList());
        }

        @Override
        public Collection<OneMerge> findMerges() {
            return merges;
        }

        @Override
        public Collection<OneMerge> findForceMerges(int maxSegmentCount) {
            return merges;
        }

        @Override
        public MergeResult doMerge(OneMerge oneMerge) {
            return null;
        }
    }

    private MergeScheduler createMergeScheduler() {
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        Indexer mockIndexer = mock(Indexer.class);
        return new MergeScheduler(new TestMergeHandler(mockIndexer, shardId), (mergeResult, oneMerge) -> {}, shardId, idxSettings);
    }

    // ---- OneMerge tests ----

    public void testOneMergeWithEmptySegments() {
        OneMerge merge = new OneMerge(Collections.emptyList());
        assertTrue(merge.getSegmentsToMerge().isEmpty());
        assertEquals(0L, merge.getTotalSizeInBytes());
        assertEquals(0L, merge.getTotalNumDocs());
    }

    public void testOneMergeAggregatesDocCounts() {
        Path dir = createTempDir();
        DataFormat format = new MockDataFormat();
        WriterFileSet fs1 = new WriterFileSet(dir.toString(), 1L, Set.of(), 10);
        WriterFileSet fs2 = new WriterFileSet(dir.toString(), 2L, Set.of(), 20);

        Segment seg1 = Segment.builder(1L).addSearchableFiles(format, fs1).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(format, fs2).build();

        OneMerge merge = new OneMerge(List.of(seg1, seg2));
        assertEquals(2, merge.getSegmentsToMerge().size());
        assertEquals(30L, merge.getTotalNumDocs());
    }

    public void testOneMergeSegmentsListIsUnmodifiable() {
        Segment seg = Segment.builder(1L).build();
        OneMerge merge = new OneMerge(List.of(seg));
        expectThrows(UnsupportedOperationException.class, () -> merge.getSegmentsToMerge().add(seg));
    }

    public void testOneMergeToString() {
        OneMerge merge = new OneMerge(Collections.emptyList());
        assertTrue(merge.toString().contains("Merge"));
    }

    // ---- MergeHandler tests ----

    public void testMergeHandlerInitiallyEmpty() {
        MergeHandler handler = new TestMergeHandler(mock(Indexer.class), new ShardId("test", "_na_", 0));
        assertFalse(handler.hasPendingMerges());
        assertNull(handler.getNextMerge());
    }

    public void testMergeHandlerFindMerges() {
        OneMerge merge = new OneMerge(List.of(Segment.builder(1L).build()));
        TestMergeHandler handler = new TestMergeHandler(mock(Indexer.class), new ShardId("test", "_na_", 0), List.of(merge));
        Collection<OneMerge> found = handler.findMerges();
        assertEquals(1, found.size());
        assertSame(merge, found.iterator().next());
    }

    public void testMergeHandlerFindForceMerges() {
        OneMerge merge = new OneMerge(List.of(Segment.builder(1L).build()));
        TestMergeHandler handler = new TestMergeHandler(mock(Indexer.class), new ShardId("test", "_na_", 0), List.of(merge));
        assertEquals(1, handler.findForceMerges(1).size());
    }

    public void testMergeHandlerDoMergeReturnsNull() {
        assertNull(
            new TestMergeHandler(mock(Indexer.class), new ShardId("test", "_na_", 0)).doMerge(new OneMerge(Collections.emptyList()))
        );
    }

    public void testMergeHandlerLifecycleCallbacks() {
        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(Collections.emptyList());
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        OneMerge merge = new OneMerge(Collections.emptyList());
        handler.registerMerge(merge);
        handler.updatePendingMerges();
        handler.onMergeFinished(merge);
        handler.onMergeFailure(merge);
    }

    public void testRegisterMergeWithValidSegments() {
        Segment seg1 = Segment.builder(1L).build();
        Segment seg2 = Segment.builder(2L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg1, seg2));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        OneMerge merge = new OneMerge(List.of(seg1, seg2));
        handler.registerMerge(merge);

        assertTrue(handler.hasPendingMerges());
        assertSame(merge, handler.getNextMerge());
        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeRejectsSegmentNotInCatalog() {
        Segment catalogSeg = Segment.builder(1L).build();
        Segment unknownSeg = Segment.builder(99L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(catalogSeg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        handler.registerMerge(new OneMerge(List.of(unknownSeg)));

        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeThrowsOnAcquireSnapshotFailure() {
        Indexer mockIndexer = mock(Indexer.class);
        when(mockIndexer.acquireSnapshot()).thenThrow(new RuntimeException("snapshot unavailable"));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        expectThrows(RuntimeException.class, () -> handler.registerMerge(new OneMerge(Collections.emptyList())));
        assertFalse(handler.hasPendingMerges());
    }

    public void testUpdatePendingMergesSkipsAlreadyMergingSegments() {
        Segment seg = Segment.builder(1L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        OneMerge merge = new OneMerge(List.of(seg));
        // Handler whose findMerges returns a merge containing seg
        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0), List.of(merge));

        // Register the merge directly so seg is in currentlyMergingSegments
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        // Now updatePendingMerges calls findMerges which returns the same merge,
        // but seg is already in currentlyMergingSegments so isValidMerge=false, skip
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));
        handler.updatePendingMerges();

        // Should still have only the original merge, no duplicate
        assertNotNull(handler.getNextMerge());
        assertNull(handler.getNextMerge());
    }

    public void testUpdatePendingMergesWithEmptySegmentsMerge() {
        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(Collections.emptyList());
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        // findMerges returns a merge with empty segments list — inner for loop doesn't iterate,
        // isValidMerge stays true, registerMerge is called
        OneMerge emptyMerge = new OneMerge(Collections.emptyList());
        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0), List.of(emptyMerge));

        handler.updatePendingMerges();
        assertTrue(handler.hasPendingMerges());
    }

    public void testUpdatePendingMergesWithNoMergesFound() {
        Indexer mockIndexer = mock(Indexer.class);
        // findMerges returns empty — outer for loop doesn't iterate
        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0), Collections.emptyList());

        handler.updatePendingMerges();
        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeWithEmptySegmentsList() {
        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(Collections.emptyList());
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        // Empty segments list — for loop in registerMerge doesn't iterate, merge is registered
        handler.registerMerge(new OneMerge(Collections.emptyList()));
        assertTrue(handler.hasPendingMerges());
    }

    public void testOnMergeFinishedRemovesSegmentsAndUpdates() {
        Segment seg = Segment.builder(1L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        OneMerge merge = new OneMerge(List.of(seg));
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));
        handler.onMergeFinished(merge);
        // After onMergeFinished, the merge is removed; updatePendingMerges is called
        // but findMerges returns empty list for this handler, so nothing new is added
        assertFalse(handler.hasPendingMerges());
    }

    public void testOnMergeFailureRemovesSegments() {
        Segment seg = Segment.builder(1L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        OneMerge merge = new OneMerge(List.of(seg));
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFailure(merge);
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsInOrder() {
        Segment seg1 = Segment.builder(1L).build();
        Segment seg2 = Segment.builder(2L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg1, seg2));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        OneMerge merge1 = new OneMerge(List.of(seg1));
        OneMerge merge2 = new OneMerge(List.of(seg2));

        handler.registerMerge(merge1);
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));
        handler.registerMerge(merge2);

        assertTrue(handler.hasPendingMerges());
        assertSame(merge1, handler.getNextMerge());
        assertSame(merge2, handler.getNextMerge());
        assertNull(handler.getNextMerge());
    }

    public void testRegisterMergeRejectsWhenSecondSegmentNotInCatalog() {
        Segment catalogSeg = Segment.builder(1L).build();
        Segment unknownSeg = Segment.builder(99L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(catalogSeg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0));
        // First segment is in catalog, second is not — covers the loop-continue-then-return branch
        handler.registerMerge(new OneMerge(List.of(catalogSeg, unknownSeg)));

        assertFalse(handler.hasPendingMerges());
    }

    public void testUpdatePendingMergesRegistersValidMerges() {
        Segment seg = Segment.builder(1L).build();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(mockSnapshot, () -> {}));

        OneMerge merge = new OneMerge(List.of(seg));
        // Handler whose findMerges returns a merge with a valid segment
        MergeHandler handler = new TestMergeHandler(mockIndexer, new ShardId("test", "_na_", 0), List.of(merge));

        handler.updatePendingMerges();

        assertTrue(handler.hasPendingMerges());
        assertSame(merge, handler.getNextMerge());
    }

    // ---- MergeScheduler tests ----

    public void testSchedulerDefaultIOThrottleReturnsInfinity() {
        assertEquals(Double.POSITIVE_INFINITY, createMergeScheduler().getIORateLimitMBPerSec(), 0.0);
    }

    public void testSchedulerEnableAutoIOThrottle() {
        MergeScheduler scheduler = createMergeScheduler();
        scheduler.enableAutoIOThrottle();
        assertEquals(20.0, scheduler.getIORateLimitMBPerSec(), 0.0);
    }

    public void testSchedulerStatsReturnsNonNull() {
        assertNotNull(createMergeScheduler().stats());
    }

    public void testSchedulerRefreshConfigIdempotent() {
        MergeScheduler scheduler = createMergeScheduler();
        // Second call with same config should be a no-op (covers the early return branch)
        scheduler.refreshConfig();
        scheduler.refreshConfig();
    }

    public void testSchedulerTriggerAndForceMerge() {
        MergeScheduler scheduler = createMergeScheduler();
        scheduler.triggerMerges();
        scheduler.forceMerge(1);
    }

    // ---- MergeScheduler: shutdown branch in triggerMerges ----

    @SuppressForbidden(reason = "test needs to set private isShutdown field via reflection")
    public void testTriggerMergesAfterShutdown() throws Exception {
        MergeScheduler scheduler = createMergeScheduler();
        setShutdownFlag(scheduler, true);
        // Should return early, no exception
        scheduler.triggerMerges();
    }

    // ---- MergeScheduler: MergeThread success path via triggerMerges ----

    public void testTriggerMergesExecutesMergeThread() throws Exception {
        Segment seg = Segment.builder(1L).build();
        OneMerge merge = new OneMerge(List.of(seg));
        CountDownLatch latch = new CountDownLatch(1);
        MergeResult result = new MergeResult(Map.of());

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(snap, () -> {}));

        MergeHandler handler = new MergeHandler(mockIndexer, new ShardId("test", "_na_", 0)) {
            private final AtomicBoolean returned = new AtomicBoolean(false);

            @Override
            public Collection<OneMerge> findMerges() {
                return returned.compareAndSet(false, true) ? List.of(merge) : Collections.emptyList();
            }

            @Override
            public Collection<OneMerge> findForceMerges(int max) {
                return Collections.emptyList();
            }

            @Override
            public MergeResult doMerge(OneMerge m) {
                latch.countDown();
                return result;
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        AtomicReference<MergeResult> captured = new AtomicReference<>();
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> captured.set(mr), shardId, idxSettings);

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // Wait for thread to finish
        Thread.sleep(200);
        assertSame(result, captured.get());
    }

    // ---- MergeScheduler: MergeThread failure path ----

    public void testTriggerMergesHandlesMergeFailure() throws Exception {
        Segment seg = Segment.builder(1L).build();
        OneMerge merge = new OneMerge(List.of(seg));
        CountDownLatch latch = new CountDownLatch(1);

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(snap, () -> {}));

        MergeHandler handler = new MergeHandler(mockIndexer, new ShardId("test", "_na_", 0)) {
            private final AtomicBoolean returned = new AtomicBoolean(false);

            @Override
            public Collection<OneMerge> findMerges() {
                return returned.compareAndSet(false, true) ? List.of(merge) : Collections.emptyList();
            }

            @Override
            public Collection<OneMerge> findForceMerges(int max) {
                return Collections.emptyList();
            }

            @Override
            public MergeResult doMerge(OneMerge m) {
                latch.countDown();
                throw new RuntimeException("merge boom");
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {}, shardId, idxSettings);

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
    }

    // ---- MergeScheduler: MergeThread shutdown check inside run() ----

    @SuppressForbidden(reason = "test needs to set private isShutdown field via reflection")
    public void testMergeThreadSkipsWhenShutdownDuringRun() throws Exception {
        Segment seg = Segment.builder(1L).build();
        OneMerge merge = new OneMerge(List.of(seg));
        AtomicBoolean doMergeCalled = new AtomicBoolean(false);

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(snap, () -> {}));

        // We need a reference to the scheduler to set isShutdown inside updatePendingMerges
        AtomicReference<MergeScheduler> schedulerRef = new AtomicReference<>();

        MergeHandler handler = new MergeHandler(mockIndexer, new ShardId("test", "_na_", 0)) {
            private final AtomicBoolean returned = new AtomicBoolean(false);

            @Override
            public Collection<OneMerge> findMerges() {
                return returned.compareAndSet(false, true) ? List.of(merge) : Collections.emptyList();
            }

            @Override
            public Collection<OneMerge> findForceMerges(int max) {
                return Collections.emptyList();
            }

            @Override
            public MergeResult doMerge(OneMerge m) {
                doMergeCalled.set(true);
                return new MergeResult(Map.of());
            }

            @Override
            public void updatePendingMerges() {
                super.updatePendingMerges();
                setShutdownFlag(schedulerRef.get(), true);
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {}, shardId, idxSettings);
        schedulerRef.set(scheduler);

        // triggerMerges: passes isShutdown check → updatePendingMerges sets isShutdown=true
        // → executeMerge starts thread → thread.run() sees isShutdown=true → skips doMerge
        scheduler.triggerMerges();

        // Wait for the thread to finish
        assertBusy(() -> assertFalse("doMerge should not be called when shutdown during run()", doMergeCalled.get()));
    }

    // ---- MergeScheduler: forceMerge with actual merges ----

    public void testForceMergeExecutesMerges() {
        Segment seg = Segment.builder(1L).build();
        OneMerge merge = new OneMerge(List.of(seg));
        MergeResult result = new MergeResult(Map.of());
        AtomicReference<MergeResult> captured = new AtomicReference<>();

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(snap, () -> {}));

        MergeHandler handler = new MergeHandler(mockIndexer, new ShardId("test", "_na_", 0)) {
            @Override
            public Collection<OneMerge> findMerges() {
                return Collections.emptyList();
            }

            @Override
            public Collection<OneMerge> findForceMerges(int max) {
                return List.of(merge);
            }

            @Override
            public MergeResult doMerge(OneMerge m) {
                return result;
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> captured.set(mr), shardId, idxSettings);

        scheduler.forceMerge(1);
        assertSame(result, captured.get());
    }

    // ---- MergeScheduler: forceMerge throws when threads active ----

    public void testForceMergeThrowsWhenMergeThreadsActive() throws Exception {
        Segment seg = Segment.builder(1L).build();
        OneMerge merge = new OneMerge(List.of(seg));
        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch mergeCanFinish = new CountDownLatch(1);

        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(snap, () -> {}));

        MergeHandler handler = new MergeHandler(mockIndexer, new ShardId("test", "_na_", 0)) {
            private final AtomicBoolean returned = new AtomicBoolean(false);

            @Override
            public Collection<OneMerge> findMerges() {
                return returned.compareAndSet(false, true) ? List.of(merge) : Collections.emptyList();
            }

            @Override
            public Collection<OneMerge> findForceMerges(int max) {
                return Collections.emptyList();
            }

            @Override
            public MergeResult doMerge(OneMerge m) {
                mergeStarted.countDown();
                try {
                    mergeCanFinish.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return new MergeResult(Map.of());
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {}, shardId, idxSettings);

        scheduler.triggerMerges();
        assertTrue(mergeStarted.await(5, TimeUnit.SECONDS));

        // Now a merge thread is active — forceMerge should throw
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> scheduler.forceMerge(1));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("Cannot force merge while background merges are active"));

        mergeCanFinish.countDown();
        // Wait for merge thread to finish to avoid thread leak
        assertBusy(() -> assertEquals(0, scheduler.stats().getCurrent()));
    }

    // ---- MergeScheduler: executeMerge returns when getNextMerge is null ----

    public void testTriggerMergesWithNoPendingMerges() {
        // Handler with no merges — executeMerge loop body never entered
        MergeScheduler scheduler = createMergeScheduler();
        scheduler.triggerMerges();
        assertEquals(0, scheduler.stats().getCurrent());
    }

    public void testTriggerMergesReturnsEarlyWhenNextMergeNull() {
        Indexer mockIndexer = mock(Indexer.class);
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(Collections.emptyList());
        when(mockIndexer.acquireSnapshot()).thenReturn(new GatedCloseable<>(snap, () -> {}));

        MergeHandler handler = new MergeHandler(mockIndexer, new ShardId("test", "_na_", 0)) {
            private final AtomicBoolean first = new AtomicBoolean(true);

            @Override
            public Collection<OneMerge> findMerges() {
                return Collections.emptyList();
            }

            @Override
            public Collection<OneMerge> findForceMerges(int max) {
                return Collections.emptyList();
            }

            @Override
            public MergeResult doMerge(OneMerge m) {
                return null;
            }

            @Override
            public boolean hasPendingMerges() {
                return first.getAndSet(false);
            }

            @Override
            public OneMerge getNextMerge() {
                return null;
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {}, shardId, idxSettings);

        // hasPendingMerges returns true once, getNextMerge returns null → early return
        scheduler.triggerMerges();
        assertEquals(0, scheduler.stats().getCurrent());
    }

    // ---- MergeScheduler: stats with auto-throttle enabled ----

    public void testStatsWithAutoThrottleEnabled() {
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        Indexer mockIndexer = mock(Indexer.class);
        MergeScheduler scheduler = new MergeScheduler(new TestMergeHandler(mockIndexer, shardId), (mr, om) -> {}, shardId, idxSettings);
        scheduler.enableAutoIOThrottle();
        assertNotNull(scheduler.stats());
    }

    @SuppressForbidden(reason = "helper to set private isShutdown field via reflection for testing")
    private static void setShutdownFlag(MergeScheduler scheduler, boolean value) {
        try {
            Field f = MergeScheduler.class.getDeclaredField("isShutdown");
            f.setAccessible(true);
            ((AtomicBoolean) f.get(scheduler)).set(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

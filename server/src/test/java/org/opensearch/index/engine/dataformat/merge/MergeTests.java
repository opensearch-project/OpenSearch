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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the merge flow: {@link OneMerge}, {@link MergeHandler}, and {@link MergeScheduler}.
 */
public class MergeTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId("test", "_na_", 0);

    private final List<ExecutorService> executors = new CopyOnWriteArrayList<>();

    private ExecutorService daemonPool(String name) {
        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(name + "-" + t.threadId());
            return t;
        });
        executors.add(pool);
        return pool;
    }

    private ThreadPool mockThreadPool() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.executor(eq(ThreadPool.Names.MERGE))).thenReturn(daemonPool(ThreadPool.Names.MERGE));
        when(tp.executor(eq(ThreadPool.Names.FORCE_MERGE))).thenReturn(daemonPool(ThreadPool.Names.FORCE_MERGE));
        return tp;
    }

    /**
     * Runs the given action on a thread whose name contains
     * {@link ThreadPool.Names#FORCE_MERGE} to satisfy the forceMerge assertion.
     */
    private void onForceMergeThread(ThrowingRunnable action) throws Exception {
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                action.run();
            } catch (Exception e) {
                failure.set(e);
            }
        }, ThreadPool.Names.FORCE_MERGE + "-test");
        t.setDaemon(true);
        t.start();
        t.join(30_000);
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Override
    public void tearDown() throws Exception {
        for (ExecutorService pool : executors) {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
        executors.clear();
        super.tearDown();
    }

    private static final MergeHandler.MergePolicy NOOP_MERGE_POLICY = new MergeHandler.MergePolicy() {
        @Override
        public List<List<Segment>> findMergeCandidates(List<Segment> segments) {
            return List.of();
        }

        @Override
        public List<List<Segment>> findForceMergeCandidates(List<Segment> segments, int maxSegmentCount) {
            return List.of();
        }
    };

    private static final MergeHandler.MergeListener NOOP_MERGE_LISTENER = new MergeHandler.MergeListener() {
        @Override
        public void addMergingSegment(Collection<Segment> mergingSegments) {}

        @Override
        public void removeMergingSegment(Collection<Segment> mergingSegments) {}
    };

    private MergeHandler createNoopHandler(Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier) {
        Merger noopMerger = mergeInput -> new MergeResult(Map.of());
        return new MergeHandler(snapshotSupplier, noopMerger, SHARD_ID, NOOP_MERGE_POLICY, NOOP_MERGE_LISTENER, () -> 1L);
    }

    private MergeHandler createHandlerWithRealPolicy(Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier, Merger merger) {
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(
            new IndexSettings(newIndexMeta("test", Settings.EMPTY), Settings.EMPTY).getMergePolicy(true),
            SHARD_ID
        );
        return new MergeHandler(snapshotSupplier, merger, SHARD_ID, policy, policy, () -> 1L);
    }

    private static Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplierOf(List<Segment> segments) {
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(segments);
        return () -> new GatedCloseable<>(snap, () -> {});
    }

    private static Supplier<GatedCloseable<CatalogSnapshot>> emptySnapshotSupplier() {
        return snapshotSupplierOf(Collections.emptyList());
    }

    private static List<Segment> createSegments(int count) {
        List<Segment> segments = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            segments.add(Segment.builder(i).build());
        }
        return segments;
    }

    private static IndexSettings mergeSchedulerSettings() {
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    private MergeScheduler createMergeScheduler() {
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        return new MergeScheduler(
            createNoopHandler(emptySnapshotSupplier()),
            (mergeResult, oneMerge) -> {},
            () -> {},
            SHARD_ID,
            idxSettings,
            mockThreadPool()
        );
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
        MockDataFormat format = new MockDataFormat();
        WriterFileSet fs1 = new WriterFileSet(dir.toString(), 1L, Set.of(), 10, 0L);
        WriterFileSet fs2 = new WriterFileSet(dir.toString(), 2L, Set.of(), 20, 0L);

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
        MergeHandler handler = createNoopHandler(() -> new GatedCloseable<>(null, () -> {}));
        assertFalse(handler.hasPendingMerges());
        assertNull(handler.getNextMerge());
    }

    public void testMergeHandlerLifecycleCallbacks() {
        MergeHandler handler = createNoopHandler(emptySnapshotSupplier());
        OneMerge merge = new OneMerge(Collections.emptyList());
        handler.registerMerge(merge);
        handler.findAndRegisterMerges();
        handler.onMergeFinished(merge, false);
        handler.onMergeFailure(merge);
    }

    public void testRegisterMergeWithValidSegments() {
        Segment seg1 = Segment.builder(1L).build();
        Segment seg2 = Segment.builder(2L).build();

        MergeHandler handler = createNoopHandler(snapshotSupplierOf(List.of(seg1, seg2)));
        OneMerge merge = new OneMerge(List.of(seg1, seg2));
        handler.registerMerge(merge);

        assertTrue(handler.hasPendingMerges());
        assertSame(merge, handler.getNextMerge());
        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeRejectsSegmentNotInCatalog() {
        Segment catalogSeg = Segment.builder(1L).build();
        Segment unknownSeg = Segment.builder(99L).build();

        MergeHandler handler = createNoopHandler(snapshotSupplierOf(List.of(catalogSeg)));
        handler.registerMerge(new OneMerge(List.of(unknownSeg)));

        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeThrowsOnAcquireSnapshotFailure() {
        Supplier<GatedCloseable<CatalogSnapshot>> failingSupplier = () -> { throw new RuntimeException("snapshot unavailable"); };

        MergeHandler handler = createNoopHandler(failingSupplier);
        expectThrows(RuntimeException.class, () -> handler.registerMerge(new OneMerge(Collections.emptyList())));
        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeWithEmptySegmentsList() {
        MergeHandler handler = createNoopHandler(emptySnapshotSupplier());
        handler.registerMerge(new OneMerge(Collections.emptyList()));
        assertTrue(handler.hasPendingMerges());
    }

    public void testOnMergeFinishedRemovesSegments() {
        Segment seg = Segment.builder(1L).build();

        MergeHandler handler = createNoopHandler(snapshotSupplierOf(List.of(seg)));
        OneMerge merge = new OneMerge(List.of(seg));
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFinished(merge, false);
        assertFalse(handler.hasPendingMerges());
    }

    public void testOnMergeFailureRemovesSegments() {
        Segment seg = Segment.builder(1L).build();

        MergeHandler handler = createNoopHandler(snapshotSupplierOf(List.of(seg)));
        OneMerge merge = new OneMerge(List.of(seg));
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFailure(merge);
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsInOrder() {
        Segment seg1 = Segment.builder(1L).build();
        Segment seg2 = Segment.builder(2L).build();

        MergeHandler handler = createNoopHandler(snapshotSupplierOf(List.of(seg1, seg2)));
        OneMerge merge1 = new OneMerge(List.of(seg1));
        OneMerge merge2 = new OneMerge(List.of(seg2));

        handler.registerMerge(merge1);
        handler.registerMerge(merge2);

        assertTrue(handler.hasPendingMerges());
        assertSame(merge1, handler.getNextMerge());
        assertSame(merge2, handler.getNextMerge());
        assertNull(handler.getNextMerge());
    }

    public void testRegisterMergeRejectsWhenSecondSegmentNotInCatalog() {
        Segment catalogSeg = Segment.builder(1L).build();
        Segment unknownSeg = Segment.builder(99L).build();

        MergeHandler handler = createNoopHandler(snapshotSupplierOf(List.of(catalogSeg)));
        handler.registerMerge(new OneMerge(List.of(catalogSeg, unknownSeg)));

        assertFalse(handler.hasPendingMerges());
    }

    // ---- MergeHandler doMerge tests ----

    public void testDoMergeReturnsResult() throws IOException {
        Path dir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        WriterFileSet inputWfs = new WriterFileSet(dir.toString(), 1L, Set.of("input.dat"), 10, 0L);
        Segment seg = Segment.builder(1L).addSearchableFiles(format, inputWfs).build();

        WriterFileSet mergedWfs = new WriterFileSet(dir.toString(), 99L, Set.of("merged.dat"), 10, 0L);
        MergeResult expectedResult = new MergeResult(Map.of(format, mergedWfs));
        Merger merger = mergeInput -> expectedResult;

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(seg)),
            merger,
            SHARD_ID,
            NOOP_MERGE_POLICY,
            NOOP_MERGE_LISTENER,
            () -> 1L
        );
        MergeResult result = handler.doMerge(new OneMerge(List.of(seg)));

        assertSame(expectedResult, result);
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
        scheduler.refreshConfig();
        scheduler.refreshConfig();
    }

    public void testSchedulerTriggerAndForceMerge() throws Exception {
        MergeScheduler scheduler = createMergeScheduler();
        scheduler.triggerMerges();
        onForceMergeThread(() -> scheduler.forceMerge(1));
    }

    @SuppressForbidden(reason = "test needs to set private isShutdown field via reflection")
    public void testTriggerMergesAfterShutdown() throws Exception {
        MergeScheduler scheduler = createMergeScheduler();
        setShutdownFlag(scheduler, true);
        scheduler.triggerMerges();
    }

    public void testTriggerMergesWithNoPendingMerges() {
        MergeScheduler scheduler = createMergeScheduler();
        scheduler.triggerMerges();
        assertEquals(0, scheduler.stats().getCurrent());
    }

    public void testStatsWithAutoThrottleEnabled() {
        Settings autoThrottleSettings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", autoThrottleSettings), Settings.EMPTY);
        MergeScheduler scheduler = new MergeScheduler(
            createNoopHandler(emptySnapshotSupplier()),
            (mr, om) -> {},
            () -> {},
            SHARD_ID,
            idxSettings,
            mockThreadPool()
        );
        scheduler.enableAutoIOThrottle();
        assertNotNull(scheduler.stats());
    }

    // ---- MergeScheduler: integration with real merge execution ----

    public void testTriggerMergesExecutesMergeThread() throws Exception {
        List<Segment> segments = createSegments(15);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 15, 0L);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));
        CountDownLatch latch = new CountDownLatch(1);

        Merger merger = mergeInput -> {
            latch.countDown();
            return mergeResult;
        };
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), merger);

        AtomicReference<MergeResult> captured = new AtomicReference<>();
        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> captured.set(mr),
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        assertNotNull(captured.get());
    }

    public void testTriggerMergesHandlesMergeFailure() throws Exception {
        List<Segment> segments = createSegments(15);
        CountDownLatch latch = new CountDownLatch(1);

        Merger failingMerger = mergeInput -> {
            latch.countDown();
            throw new IOException("merge boom");
        };
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), failingMerger);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> {},
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
    }

    public void testForceMergeExecutesMerges() throws Exception {
        List<Segment> segments = createSegments(3);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 3, 0L);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));
        CountDownLatch latch = new CountDownLatch(1);

        Merger merger = mergeInput -> mergeResult;
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), merger);

        AtomicReference<MergeResult> captured = new AtomicReference<>();
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {
            captured.set(mr);
            latch.countDown();
        }, () -> {}, SHARD_ID, mergeSchedulerSettings(), mockThreadPool());

        onForceMergeThread(() -> scheduler.forceMerge(1));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(captured.get());
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

    // ---- MergeScheduler: forceMerge serialization and lifecycle tests ----

    public void testForceMergeSerializesOnlyConcurrentCallers() throws Exception {
        List<Segment> segments = createSegments(3);
        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch allowMergeToFinish = new CountDownLatch(1);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 3, 0L);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));

        Merger slowMerger = mergeInput -> {
            mergeStarted.countDown();
            try {
                allowMergeToFinish.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return mergeResult;
        };
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), slowMerger);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> {},
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        AtomicBoolean secondStarted = new AtomicBoolean(false);
        AtomicBoolean secondFinished = new AtomicBoolean(false);

        Thread t1 = new Thread(() -> {
            try {
                scheduler.forceMerge(1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, ThreadPool.Names.FORCE_MERGE + "-t1");

        Thread t2 = new Thread(() -> {
            try {
                mergeStarted.await(5, TimeUnit.SECONDS);
                secondStarted.set(true);
                scheduler.forceMerge(1);
                secondFinished.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, ThreadPool.Names.FORCE_MERGE + "-t2");

        t1.start();
        t2.start();

        assertTrue(mergeStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        // Second caller should be blocked (not finished) while first is in progress
        assertTrue(secondStarted.get());
        assertFalse(secondFinished.get());

        allowMergeToFinish.countDown();
        t1.join(5000);
        t2.join(5000);
        assertTrue(secondFinished.get());
    }

    public void testForceMergeBlocksUntilComplete() throws Exception {
        List<Segment> segments = createSegments(3);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 3, 0L);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));

        Merger slowMerger = mergeInput -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return mergeResult;
        };
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), slowMerger);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> {},
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        long start = System.nanoTime();
        onForceMergeThread(() -> scheduler.forceMerge(1));
        long elapsed = TimeValue.nsecToMSec(System.nanoTime() - start);

        assertTrue("forceMerge should block until complete, took " + elapsed + "ms", elapsed >= 150);
    }

    public void testForceMergePropagatesFailure() throws Exception {
        List<Segment> segments = createSegments(3);

        Merger failingMerger = mergeInput -> { throw new IOException("simulated merge failure"); };
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), failingMerger);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> {},
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        AtomicReference<Exception> caught = new AtomicReference<>();
        onForceMergeThread(() -> {
            try {
                scheduler.forceMerge(1);
            } catch (IOException e) {
                caught.set(e);
            }
        });
        assertNotNull("Expected IOException from forceMerge", caught.get());
        assertTrue(caught.get().getMessage().contains("simulated merge failure"));
    }

    public void testRunMergeInvokesCleanupOnFailure() throws Exception {
        List<Segment> segments = createSegments(3);
        AtomicBoolean cleanupCalled = new AtomicBoolean(false);

        Merger failingMerger = mergeInput -> { throw new IOException("merge failure"); };
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), failingMerger);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> {},
            () -> cleanupCalled.set(true),
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        // Use forceMerge which catches exceptions from runMerge properly
        onForceMergeThread(() -> {
            try {
                scheduler.forceMerge(1);
            } catch (IOException expected) {
                // expected
            }
        });
        assertTrue("onMergeFailureCleanup should be called", cleanupCalled.get());
    }

    public void testRunMergeInvokesApplyOnSuccess() throws Exception {
        List<Segment> segments = createSegments(3);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 3, 0L);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));
        AtomicReference<MergeResult> capturedResult = new AtomicReference<>();

        Merger merger = mergeInput -> mergeResult;
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), merger);

        BiConsumer<MergeResult, OneMerge> applyCallback = (mr, om) -> capturedResult.set(mr);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            applyCallback,
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        onForceMergeThread(() -> scheduler.forceMerge(1));
        assertSame(mergeResult, capturedResult.get());
    }

    public void testForceMergeWithNoSegmentsIsNoop() throws Exception {
        MergeScheduler scheduler = new MergeScheduler(createNoopHandler(emptySnapshotSupplier()), (mr, om) -> {
            fail("applyMergeChanges should not be called");
        }, () -> { fail("onMergeFailureCleanup should not be called"); }, SHARD_ID, mergeSchedulerSettings(), mockThreadPool());

        onForceMergeThread(() -> scheduler.forceMerge(1));
    }

    public void testConcurrentForceMergeAndBackgroundMerge() throws Exception {
        List<Segment> segments = createSegments(15);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 15, 0L);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));

        Merger merger = mergeInput -> mergeResult;
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), merger);

        MergeScheduler scheduler = new MergeScheduler(
            handler,
            (mr, om) -> {},
            () -> {},
            SHARD_ID,
            mergeSchedulerSettings(),
            mockThreadPool()
        );

        AtomicReference<Exception> forceMergeError = new AtomicReference<>();
        AtomicReference<Exception> triggerError = new AtomicReference<>();

        Thread forceMergeThread = new Thread(() -> {
            try {
                scheduler.forceMerge(1);
            } catch (Exception e) {
                forceMergeError.set(e);
            }
        }, ThreadPool.Names.FORCE_MERGE + "-test");

        Thread triggerThread = new Thread(() -> {
            try {
                scheduler.triggerMerges();
            } catch (Exception e) {
                triggerError.set(e);
            }
        });

        forceMergeThread.start();
        triggerThread.start();

        forceMergeThread.join(10000);
        triggerThread.join(10000);

        assertNull("forceMerge should complete without error", forceMergeError.get());
        assertNull("triggerMerges should complete without error", triggerError.get());
    }

    // ---- Auxiliary (side table) pairing ----

    /** A policy that records what it was offered and merges everything it is offered. */
    private static final class RecordingMergePolicy implements MergeHandler.MergePolicy {
        private final AtomicReference<List<Segment>> offered = new AtomicReference<>();

        @Override
        public List<List<Segment>> findMergeCandidates(List<Segment> segments) {
            offered.set(segments);
            return segments.isEmpty() ? List.of() : List.of(List.copyOf(segments));
        }

        @Override
        public List<List<Segment>> findForceMergeCandidates(List<Segment> segments, int maxSegmentCount) {
            return findMergeCandidates(segments);
        }
    }

    private static Segment documentSegment(Path dir, long generation, long rows) {
        return Segment.builder(generation)
            .addSearchableFiles(new MockDataFormat(), new WriterFileSet(dir.toString(), generation, Set.of(), rows, 0L))
            .build();
    }

    private static Segment childSegment(Path dir, long parentGeneration, long elementRows) {
        long generation = AuxiliaryDataFormat.generationFor(parentGeneration);
        return Segment.builder(generation)
            .addSearchableFiles(
                AuxiliaryDataFormat.nameFor(new MockDataFormat().name(), AuxiliaryDataFormat.NESTED_CHILD_ROLE),
                new WriterFileSet(dir.toString(), generation, Set.of(), elementRows, 0L)
            )
            .build();
    }

    public void testMergePolicyIsOfferedDocumentSegmentsOnly() {
        Path dir = createTempDir();
        Segment parent1 = documentSegment(dir, 1L, 2);
        Segment parent2 = documentSegment(dir, 2L, 2);
        List<Segment> catalog = List.of(parent1, childSegment(dir, 1L, 3), parent2, childSegment(dir, 2L, 3));

        RecordingMergePolicy policy = new RecordingMergePolicy();
        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(catalog),
            mergeInput -> new MergeResult(Map.of()),
            SHARD_ID,
            policy,
            NOOP_MERGE_LISTENER,
            () -> 3L
        );

        handler.findMerges();

        assertEquals(
            "policy must not see side tables — their element rows are not documents",
            List.of(parent1, parent2),
            policy.offered.get()
        );
    }

    public void testSelectedMergeCarriesPairedChildSegments() {
        Path dir = createTempDir();
        Segment parent1 = documentSegment(dir, 1L, 2);
        Segment parent2 = documentSegment(dir, 2L, 2);
        Segment child1 = childSegment(dir, 1L, 3);
        Segment child2 = childSegment(dir, 2L, 3);

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(parent1, child1, parent2, child2)),
            mergeInput -> new MergeResult(Map.of()),
            SHARD_ID,
            new RecordingMergePolicy(),
            NOOP_MERGE_LISTENER,
            () -> 3L
        );

        Collection<OneMerge> merges = handler.findMerges();
        assertEquals(1, merges.size());
        List<Segment> selected = merges.iterator().next().getSegmentsToMerge();
        assertEquals(4, selected.size());
        assertTrue("both parents must be selected", selected.containsAll(List.of(parent1, parent2)));
        assertTrue("each parent's side table must be selected with it", selected.containsAll(List.of(child1, child2)));
    }

    public void testUnpairedChildSegmentIsNotSelected() {
        Path dir = createTempDir();
        Segment parent1 = documentSegment(dir, 1L, 2);
        // Generation 2's documents are not in the catalog, so its side table has nothing to pair with.
        Segment orphanChild = childSegment(dir, 2L, 3);

        RecordingMergePolicy policy = new RecordingMergePolicy();
        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(parent1, childSegment(dir, 1L, 3), orphanChild)),
            mergeInput -> new MergeResult(Map.of()),
            SHARD_ID,
            policy,
            NOOP_MERGE_LISTENER,
            () -> 3L
        );

        List<Segment> selected = handler.findMerges().iterator().next().getSegmentsToMerge();
        assertEquals(2, selected.size());
        assertFalse("a side table whose documents are not merging must be left alone", selected.contains(orphanChild));
    }

    public void testAuxiliarySegmentAtDocumentGenerationIsSkipped() {
        Path dir = createTempDir();
        Segment parent1 = documentSegment(dir, 1L, 2);
        // An auxiliary segment that never got the generation offset: unpairable, so excluded
        // rather than merged blind.
        Segment unoffset = Segment.builder(1L)
            .addSearchableFiles(
                AuxiliaryDataFormat.nameFor(new MockDataFormat().name(), AuxiliaryDataFormat.NESTED_CHILD_ROLE),
                new WriterFileSet(dir.toString(), 1L, Set.of(), 3, 0L)
            )
            .build();

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(parent1, unoffset)),
            mergeInput -> new MergeResult(Map.of()),
            SHARD_ID,
            new RecordingMergePolicy(),
            NOOP_MERGE_LISTENER,
            () -> 3L
        );

        List<Segment> selected = handler.findMerges().iterator().next().getSegmentsToMerge();
        assertEquals(List.of(parent1), selected);
    }

    public void testForceMergeAlsoPairsChildSegments() throws Exception {
        Path dir = createTempDir();
        Segment parent1 = documentSegment(dir, 1L, 2);
        Segment child1 = childSegment(dir, 1L, 3);

        RecordingMergePolicy policy = new RecordingMergePolicy();
        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(parent1, child1)),
            mergeInput -> new MergeResult(Map.of()),
            SHARD_ID,
            policy,
            NOOP_MERGE_LISTENER,
            () -> 3L
        );

        Collection<OneMerge> merges = handler.findForceMerges(1);
        assertEquals(List.of(parent1), policy.offered.get());
        assertEquals(1, merges.size());
        assertEquals(List.of(parent1, child1), merges.iterator().next().getSegmentsToMerge());
    }
}

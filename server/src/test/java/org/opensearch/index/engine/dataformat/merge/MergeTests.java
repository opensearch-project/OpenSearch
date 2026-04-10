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
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.lang.reflect.Field;
import java.nio.file.Path;
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
import java.util.function.Supplier;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the merge flow: {@link OneMerge}, {@link MergeHandler}, and {@link MergeScheduler}.
 */
public class MergeTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId("test", "_na_", 0);

    private final List<ExecutorService> executors = new CopyOnWriteArrayList<>();

    private ExecutorService daemonPool() {
        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        executors.add(pool);
        return pool;
    }

    private ThreadPool mockThreadPool() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.executor(eq(ThreadPool.Names.MERGE))).thenReturn(daemonPool());
        return tp;
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

    private MergeHandler createNoopHandler(Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier) {
        DataFormatAwareMergePolicy policy = mock(DataFormatAwareMergePolicy.class);
        return new MergeHandler(snapshotSupplier, policy, SHARD_ID) {
            @Override
            public MergeResult doMerge(OneMerge oneMerge) {
                return new MergeResult(Map.of());
            }
        };
    }

    private MergeHandler createHandlerWithResult(Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier, MergeResult result) {
        DataFormatAwareMergePolicy policy = mock(DataFormatAwareMergePolicy.class);
        return new MergeHandler(snapshotSupplier, policy, SHARD_ID) {
            @Override
            public MergeResult doMerge(OneMerge oneMerge) {
                return result;
            }
        };
    }

    private static Supplier<GatedCloseable<CatalogSnapshot>> emptySnapshotSupplier() {
        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(Collections.emptyList());
        return () -> new GatedCloseable<>(snap, () -> {});
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
        MergeHandler handler = createNoopHandler(() -> new GatedCloseable<>(null, () -> {}));
        assertFalse(handler.hasPendingMerges());
        assertNull(handler.getNextMerge());
    }

    public void testMergeHandlerLifecycleCallbacks() {
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(Collections.emptyList());
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
        OneMerge merge = new OneMerge(Collections.emptyList());
        handler.registerMerge(merge);
        handler.updatePendingMerges();
        handler.onMergeFinished(merge);
        handler.onMergeFailure(merge);
    }

    public void testRegisterMergeWithValidSegments() {
        Segment seg1 = Segment.builder(1L).build();
        Segment seg2 = Segment.builder(2L).build();

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg1, seg2));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
        OneMerge merge = new OneMerge(List.of(seg1, seg2));
        handler.registerMerge(merge);

        assertTrue(handler.hasPendingMerges());
        assertSame(merge, handler.getNextMerge());
        assertFalse(handler.hasPendingMerges());
    }

    public void testRegisterMergeRejectsSegmentNotInCatalog() {
        Segment catalogSeg = Segment.builder(1L).build();
        Segment unknownSeg = Segment.builder(99L).build();

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(catalogSeg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
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
        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(Collections.emptyList());
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
        handler.registerMerge(new OneMerge(Collections.emptyList()));
        assertTrue(handler.hasPendingMerges());
    }

    public void testOnMergeFinishedRemovesSegments() {
        Segment seg = Segment.builder(1L).build();

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
        OneMerge merge = new OneMerge(List.of(seg));
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFinished(merge);
        assertFalse(handler.hasPendingMerges());
    }

    public void testOnMergeFailureRemovesSegments() {
        Segment seg = Segment.builder(1L).build();

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
        OneMerge merge = new OneMerge(List.of(seg));
        handler.registerMerge(merge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFailure(merge);
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsInOrder() {
        Segment seg1 = Segment.builder(1L).build();
        Segment seg2 = Segment.builder(2L).build();

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(seg1, seg2));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
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

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);
        when(mockSnapshot.getSegments()).thenReturn(List.of(catalogSeg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(mockSnapshot, () -> {});

        MergeHandler handler = createNoopHandler(snapshotSupplier);
        handler.registerMerge(new OneMerge(List.of(catalogSeg, unknownSeg)));

        assertFalse(handler.hasPendingMerges());
    }

    // ---- MergeHandler doMerge tests ----

    public void testDoMergeReturnsResult() {
        MergeResult expectedResult = new MergeResult(Map.of());

        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(Collections.emptyList());
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(snap, () -> {});

        MergeHandler handler = createHandlerWithResult(snapshotSupplier, expectedResult);
        MergeResult result = handler.doMerge(new OneMerge(Collections.emptyList()));

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

    public void testSchedulerTriggerAndForceMerge() {
        MergeScheduler scheduler = createMergeScheduler();
        scheduler.triggerMerges();
        scheduler.forceMerge(1);
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
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        MergeScheduler scheduler = new MergeScheduler(
            createNoopHandler(emptySnapshotSupplier()),
            (mr, om) -> {},
            SHARD_ID,
            idxSettings,
            mockThreadPool()
        );
        scheduler.enableAutoIOThrottle();
        assertNotNull(scheduler.stats());
    }

    // ---- MergeScheduler: integration with real merge execution ----

    public void testTriggerMergesExecutesMergeThread() throws Exception {
        Segment seg = Segment.builder(1L).build();
        MergeResult mergeResult = new MergeResult(Map.of());
        CountDownLatch latch = new CountDownLatch(1);

        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(snap, () -> {});

        DataFormatAwareMergePolicy policy = mock(DataFormatAwareMergePolicy.class);
        AtomicBoolean returned = new AtomicBoolean(false);
        when(policy.findMergeCandidates(any())).thenAnswer(inv -> {
            if (returned.compareAndSet(false, true)) {
                return List.of(List.of(seg));
            }
            return Collections.emptyList();
        });

        MergeHandler handler = new MergeHandler(snapshotSupplier, policy, SHARD_ID) {
            @Override
            public MergeResult doMerge(OneMerge oneMerge) {
                latch.countDown();
                return mergeResult;
            }
        };

        AtomicReference<MergeResult> captured = new AtomicReference<>();
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> captured.set(mr), SHARD_ID, idxSettings, mockThreadPool());

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        assertNotNull(captured.get());
    }

    public void testTriggerMergesHandlesMergeFailure() throws Exception {
        Segment seg = Segment.builder(1L).build();
        CountDownLatch latch = new CountDownLatch(1);

        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(snap, () -> {});

        DataFormatAwareMergePolicy policy = mock(DataFormatAwareMergePolicy.class);
        AtomicBoolean returned = new AtomicBoolean(false);
        when(policy.findMergeCandidates(any())).thenAnswer(inv -> {
            if (returned.compareAndSet(false, true)) {
                return List.of(List.of(seg));
            }
            return Collections.emptyList();
        });

        MergeHandler handler = new MergeHandler(snapshotSupplier, policy, SHARD_ID) {
            @Override
            public MergeResult doMerge(OneMerge oneMerge) {
                latch.countDown();
                throw new RuntimeException("merge boom");
            }
        };

        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {}, SHARD_ID, idxSettings, mockThreadPool());

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
    }

    public void testForceMergeExecutesMerges() throws Exception {
        Segment seg = Segment.builder(1L).build();
        MergeResult mergeResult = new MergeResult(Map.of());

        CatalogSnapshot snap = mock(CatalogSnapshot.class);
        when(snap.getSegments()).thenReturn(List.of(seg));
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier = () -> new GatedCloseable<>(snap, () -> {});

        DataFormatAwareMergePolicy policy = mock(DataFormatAwareMergePolicy.class);
        when(policy.findForceMergeCandidates(any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(List.of(List.of(seg)));

        MergeHandler handler = new MergeHandler(snapshotSupplier, policy, SHARD_ID) {
            @Override
            public MergeResult doMerge(OneMerge oneMerge) {
                return mergeResult;
            }
        };

        AtomicReference<MergeResult> captured = new AtomicReference<>();
        Settings settings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> captured.set(mr), SHARD_ID, idxSettings, mockThreadPool());

        scheduler.forceMerge(1);
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
}

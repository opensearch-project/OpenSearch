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
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
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
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        when(tp.executor(eq(ThreadPool.Names.FORCE_MERGE))).thenReturn(daemonPool());
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
        return new MergeHandler(snapshotSupplier, noopMerger, SHARD_ID, NOOP_MERGE_POLICY, NOOP_MERGE_LISTENER, () -> 1L, null);
    }

    private MergeHandler createHandlerWithRealPolicy(Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier, Merger merger) {
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(
            new IndexSettings(newIndexMeta("test", Settings.EMPTY), Settings.EMPTY).getMergePolicy(true),
            SHARD_ID
        );
        return new MergeHandler(snapshotSupplier, merger, SHARD_ID, policy, policy, () -> 1L, null);
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
        MergeHandler handler = createNoopHandler(emptySnapshotSupplier());
        OneMerge merge = new OneMerge(Collections.emptyList());
        handler.registerMerge(merge);
        handler.findAndRegisterMerges();
        handler.onMergeFinished(merge);
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

        handler.onMergeFinished(merge);
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
        WriterFileSet inputWfs = new WriterFileSet(dir.toString(), 1L, Set.of("input.dat"), 10);
        Segment seg = Segment.builder(1L).addSearchableFiles(format, inputWfs).build();

        WriterFileSet mergedWfs = new WriterFileSet(dir.toString(), 99L, Set.of("merged.dat"), 10);
        MergeResult expectedResult = new MergeResult(Map.of(format, mergedWfs));
        Merger merger = mergeInput -> expectedResult;

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(seg)),
            merger,
            SHARD_ID,
            NOOP_MERGE_POLICY,
            NOOP_MERGE_LISTENER,
            () -> 1L,
            null
        );
        MergeResult result = handler.doMerge(new OneMerge(List.of(seg)));

        assertSame(expectedResult, result);
    }

    public void testDoMergeWithNullDeleteEngineUsesEmptyLiveDocs() throws IOException {
        Path dir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        WriterFileSet inputWfs = new WriterFileSet(dir.toString(), 1L, Set.of("input.dat"), 10);
        Segment seg = Segment.builder(1L).addSearchableFiles(format, inputWfs).build();

        AtomicReference<MergeInput> captured = new AtomicReference<>();
        WriterFileSet mergedWfs = new WriterFileSet(dir.toString(), 99L, Set.of("merged.dat"), 10);
        Merger merger = input -> {
            captured.set(input);
            return new MergeResult(Map.of(format, mergedWfs));
        };

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(seg)),
            merger,
            SHARD_ID,
            NOOP_MERGE_POLICY,
            NOOP_MERGE_LISTENER,
            () -> 1L,
            null
        );
        handler.doMerge(new OneMerge(List.of(seg)));

        assertNotNull(captured.get());
        assertTrue("null delete engine ⇒ empty live-docs map", captured.get().liveDocsPerSegment().isEmpty());
    }

    public void testDoMergeCallsDeleteEngineWithMergeSegments() throws IOException {
        Path dir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        Segment seg1 = Segment.builder(1L).addSearchableFiles(format, new WriterFileSet(dir.toString(), 1L, Set.of("a.dat"), 5)).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(format, new WriterFileSet(dir.toString(), 2L, Set.of("b.dat"), 5)).build();

        DeleteExecutionEngine<?> deleteEngine = mock(DeleteExecutionEngine.class);
        when(deleteEngine.getLiveDocsForSegments(anyList())).thenReturn(Map.of());

        WriterFileSet mergedWfs = new WriterFileSet(dir.toString(), 99L, Set.of("merged.dat"), 10);
        Merger merger = input -> new MergeResult(Map.of(format, mergedWfs));

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(seg1, seg2)),
            merger,
            SHARD_ID,
            NOOP_MERGE_POLICY,
            NOOP_MERGE_LISTENER,
            () -> 99L,
            deleteEngine
        );
        handler.doMerge(new OneMerge(List.of(seg1, seg2)));

        ArgumentCaptor<List<Segment>> captor = ArgumentCaptor.forClass(List.class);
        verify(deleteEngine, times(1)).getLiveDocsForSegments(captor.capture());
        List<Segment> actualSegments = captor.getValue();
        assertEquals(List.of(seg1, seg2), actualSegments);
    }

    public void testDoMergeForwardsLiveDocsToMerger() throws IOException {
        Path dir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        Segment seg = Segment.builder(1L).addSearchableFiles(format, new WriterFileSet(dir.toString(), 1L, Set.of("a.dat"), 10)).build();

        // Bitmap with row 0 dead, rest alive.
        long[] bits = new long[] { ~1L };
        Map<Long, long[]> liveDocs = Map.of(1L, bits);

        DeleteExecutionEngine<?> deleteEngine = mock(DeleteExecutionEngine.class);
        when(deleteEngine.getLiveDocsForSegments(anyList())).thenReturn(liveDocs);

        AtomicReference<MergeInput> captured = new AtomicReference<>();
        WriterFileSet mergedWfs = new WriterFileSet(dir.toString(), 99L, Set.of("merged.dat"), 9);
        Merger merger = input -> {
            captured.set(input);
            return new MergeResult(Map.of(format, mergedWfs));
        };

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(seg)),
            merger,
            SHARD_ID,
            NOOP_MERGE_POLICY,
            NOOP_MERGE_LISTENER,
            () -> 99L,
            deleteEngine
        );
        handler.doMerge(new OneMerge(List.of(seg)));

        assertNotNull(captured.get());
        long[] forwarded = captured.get().getLiveDocsForSegment(1L);
        assertNotNull(forwarded);
        assertArrayEquals(bits, forwarded);
    }

    public void testDoMergePropagatesDeleteEngineIOException() throws IOException {
        Path dir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        Segment seg = Segment.builder(1L).addSearchableFiles(format, new WriterFileSet(dir.toString(), 1L, Set.of("a.dat"), 10)).build();

        DeleteExecutionEngine<?> deleteEngine = mock(DeleteExecutionEngine.class);
        when(deleteEngine.getLiveDocsForSegments(anyList())).thenThrow(new IOException("delete engine blew up"));

        WriterFileSet mergedWfs = new WriterFileSet(dir.toString(), 99L, Set.of("merged.dat"), 10);
        Merger merger = input -> new MergeResult(Map.of(format, mergedWfs));

        MergeHandler handler = new MergeHandler(
            snapshotSupplierOf(List.of(seg)),
            merger,
            SHARD_ID,
            NOOP_MERGE_POLICY,
            NOOP_MERGE_LISTENER,
            () -> 99L,
            deleteEngine
        );
        IOException ex = expectThrows(IOException.class, () -> handler.doMerge(new OneMerge(List.of(seg))));
        assertEquals("delete engine blew up", ex.getMessage());
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

    public void testSchedulerTriggerAndForceMerge() throws IOException {
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
        Settings autoThrottleSettings = Settings.builder()
            .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), "1")
            .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), "6")
            .put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), "true")
            .build();
        IndexSettings idxSettings = new IndexSettings(newIndexMeta("test", autoThrottleSettings), Settings.EMPTY);
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
        List<Segment> segments = createSegments(15);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 15);
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

        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {}, SHARD_ID, mergeSchedulerSettings(), mockThreadPool());

        scheduler.triggerMerges();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
    }

    public void testForceMergeExecutesMerges() throws Exception {
        List<Segment> segments = createSegments(3);
        MockDataFormat format = new MockDataFormat();
        WriterFileSet mergedWfs = new WriterFileSet(createTempDir().toString(), 99L, Set.of("merged.dat"), 3);
        MergeResult mergeResult = new MergeResult(Map.of(format, mergedWfs));
        CountDownLatch latch = new CountDownLatch(1);

        Merger merger = mergeInput -> mergeResult;
        MergeHandler handler = createHandlerWithRealPolicy(snapshotSupplierOf(segments), merger);

        AtomicReference<MergeResult> captured = new AtomicReference<>();
        MergeScheduler scheduler = new MergeScheduler(handler, (mr, om) -> {
            captured.set(mr);
            latch.countDown();
        }, SHARD_ID, mergeSchedulerSettings(), mockThreadPool());

        scheduler.forceMerge(1);
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
}

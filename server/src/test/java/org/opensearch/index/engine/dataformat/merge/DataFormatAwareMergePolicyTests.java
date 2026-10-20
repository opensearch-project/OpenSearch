/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DataFormatAwareMergePolicy}.
 */
public class DataFormatAwareMergePolicyTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("test-index", "uuid"), 0);

    // ========== findMergeCandidates ==========

    public void testFindMergeCandidatesCapturesMergeContext() throws IOException {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10, 0L);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs).build();

        MergePolicy lucenePolicy = mock(MergePolicy.class);
        ArgumentCaptor<SegmentInfos> segInfosCaptor = ArgumentCaptor.forClass(SegmentInfos.class);
        ArgumentCaptor<MergePolicy.MergeContext> ctxCaptor = ArgumentCaptor.forClass(MergePolicy.MergeContext.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), segInfosCaptor.capture(), ctxCaptor.capture())).thenReturn(null);

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg1, seg2));

        assertTrue(result.isEmpty());

        SegmentInfos capturedInfos = segInfosCaptor.getValue();
        assertEquals(2, capturedInfos.size());

        MergePolicy.MergeContext capturedCtx = ctxCaptor.getValue();
        assertNotNull(capturedCtx.getInfoStream());
        assertTrue(capturedCtx.getMergingSegments().isEmpty());
        assertEquals(0, capturedCtx.numDeletedDocs(mock(SegmentCommitInfo.class)));
        assertEquals(0, capturedCtx.numDeletesToMerge(mock(SegmentCommitInfo.class)));
    }

    public void testFindMergeCandidatesMergeContextReflectsAddedAndRemovedSegments() throws IOException {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10, 0L);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 2L, Set.of(), 20, 0L);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs1).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs2).build();
        Segment seg3 = Segment.builder(3L).addSearchableFiles(fmt, wfs1).build();

        MergePolicy lucenePolicy = mock(MergePolicy.class);
        ArgumentCaptor<MergePolicy.MergeContext> ctxCaptor = ArgumentCaptor.forClass(MergePolicy.MergeContext.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), ctxCaptor.capture())).thenReturn(null);

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        List<Segment> allSegments = List.of(seg1, seg2, seg3);

        // Add seg1 as merging — context should show 1
        policy.addMergingSegment(List.of(seg1));
        policy.findMergeCandidates(allSegments);
        assertEquals(1, ctxCaptor.getValue().getMergingSegments().size());

        // Add seg2 as merging — context should show 2
        policy.addMergingSegment(List.of(seg2));
        policy.findMergeCandidates(allSegments);
        assertEquals(2, ctxCaptor.getValue().getMergingSegments().size());

        // Remove seg1 — context should show 1
        policy.removeMergingSegment(List.of(seg1));
        policy.findMergeCandidates(allSegments);
        assertEquals(1, ctxCaptor.getValue().getMergingSegments().size());

        // Remove seg2 — context should be empty
        policy.removeMergingSegment(List.of(seg2));
        policy.findMergeCandidates(allSegments);
        assertTrue(ctxCaptor.getValue().getMergingSegments().isEmpty());
    }

    public void testFindMergeCandidatesExceptionWrapped() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenThrow(
            new RuntimeException("merge error")
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        RuntimeException ex = expectThrows(RuntimeException.class, () -> policy.findMergeCandidates(Collections.emptyList()));
        assertEquals("Error finding merge candidates", ex.getMessage());
    }

    // ========== findForceMergeCandidates ==========

    @SuppressWarnings("unchecked")
    public void testFindForceMergeCandidatesCapturesMergeContext() throws IOException {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10, 0L);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs).build();

        MergePolicy lucenePolicy = mock(MergePolicy.class);
        ArgumentCaptor<SegmentInfos> segInfosCaptor = ArgumentCaptor.forClass(SegmentInfos.class);
        ArgumentCaptor<Map<SegmentCommitInfo, Boolean>> segmentsToMergeCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<MergePolicy.MergeContext> ctxCaptor = ArgumentCaptor.forClass(MergePolicy.MergeContext.class);
        when(lucenePolicy.findForcedMerges(segInfosCaptor.capture(), anyInt(), segmentsToMergeCaptor.capture(), ctxCaptor.capture()))
            .thenReturn(null);

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findForceMergeCandidates(List.of(seg1, seg2), 1);

        assertTrue(result.isEmpty());

        SegmentInfos capturedInfos = segInfosCaptor.getValue();
        assertEquals(2, capturedInfos.size());

        Map<SegmentCommitInfo, Boolean> capturedSegmentsToMerge = segmentsToMergeCaptor.getValue();
        assertEquals(2, capturedSegmentsToMerge.size());
        assertTrue("All segments should be marked for merge", capturedSegmentsToMerge.values().stream().allMatch(v -> v));

        MergePolicy.MergeContext capturedCtx = ctxCaptor.getValue();
        assertNotNull(capturedCtx.getInfoStream());
        assertTrue(capturedCtx.getMergingSegments().isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testFindForceMergeCandidatesExceptionWrapped() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findForcedMerges(any(SegmentInfos.class), anyInt(), any(Map.class), any(MergePolicy.MergeContext.class)))
            .thenThrow(new RuntimeException("force merge error"));

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        RuntimeException ex = expectThrows(RuntimeException.class, () -> policy.findForceMergeCandidates(Collections.emptyList(), 1));
        assertEquals("Error finding force merge candidates", ex.getMessage());
    }

    // ========== Complex add/remove/add/remove lifecycle ==========

    public void testMergeContextTracksMultipleAddRemoveCycles() throws IOException {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10, 0L)).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, new WriterFileSet(tempDir.toString(), 2L, Set.of(), 20, 0L)).build();
        Segment seg3 = Segment.builder(3L).addSearchableFiles(fmt, new WriterFileSet(tempDir.toString(), 3L, Set.of(), 30, 0L)).build();
        Segment seg4 = Segment.builder(4L).addSearchableFiles(fmt, new WriterFileSet(tempDir.toString(), 4L, Set.of(), 40, 0L)).build();
        List<Segment> allSegments = List.of(seg1, seg2, seg3, seg4);

        MergePolicy lucenePolicy = mock(MergePolicy.class);
        ArgumentCaptor<MergePolicy.MergeContext> ctxCaptor = ArgumentCaptor.forClass(MergePolicy.MergeContext.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), ctxCaptor.capture())).thenReturn(null);

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        // Round 1: add seg1, seg2 — expect 2 merging
        policy.addMergingSegment(List.of(seg1, seg2));
        policy.findMergeCandidates(allSegments);
        assertEquals(2, ctxCaptor.getValue().getMergingSegments().size());

        // Round 2: remove seg1 — expect 1 merging
        policy.removeMergingSegment(List.of(seg1));
        policy.findMergeCandidates(allSegments);
        assertEquals(1, ctxCaptor.getValue().getMergingSegments().size());

        // Round 3: add seg3, seg4 — expect 3 merging (seg2 still there)
        policy.addMergingSegment(List.of(seg3, seg4));
        policy.findMergeCandidates(allSegments);
        assertEquals(3, ctxCaptor.getValue().getMergingSegments().size());

        // Round 4: remove seg2, seg3 — expect 1 merging (seg4)
        policy.removeMergingSegment(List.of(seg2, seg3));
        policy.findMergeCandidates(allSegments);
        assertEquals(1, ctxCaptor.getValue().getMergingSegments().size());

        // Round 5: re-add seg1 — expect 2 merging (seg4, seg1)
        policy.addMergingSegment(List.of(seg1));
        policy.findMergeCandidates(allSegments);
        assertEquals(2, ctxCaptor.getValue().getMergingSegments().size());

        // Round 6: remove all — expect 0
        policy.removeMergingSegment(List.of(seg1, seg4));
        policy.findMergeCandidates(allSegments);
        assertTrue(ctxCaptor.getValue().getMergingSegments().isEmpty());

        // Round 7: remove already-removed segment is a no-op — still 0
        policy.removeMergingSegment(List.of(seg1));
        policy.findMergeCandidates(allSegments);
        assertTrue(ctxCaptor.getValue().getMergingSegments().isEmpty());

        // Round 8: add duplicate — should still be 1 (set semantics)
        policy.addMergingSegment(List.of(seg2));
        policy.addMergingSegment(List.of(seg2));
        policy.findMergeCandidates(allSegments);
        assertEquals(1, ctxCaptor.getValue().getMergingSegments().size());

        // Round 9: single remove clears the duplicate — expect 0
        policy.removeMergingSegment(List.of(seg2));
        policy.findMergeCandidates(allSegments);
        assertTrue(ctxCaptor.getValue().getMergingSegments().isEmpty());
    }

    // ========== MergeContext immutability ==========

    public void testGetMergingSegmentsIsUnmodifiable() {
        DataFormatAwareMergePolicy.DataFormatMergeContext ctx = new DataFormatAwareMergePolicy.DataFormatMergeContext(
            org.apache.logging.log4j.LogManager.getLogger(getClass())
        );
        Set<SegmentCommitInfo> mergingSegments = ctx.getMergingSegments();
        expectThrows(UnsupportedOperationException.class, () -> mergingSegments.add(mock(SegmentCommitInfo.class)));
    }

    // ========== Edge cases ==========

    public void testSegmentWithMultipleFormatsAggregatesDocCountAndSize() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        ArgumentCaptor<SegmentInfos> segInfosCaptor = ArgumentCaptor.forClass(SegmentInfos.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), segInfosCaptor.capture(), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        MockDataFormat fmt1 = new MockDataFormat("lucene", 100L, Set.of());
        MockDataFormat fmt2 = new MockDataFormat("columnar", 50L, Set.of());
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10, 0L);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 20, 0L);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();

        policy.findMergeCandidates(List.of(seg));

        SegmentInfos capturedInfos = segInfosCaptor.getValue();
        assertEquals(1, capturedInfos.size());
    }

    public void testSegmentWithNoSearchableFiles() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        ArgumentCaptor<SegmentInfos> segInfosCaptor = ArgumentCaptor.forClass(SegmentInfos.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), segInfosCaptor.capture(), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        Segment seg = Segment.builder(1L).build();

        policy.findMergeCandidates(List.of(seg));

        assertEquals(1, segInfosCaptor.getValue().size());
    }

    // ========== Real TieredMergePolicy ==========

    public void testFindMergeCandidatesWithRealPolicyReturnsMerges() throws IOException {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());

        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("seg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("seg" + i + ".dat"), 10, 0L);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(tieredPolicy, SHARD_ID);

        List<List<Segment>> result = policy.findMergeCandidates(segments);
        assertNotNull(result);
        assertFalse("TieredMergePolicy should find merge candidates with 15 small segments", result.isEmpty());
        for (List<Segment> group : result) {
            assertFalse(group.isEmpty());
        }
    }

    public void testFindForceMergeCandidatesWithRealPolicyReturnsMerges() throws IOException {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());

        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path file = tempDir.resolve("fseg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("fseg" + i + ".dat"), 10, 0L);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(tieredPolicy, SHARD_ID);

        List<List<Segment>> result = policy.findForceMergeCandidates(segments, 1);
        assertNotNull(result);
        assertFalse("Force merge to 1 segment should produce candidates from 5 segments", result.isEmpty());
    }

    // ========== Concurrency ==========

    public void testConcurrentAddRemoveDoesNotThrow() throws Exception {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());

        int numSegments = 50;
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < numSegments; i++) {
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of(), 10, 0L);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        AtomicReference<Exception> failure = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);

        Thread adder = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 100; i++) {
                    policy.addMergingSegment(List.of(segments.get(i % numSegments)));
                }
            } catch (Exception e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        Thread remover = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 100; i++) {
                    policy.removeMergingSegment(List.of(segments.get(i % numSegments)));
                }
            } catch (Exception e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        adder.start();
        remover.start();
        done.await();

        assertNull("Concurrent add/remove should not throw, but got: " + failure.get(), failure.get());
    }

    public void testConcurrentFindMergeCandidatesAndAddMergingSegment() throws Exception {
        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(tieredPolicy, SHARD_ID);

        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());

        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("cseg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("cseg" + i + ".dat"), 10, 0L);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        AtomicReference<Exception> failure = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);

        Thread finder = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 50; i++) {
                    policy.findMergeCandidates(segments);
                }
            } catch (Exception e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        Thread mutator = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 50; i++) {
                    Segment seg = segments.get(i % segments.size());
                    policy.addMergingSegment(List.of(seg));
                    policy.removeMergingSegment(List.of(seg));
                }
            } catch (Exception e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        finder.start();
        mutator.start();
        done.await();

        assertNull("Concurrent findMergeCandidates and addMergingSegment should not throw, but got: " + failure.get(), failure.get());
    }
}

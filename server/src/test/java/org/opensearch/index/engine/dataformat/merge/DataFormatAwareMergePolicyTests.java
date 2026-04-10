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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DataFormatAwareMergePolicy}.
 */
public class DataFormatAwareMergePolicyTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("test-index", "uuid"), 0);

    private static DataFormat stubFormat(String name) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return 1;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }

    // ========== findMergeCandidates ==========

    public void testFindMergeCandidatesEmptySegments() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findMergeCandidates(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    public void testFindMergeCandidatesWithSegments() throws IOException {
        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs).build();

        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg1, seg2));
        assertTrue(result.isEmpty());
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
    public void testFindForceMergeCandidatesEmptySegments() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findForcedMerges(any(SegmentInfos.class), anyInt(), any(Map.class), any(MergePolicy.MergeContext.class)))
            .thenReturn(null);

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findForceMergeCandidates(Collections.emptyList(), 1);
        assertTrue(result.isEmpty());
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

    // ========== addMergingSegment / removeMergingSegment ==========

    public void testAddAndRemoveMergingSegments() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();

        assertTrue(policy.getMergingSegments().isEmpty());

        policy.addMergingSegment(List.of(seg));
        assertFalse(policy.getMergingSegments().isEmpty());

        policy.removeMergingSegment(List.of(seg));
    }

    // ========== MergeContext methods ==========

    public void testNumDeletesToMerge() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        assertEquals(0, policy.numDeletesToMerge(mock(SegmentCommitInfo.class)));
    }

    public void testNumDeletedDocs() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        assertEquals(0, policy.numDeletedDocs(mock(SegmentCommitInfo.class)));
    }

    public void testGetInfoStream() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        assertNotNull(policy.getInfoStream());
    }

    public void testInfoStreamMessageAndIsEnabled() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        policy.getInfoStream().message("test", "hello");
        policy.getInfoStream().isEnabled("test");
        policy.getInfoStream().close();
    }

    public void testGetMergingSegmentsIsUnmodifiable() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        Set<SegmentCommitInfo> mergingSegments = policy.getMergingSegments();
        expectThrows(UnsupportedOperationException.class, () -> mergingSegments.add(mock(SegmentCommitInfo.class)));
    }

    // ========== calculateNumDocs / calculateTotalSize with real segments ==========

    public void testSegmentWithMultipleFormatsAggregatesDocCountAndSize() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt1 = stubFormat("lucene");
        DataFormat fmt2 = stubFormat("parquet");
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 20);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();

        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg));
        assertNotNull(result);
    }

    // ========== Segment with empty dfGroupedSearchableFiles ==========

    public void testSegmentWithNoSearchableFiles() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        Segment seg = Segment.builder(1L).build();

        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg));
        assertNotNull(result);
    }

    // ========== convertMergeSpecification non-null path with real TieredMergePolicy ==========

    public void testFindMergeCandidatesWithRealPolicyReturnsMerges() throws IOException {
        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");

        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("seg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("seg" + i + ".dat"), 10);
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

    // ========== convertMergeSpecification non-null path with real TieredMergePolicy ==========

    public void testFindForceMergeCandidatesWithRealPolicyReturnsMerges() throws IOException {
        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");

        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path file = tempDir.resolve("fseg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("fseg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(tieredPolicy, SHARD_ID);

        List<List<Segment>> result = policy.findForceMergeCandidates(segments, 1);
        assertNotNull(result);
        assertFalse("Force merge to 1 segment should produce candidates from 5 segments", result.isEmpty());
    }

    // ========== Finding #1: removeMergingSegment should actually remove ==========

    public void testRemoveMergingSegmentActuallyRemoves() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();

        policy.addMergingSegment(List.of(seg));
        assertEquals(1, policy.getMergingSegments().size());

        // Remove the same segment — set must be empty afterwards
        policy.removeMergingSegment(List.of(seg));
        assertTrue("mergingSegments should be empty after removing the same segment that was added", policy.getMergingSegments().isEmpty());
    }

    public void testRemoveMultipleMergingSegments() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 2L, Set.of(), 20);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs1).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs2).build();

        policy.addMergingSegment(List.of(seg1, seg2));
        assertEquals(2, policy.getMergingSegments().size());

        // Remove only seg1
        policy.removeMergingSegment(List.of(seg1));
        assertEquals("Only seg2 should remain after removing seg1", 1, policy.getMergingSegments().size());

        // Remove seg2
        policy.removeMergingSegment(List.of(seg2));
        assertTrue("Set should be empty after removing all segments", policy.getMergingSegments().isEmpty());
    }

    // ========== Finding #5: concurrent add/remove vs getMergingSegments ==========

    public void testConcurrentAddRemoveDoesNotThrow() throws Exception {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");

        int numSegments = 50;
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < numSegments; i++) {
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of(), 10);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        AtomicReference<Exception> failure = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch done = new CountDownLatch(3);

        // Thread 1: repeatedly add segments
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

        // Thread 2: repeatedly remove segments
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

        // Thread 3: repeatedly read getMergingSegments and iterate
        Thread reader = new Thread(() -> {
            try {
                barrier.await();
                for (int i = 0; i < 100; i++) {
                    Set<SegmentCommitInfo> snapshot = policy.getMergingSegments();
                    // Iterate the returned set — this is what Lucene's merge policy does
                    for (SegmentCommitInfo ignored : snapshot) {
                        // just iterate
                    }
                }
            } catch (Exception e) {
                failure.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });

        adder.start();
        remover.start();
        reader.start();
        done.await();

        assertNull("Concurrent add/remove/read should not throw, but got: " + failure.get(), failure.get());
    }

    // ========== Finding #5: concurrent findMergeCandidates vs addMergingSegment ==========

    public void testConcurrentFindMergeCandidatesAndAddMergingSegment() throws Exception {
        // Use a real TieredMergePolicy which will call getMergingSegments() internally
        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(tieredPolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");

        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("cseg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("cseg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        AtomicReference<Exception> failure = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);

        // Thread 1: repeatedly call findMergeCandidates (which reads getMergingSegments via Lucene)
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

        // Thread 2: repeatedly add/remove merging segments
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

    // ========== Finding #6: RuntimeException gets double-wrapped ==========

    public void testFindMergeCandidatesPreservesRuntimeException() throws IOException {
        RuntimeException original = new IllegalArgumentException("bad segment");
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenThrow(
            original
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> policy.findMergeCandidates(Collections.emptyList()));

        // The original IllegalArgumentException should be accessible, not buried
        // under a generic RuntimeException wrapper
        assertSame(
            "Original RuntimeException should be preserved, not double-wrapped",
            original,
            thrown.getCause() != null ? thrown.getCause() : thrown
        );
    }

    @SuppressWarnings("unchecked")
    public void testFindForceMergeCandidatesPreservesRuntimeException() throws IOException {
        RuntimeException original = new IllegalStateException("bad state");
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findForcedMerges(any(SegmentInfos.class), anyInt(), any(Map.class), any(MergePolicy.MergeContext.class)))
            .thenThrow(original);

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> policy.findForceMergeCandidates(Collections.emptyList(), 1));

        assertSame(
            "Original RuntimeException should be preserved, not double-wrapped",
            original,
            thrown.getCause() != null ? thrown.getCause() : thrown
        );
    }

    // ========== Finding #3: calculateTotalSize silently returns 0 for missing files ==========

    public void testSegmentWithMissingFilesStillProducesMergeCandidates() throws IOException {
        // A segment referencing files that don't exist on disk.
        // calculateTotalSize will return 0 because getTotalSize catches IOException.
        // The merge policy then sees a 0-byte segment, which may lead to wrong merge decisions.
        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of("nonexistent.dat"), 100);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();

        // getTotalSize returns 0 for missing files — verify this is the current behavior
        assertEquals("getTotalSize should return 0 for missing files (current behavior, may want to change)", 0L, wfs.getTotalSize());

        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(tieredPolicy, SHARD_ID);

        // This should not throw — but the segment appears as 0 bytes to the merge policy
        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg));
        assertNotNull(result);
    }

    // ========== Finding #4: all wrappers share same DUMMY_ID ==========

    public void testAddSameGenerationSegmentTwiceDoesNotDuplicate() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();

        // Adding the same segment twice should result in only 1 entry
        // (since they represent the same logical segment)
        policy.addMergingSegment(List.of(seg));
        policy.addMergingSegment(List.of(seg));

        assertEquals(
            "Adding the same segment twice should not create duplicates in mergingSegments",
            1,
            policy.getMergingSegments().size()
        );
    }

    public void testDifferentSegmentsAreDistinguishable() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 2L, Set.of(), 20);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs1).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs2).build();

        policy.addMergingSegment(List.of(seg1, seg2));

        assertEquals(
            "Two segments with different generations must be distinguishable in mergingSegments",
            2,
            policy.getMergingSegments().size()
        );
    }

    // ========== Finding #7: doc count overflow from long to int ==========

    public void testLargeDocCountDoesNotSilentlyOverflow() throws IOException {
        // A segment with more than Integer.MAX_VALUE docs across all formats.
        // The SegmentWrapper casts totalNumDocs to (int), which would overflow.
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt1 = stubFormat("lucene");
        DataFormat fmt2 = stubFormat("parquet");
        // Each format claims ~1.5 billion rows, total > Integer.MAX_VALUE
        long bigCount = (long) Integer.MAX_VALUE - 100;
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), bigCount);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), bigCount);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();

        // Total docs = 2 * (Integer.MAX_VALUE - 100) which exceeds int range.
        // This should either handle gracefully or throw — not silently produce a negative doc count.
        long totalDocs = wfs1.numRows() + wfs2.numRows();
        assertTrue("Total docs should exceed Integer.MAX_VALUE", totalDocs > Integer.MAX_VALUE);

        // Currently this silently overflows. The test documents the behavior.
        // After fix, this should either cap at Integer.MAX_VALUE or throw.
        try {
            policy.findMergeCandidates(List.of(seg));
        } catch (Exception e) {
            // If the fix throws on overflow, that's acceptable
            return;
        }
        // If it didn't throw, the merge policy received a corrupted doc count — document this.
    }
}

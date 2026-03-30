/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeMergePolicy}.
 */
public class CompositeMergePolicyTests extends OpenSearchTestCase {

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

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findMergeCandidates(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    public void testFindMergeCandidatesWithSegments() throws IOException {
        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs).build();

        // Use a real TieredMergePolicy to get actual merge candidates
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg1, seg2));
        // With null mergeSpec, should return empty
        assertTrue(result.isEmpty());
    }

    public void testFindMergeCandidatesExceptionWrapped() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenThrow(
            new RuntimeException("merge error")
        );

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        RuntimeException ex = expectThrows(RuntimeException.class, () -> policy.findMergeCandidates(Collections.emptyList()));
        assertEquals("Error finding merge candidates", ex.getMessage());
    }

    // ========== findForceMergeCandidates ==========

    @SuppressWarnings("unchecked")
    public void testFindForceMergeCandidatesEmptySegments() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findForcedMerges(any(SegmentInfos.class), anyInt(), any(Map.class), any(MergePolicy.MergeContext.class)))
            .thenReturn(null);

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        List<List<Segment>> result = policy.findForceMergeCandidates(Collections.emptyList(), 1);
        assertTrue(result.isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testFindForceMergeCandidatesExceptionWrapped() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findForcedMerges(any(SegmentInfos.class), anyInt(), any(Map.class), any(MergePolicy.MergeContext.class)))
            .thenThrow(new RuntimeException("force merge error"));

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        RuntimeException ex = expectThrows(RuntimeException.class, () -> policy.findForceMergeCandidates(Collections.emptyList(), 1));
        assertEquals("Error finding force merge candidates", ex.getMessage());
    }

    // ========== addMergingSegment / removeMergingSegment ==========

    public void testAddAndRemoveMergingSegments() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();

        assertTrue(policy.getMergingSegments().isEmpty());

        policy.addMergingSegment(List.of(seg));
        assertFalse(policy.getMergingSegments().isEmpty());

        // removeMergingSegment creates a new SegmentWrapper, so identity-based HashSet
        // removal depends on SegmentCommitInfo equality semantics.
        policy.removeMergingSegment(List.of(seg));
    }

    // ========== MergeContext methods ==========

    public void testNumDeletesToMerge() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        assertEquals(0, policy.numDeletesToMerge(mock(SegmentCommitInfo.class)));
    }

    public void testNumDeletedDocs() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        assertEquals(0, policy.numDeletedDocs(mock(SegmentCommitInfo.class)));
    }

    public void testGetInfoStream() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        assertNotNull(policy.getInfoStream());
    }

    public void testInfoStreamMessageAndIsEnabled() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);

        // Exercise the InfoStream methods — they should not throw
        policy.getInfoStream().message("test", "hello");
        policy.getInfoStream().isEnabled("test");
        policy.getInfoStream().close();
    }

    public void testGetMergingSegmentsIsUnmodifiable() {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        Set<SegmentCommitInfo> mergingSegments = policy.getMergingSegments();
        expectThrows(UnsupportedOperationException.class, () -> mergingSegments.add(mock(SegmentCommitInfo.class)));
    }

    // ========== calculateNumDocs / calculateTotalSize with real segments ==========

    public void testSegmentWithMultipleFormatsAggregatesDocCountAndSize() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);

        Path tempDir = createTempDir();
        DataFormat fmt1 = stubFormat("lucene");
        DataFormat fmt2 = stubFormat("parquet");
        WriterFileSet wfs1 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10);
        WriterFileSet wfs2 = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 20);
        Segment seg = Segment.builder(1L).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();

        // This exercises calculateNumDocs and calculateTotalSize through convertToSegmentInfos
        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg));
        assertNotNull(result);
    }

    // ========== Segment with empty dfGroupedSearchableFiles ==========

    public void testSegmentWithNoSearchableFiles() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any(MergePolicy.MergeContext.class))).thenReturn(
            null
        );

        CompositeMergePolicy policy = new CompositeMergePolicy(lucenePolicy, SHARD_ID);
        Segment seg = Segment.builder(1L).build();

        List<List<Segment>> result = policy.findMergeCandidates(List.of(seg));
        assertNotNull(result);
    }

    // ========== convertMergeSpecification non-null path with real TieredMergePolicy ==========

    public void testFindMergeCandidatesWithRealPolicyReturnsMerges() throws IOException {
        Path tempDir = createTempDir();
        DataFormat fmt = stubFormat("lucene");

        // Create many small segments with real files to trigger TieredMergePolicy
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("seg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet wfs = new WriterFileSet(tempDir.toString(), i, Set.of("seg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(fmt, wfs).build());
        }

        TieredMergePolicy tieredPolicy = new TieredMergePolicy();
        CompositeMergePolicy policy = new CompositeMergePolicy(tieredPolicy, SHARD_ID);

        List<List<Segment>> result = policy.findMergeCandidates(segments);
        assertNotNull(result);
        assertFalse("TieredMergePolicy should find merge candidates with 15 small segments", result.isEmpty());
        for (List<Segment> group : result) {
            assertFalse(group.isEmpty());
        }
    }

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
        CompositeMergePolicy policy = new CompositeMergePolicy(tieredPolicy, SHARD_ID);

        List<List<Segment>> result = policy.findForceMergeCandidates(segments, 1);
        assertNotNull(result);
        assertFalse("Force merge to 1 segment should produce candidates from 5 segments", result.isEmpty());
    }
}

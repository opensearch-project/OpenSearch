/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MergeHandler}: the merge queue, pending-count accounting,
 * candidate registration/dedup, and the merge lifecycle callbacks.
 * <p>
 * These exercise the pending-merge tracking surface ({@link MergeHandler#hasPendingMerges()},
 * {@link MergeHandler#getPendingMergeCount()}) that tiering preparation relies on to decide
 * whether a shard's merges have drained.
 */
public class MergeHandlerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("idx", "uuid"), 0);

    private Merger merger;
    private MergeHandler.MergePolicy mergePolicy;
    private MergeHandler.MergeListener mergeListener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        merger = mock(Merger.class);
        mergePolicy = mock(MergeHandler.MergePolicy.class);
        mergeListener = mock(MergeHandler.MergeListener.class);
    }

    private static Segment seg(long generation) {
        return new Segment(generation, Map.of());
    }

    /** Builds a snapshot supplier that always exposes the given segments as the current catalog. */
    private Supplier<GatedCloseable<CatalogSnapshot>> snapshotWith(Segment... segments) {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segments));
        // Return a fresh GatedCloseable per call so try-with-resources close() is always valid.
        return () -> new GatedCloseable<>(snapshot, () -> {});
    }

    private MergeHandler newHandler(Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier) {
        return new MergeHandler(snapshotSupplier, merger, SHARD_ID, mergePolicy, mergeListener, () -> 1L);
    }

    public void testEmptyHandler_HasNoPendingMerges() {
        MergeHandler handler = newHandler(snapshotWith());
        assertFalse(handler.hasPendingMerges());
        assertEquals(0, handler.getPendingMergeCount());
        assertNull("getNextMerge on empty queue returns null", handler.getNextMerge());
    }

    public void testRegisterMerge_AddsToPendingAndNotifiesListener() {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        MergeHandler handler = newHandler(snapshotWith(s1, s2));

        OneMerge merge = new OneMerge(List.of(s1, s2));
        handler.registerMerge(merge);

        assertTrue(handler.hasPendingMerges());
        assertEquals(1, handler.getPendingMergeCount());
        verify(mergeListener).addMergingSegment(merge.getSegmentsToMerge());
    }

    public void testRegisterMerge_SkipsWhenSegmentNotInCatalog() {
        Segment inCatalog = seg(1);
        Segment stale = seg(99);
        // Catalog only contains inCatalog; a merge referencing a stale segment must be ignored.
        MergeHandler handler = newHandler(snapshotWith(inCatalog));

        handler.registerMerge(new OneMerge(List.of(stale)));

        assertFalse(handler.hasPendingMerges());
        assertEquals(0, handler.getPendingMergeCount());
        verify(mergeListener, never()).addMergingSegment(any());
    }

    public void testFindAndRegisterMerges_RegistersPolicyCandidates() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        MergeHandler handler = newHandler(snapshotWith(s1, s2));
        when(mergePolicy.findMergeCandidates(any())).thenReturn(List.of(List.of(s1, s2)));

        handler.findAndRegisterMerges();

        assertEquals(1, handler.getPendingMergeCount());
        verify(mergeListener).addMergingSegment(List.of(s1, s2));
    }

    public void testFindAndRegisterMerges_SkipsCandidatesOverlappingActiveMerge() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);
        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3));

        // s1+s2 already registered (and thus "currently merging").
        handler.registerMerge(new OneMerge(List.of(s1, s2)));
        assertEquals(1, handler.getPendingMergeCount());

        // Policy proposes a candidate that overlaps s2 — it must be skipped.
        when(mergePolicy.findMergeCandidates(any())).thenReturn(List.of(List.of(s2, s3)));
        handler.findAndRegisterMerges();

        assertEquals("Overlapping candidate must not be registered", 1, handler.getPendingMergeCount());
    }

    public void testGetNextMerge_IsFifoThenNull() {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);
        Segment s4 = seg(4);
        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3, s4));

        OneMerge first = new OneMerge(List.of(s1, s2));
        OneMerge second = new OneMerge(List.of(s3, s4));
        handler.registerMerge(first);
        handler.registerMerge(second);
        assertEquals(2, handler.getPendingMergeCount());

        assertSame(first, handler.getNextMerge());
        assertSame(second, handler.getNextMerge());
        assertNull(handler.getNextMerge());
        assertEquals(0, handler.getPendingMergeCount());
    }

    public void testOnMergeFinished_NotFrozen_ReRegistersNewCandidates() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);
        Segment s4 = seg(4);
        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3, s4));

        OneMerge finished = new OneMerge(List.of(s1, s2));
        handler.registerMerge(finished);
        assertEquals(1, handler.getPendingMergeCount());

        // After this merge finishes (not frozen), the handler should look for new merges.
        when(mergePolicy.findMergeCandidates(any())).thenReturn(List.of(List.of(s3, s4)));
        handler.onMergeFinished(finished, false);

        // finished removed; new [s3,s4] registered.
        assertEquals(1, handler.getPendingMergeCount());
        verify(mergeListener).removeMergingSegment(finished.getSegmentsToMerge());
        verify(mergeListener).addMergingSegment(List.of(s3, s4));
    }

    public void testOnMergeFinished_Frozen_DoesNotReRegister() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);
        Segment s4 = seg(4);
        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3, s4));

        OneMerge finished = new OneMerge(List.of(s1, s2));
        handler.registerMerge(finished);
        assertEquals(1, handler.getPendingMergeCount());

        // Even if the policy would propose new work, a frozen handler must not register it.
        when(mergePolicy.findMergeCandidates(any())).thenReturn(List.of(List.of(s3, s4)));
        handler.onMergeFinished(finished, true);

        assertEquals("Frozen handler must not register new merges", 0, handler.getPendingMergeCount());
        assertFalse(handler.hasPendingMerges());
        verify(mergeListener).removeMergingSegment(finished.getSegmentsToMerge());
        verify(mergePolicy, never()).findMergeCandidates(any());
    }

    public void testOnMergeFailure_ClearsMergingSegments() {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        MergeHandler handler = newHandler(snapshotWith(s1, s2));

        OneMerge failing = new OneMerge(List.of(s1, s2));
        handler.registerMerge(failing);
        assertEquals(1, handler.getPendingMergeCount());

        handler.onMergeFailure(failing);

        assertEquals(0, handler.getPendingMergeCount());
        assertFalse(handler.hasPendingMerges());
        verify(mergeListener).removeMergingSegment(failing.getSegmentsToMerge());
    }

    public void testDoMerge_DelegatesToMerger() throws Exception {
        Segment s1 = seg(1);
        MergeHandler handler = newHandler(snapshotWith(s1));

        MergeResult result = mock(MergeResult.class);
        WriterFileSet mergedFiles = new WriterFileSet("dir", 1L, java.util.Set.of("merged.file"), 0L, 0L);
        when(result.getMergedWriterFileSet()).thenReturn(Map.of(mock(DataFormat.class), mergedFiles));
        when(merger.merge(any(MergeInput.class))).thenReturn(result);

        OneMerge merge = new OneMerge(List.of(s1));
        MergeResult actual = handler.doMerge(merge);

        assertSame(result, actual);
        verify(merger).merge(any(MergeInput.class));
    }

    /**
     * Verifies that {@code findForceMerges} excludes segments that are already being merged
     * by a background merge. Without this fix, a concurrent force merge + background merge
     * could pick the same segments, causing the Lucene secondary to merge fewer docs than
     * parquet (row count mismatch).
     */
    public void testFindForceMergesExcludesAlreadyMergingSegments() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);
        Segment s4 = seg(4);

        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3, s4));

        // Simulate merge policy returning all segments as force merge candidates
        when(mergePolicy.findForceMergeCandidates(any(), any(Integer.class))).thenReturn(List.of(List.of(s1, s2, s3, s4)));

        // Register a background merge for segments [s1, s2] — marks them as currently merging
        OneMerge backgroundMerge = new OneMerge(List.of(s1, s2));
        handler.registerMerge(backgroundMerge);

        // Now findForceMerges should exclude s1 and s2 since they're already merging
        var forceMerges = handler.findForceMerges(1);

        // The force merge group [s1, s2, s3, s4] contains merging segments → should be filtered out
        assertTrue("Force merge should not pick segments already being merged by background merge", forceMerges.isEmpty());
    }

    /**
     * Verifies that {@code findForceMerges} returns candidates when no segments overlap
     * with currently merging segments.
     */
    public void testFindForceMergesReturnsNonConflictingCandidates() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);
        Segment s4 = seg(4);

        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3, s4));

        // Merge policy returns two groups: [s1, s2] and [s3, s4]
        when(mergePolicy.findForceMergeCandidates(any(), any(Integer.class))).thenReturn(List.of(List.of(s1, s2), List.of(s3, s4)));

        // Register background merge for [s1, s2]
        OneMerge backgroundMerge = new OneMerge(List.of(s1, s2));
        handler.registerMerge(backgroundMerge);

        // findForceMerges should return only the non-conflicting group [s3, s4]
        var forceMerges = handler.findForceMerges(1);

        assertEquals("Should return 1 non-conflicting merge group", 1, forceMerges.size());
        OneMerge remaining = forceMerges.iterator().next();
        assertEquals(2, remaining.getSegmentsToMerge().size());
        assertTrue(remaining.getSegmentsToMerge().contains(s3));
        assertTrue(remaining.getSegmentsToMerge().contains(s4));
    }

    /**
     * Verifies that {@code findForceMerges} does NOT register conflicting merge groups
     * in currentlyMergingSegments. Only non-conflicting groups should be registered.
     */
    public void testFindForceMergesOnlyRegistersNonConflictingGroups() throws Exception {
        Segment s1 = seg(1);
        Segment s2 = seg(2);
        Segment s3 = seg(3);

        MergeHandler handler = newHandler(snapshotWith(s1, s2, s3));

        // Merge policy returns two groups: [s1, s2] (will conflict) and [s3] (won't conflict)
        when(mergePolicy.findForceMergeCandidates(any(), any(Integer.class))).thenReturn(List.of(List.of(s1, s2), List.of(s3)));

        // Register background merge for s1 — makes [s1, s2] group conflicting
        handler.registerMerge(new OneMerge(List.of(s1)));

        // findForceMerges should only return and register [s3]
        var forceMerges = handler.findForceMerges(1);
        assertEquals(1, forceMerges.size());

        // s2 should NOT be stuck in currentlyMergingSegments
        // Verify by registering a merge containing s2 — should succeed
        OneMerge s2Merge = new OneMerge(List.of(s2));
        handler.registerMerge(s2Merge);
        // background s1 is pending (from registerMerge), force s3 is NOT pending (only in currentlyMerging),
        // newly registered s2 is pending → 2 pending total
        assertEquals(2, handler.getPendingMergeCount());
    }
}

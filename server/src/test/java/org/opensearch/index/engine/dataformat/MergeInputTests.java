/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.exec.Segment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MergeInput} live-docs plumbing and the {@link Merger}
 * prepare/abort default implementations.
 */
public class MergeInputTests extends OpenSearchTestCase {

    // ========== liveDocs defaults and validation ==========

    public void testBuilderDefaultsLiveDocsToAllAlive() {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).build();
        assertSame(LiveDocs.ALL_ALIVE, input.liveDocs());
    }

    public void testBuilderRejectsNullLiveDocs() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> MergeInput.builder().liveDocs(null));
        assertTrue(e.getMessage().contains("liveDocs must not be null"));
    }

    public void testCanonicalConstructorRejectsNullLiveDocs() {
        NullPointerException e = expectThrows(NullPointerException.class, () -> new MergeInput(List.of(), null, 1L, null));
        assertTrue(e.getMessage().contains("liveDocs must not be null"));
    }

    public void testBuilderCarriesExplicitLiveDocs() {
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(3L, new long[] { 0b1L }));
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).liveDocs(liveDocs).build();
        assertSame(liveDocs, input.liveDocs());
    }

    // ========== getLiveDocsForSegment ==========

    public void testGetLiveDocsForSegmentDelegatesToLiveDocs() {
        long[] bits = new long[] { 0b101L };
        LiveDocs liveDocs = LiveDocs.fromPackedBits(Map.of(3L, bits));
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).liveDocs(liveDocs).build();

        assertArrayEquals(bits, input.getLiveDocsForSegment(3L));
        assertNull("segments without deletes must return null", input.getLiveDocsForSegment(4L));
    }

    public void testGetLiveDocsForSegmentAllAliveReturnsNull() {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).build();
        assertNull(input.getLiveDocsForSegment(randomLong()));
    }

    // ========== segments immutability ==========

    public void testSegmentsAreCopiedDefensively() {
        List<Segment> segments = new java.util.ArrayList<>();
        segments.add(Segment.builder(1L).build());
        MergeInput input = MergeInput.builder().segments(segments).newWriterGeneration(1L).build();

        segments.add(Segment.builder(2L).build());
        assertEquals("MergeInput must copy the segment list", 1, input.segments().size());
        expectThrows(UnsupportedOperationException.class, () -> input.segments().add(Segment.builder(3L).build()));
    }

    // ========== Merger default prepare/abort hooks ==========

    public void testMergerDefaultPrepareMergeReturnsAllAlive() throws IOException {
        Merger merger = mergeInput -> null; // lambda implements only merge()
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).build();

        assertSame(LiveDocs.ALL_ALIVE, merger.prepareMerge(input));
    }

    public void testMergerDefaultAbortPreparedMergeIsNoOp() throws IOException {
        Merger merger = mergeInput -> null;
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(1L).build();

        merger.abortPreparedMerge(input); // must not throw
    }
}

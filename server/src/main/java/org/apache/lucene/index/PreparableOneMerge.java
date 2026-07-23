/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOFunction;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

/**
 * A {@link MergePolicy.OneMerge} that supports being prepared (its merge readers populated
 * and live-docs snapshot frozen via copy-on-write) ahead of {@link IndexWriter#merge}. The
 * idempotent {@link #initMergeReaders} override lets {@code mergeMiddle} re-enter without
 * re-snapshotting. Lives in {@code org.apache.lucene.index} to access package-private
 * Lucene internals.
 *
 * @opensearch.experimental
 */
public class PreparableOneMerge extends MergePolicy.OneMerge {

    private boolean readersInitialized = false;

    public PreparableOneMerge(List<SegmentCommitInfo> segments) {
        super(segments);
    }

    @Override
    void initMergeReaders(IOFunction<SegmentCommitInfo, MergePolicy.MergeReader> readerFactory) throws IOException {
        if (readersInitialized) {
            return;
        }
        super.initMergeReaders(readerFactory);
        readersInitialized = true;
    }

    public boolean readersInitialized() {
        return readersInitialized;
    }

    /**
     * Packs the per-segment frozen {@code hardLiveDocs} snapshot (FixedBitSet layout, bit set
     * = alive) keyed by application generation. Segments with no deletes are absent from the
     * result. Must be invoked after {@link #initMergeReaders} has populated the merge readers.
     */
    public Map<Long, long[]> packLiveDocsByGeneration(ToLongFunction<SegmentCommitInfo> generationOf) {
        List<MergePolicy.MergeReader> readers = getMergeReader();
        if (readers.isEmpty()) {
            return Map.of();
        }
        Map<Long, long[]> result = new HashMap<>();
        for (int i = 0; i < readers.size(); i++) {
            MergePolicy.MergeReader mr = readers.get(i);
            Bits hardLive = mr.hardLiveDocs;
            if (hardLive == null) {
                continue;
            }
            long gen = generationOf.applyAsLong(segments.get(i));
            int maxDoc = mr.reader.maxDoc();
            FixedBitSet bitSet = new FixedBitSet(maxDoc);
            for (int d = 0; d < maxDoc; d++) {
                if (hardLive.get(d)) {
                    bitSet.set(d);
                }
            }
            result.put(gen, bitSet.getBits());
        }
        return result;
    }
}

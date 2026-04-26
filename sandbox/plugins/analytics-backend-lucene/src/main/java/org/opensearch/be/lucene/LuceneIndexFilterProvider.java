/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.CollectorQueryLifecycleManager;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SegmentCollector;

import java.io.IOException;

/**
 * Lucene-backed {@link IndexFilterProvider}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexFilterProvider implements IndexFilterProvider<Query, LuceneIndexFilterContext, DirectoryReader> {

    /** Creates a new LuceneIndexFilterProvider. */
    public LuceneIndexFilterProvider() {}

    @Override
    public LuceneIndexFilterContext createContext(Query query, DirectoryReader reader) throws IOException {
        return new LuceneIndexFilterContext(query, reader);
    }

    /**
     * Creates a collector for the given segment and registers it in the
     * context's {@link CollectorQueryLifecycleManager}.
     *
     * @param context the index filter context
     * @param segmentOrd the segment ordinal
     * @param minDoc the minimum document ID
     * @param maxDoc the maximum document ID
     * @return an int key that identifies this collector across JNI
     */
    @Override
    public int createCollector(LuceneIndexFilterContext context, int segmentOrd, int minDoc, int maxDoc) {
        SegmentCollector collector = createCollectorInternal(context, segmentOrd, minDoc, maxDoc);
        return context.getCollectorManager().registerCollector(collector);
    }

    /**
     * Collects matching doc IDs for the collector identified by {@code key}.
     *
     * @param context the index filter context
     * @param key the collector key
     * @param minDoc the minimum document ID
     * @param maxDoc the maximum document ID
     */
    public long[] collectDocs(LuceneIndexFilterContext context, int key, int minDoc, int maxDoc) {
        return context.getCollectorManager().collectDocs(key, minDoc, maxDoc);
    }

    /**
     * Releases the collector identified by {@code key}.
     *
     * @param context the index filter context
     * @param key the collector key
     */
    public void releaseCollector(LuceneIndexFilterContext context, int key) {
        context.getCollectorManager().releaseCollector(key);
    }

    @Override
    public void close() {}

    private SegmentCollector createCollectorInternal(LuceneIndexFilterContext context, int segmentOrd, int minDoc, int maxDoc) {
        try {
            Scorer scorer = context.getWeight().scorer(context.getLeaves().get(segmentOrd));
            if (scorer == null) {
                return EMPTY_COLLECTOR;
            }
            return new LuceneSegmentCollector(scorer.iterator(), minDoc, maxDoc);
        } catch (IOException e) {
            return EMPTY_COLLECTOR;
        }
    }

    private static final SegmentCollector EMPTY_COLLECTOR = (min, max) -> {
        if (max <= min) {
            return new long[0];
        }
        int wordCount = (max - min + 63) >>> 6;
        return new long[wordCount];
    };

    /**
     * Per-segment cursor over matching docs.
     *
     * <p>Forward-only: successive {@link #collectDocs(int, int)} calls MUST use
     * non-decreasing, non-overlapping {@code [minDoc, maxDoc)} ranges. The
     * Lucene {@link DocIdSetIterator} is a one-shot cursor and cannot seek
     * backwards.
     *
     * <p>Bit layout: the returned {@code long[]} is a packed bitset where
     * word {@code j} bit {@code i} (LSB-first) represents the doc at relative
     * position {@code j*64 + i} within the caller's {@code [minDoc, maxDoc)}
     * range. That is, bit {@code k} represents absolute doc id
     * {@code minDoc + k}. Length is always {@code ceilDiv(maxDoc - minDoc, 64)}
     * words regardless of how many bits are set.
     */
    private static final class LuceneSegmentCollector implements SegmentCollector {
        private final DocIdSetIterator iterator;
        /** Partition bounds — the iterator only produces matches in this range. */
        private final int partitionMinDoc;
        private final int partitionMaxDoc;
        /** Cursor: resumes from here on the next collectDocs call. */
        private int currentDoc = -1;

        LuceneSegmentCollector(DocIdSetIterator iterator, int partitionMinDoc, int partitionMaxDoc) {
            this.iterator = iterator;
            this.partitionMinDoc = partitionMinDoc;
            this.partitionMaxDoc = partitionMaxDoc;
        }

        @Override
        public long[] collectDocs(int minDoc, int maxDoc) {
            if (maxDoc <= minDoc) {
                return new long[0];
            }
            // Anchor bits at the caller's minDoc. Bit k represents doc minDoc + k.
            // Length is fixed at ceilDiv(span, 64) regardless of how many bits are set.
            int span = maxDoc - minDoc;
            FixedBitSet bits = new FixedBitSet(span);

            // The iterator only produces matches inside the partition. Clamp the
            // scan range accordingly; bits outside the partition stay zero.
            int scanFrom = Math.max(minDoc, partitionMinDoc);
            int scanTo = Math.min(maxDoc, partitionMaxDoc);
            if (scanFrom >= scanTo) {
                return bits.getBits().clone();
            }

            try {
                int docId = currentDoc;
                if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                    return bits.getBits().clone();
                }
                if (docId < scanFrom) {
                    docId = iterator.advance(scanFrom);
                }
                while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
                    bits.set(docId - minDoc); // minDoc-relative, NOT scanFrom-relative
                    docId = iterator.nextDoc();
                }
                currentDoc = docId;
            } catch (IOException e) {
                // Return what we collected so far in a correctly-sized buffer.
                return bits.getBits().clone();
            }
            // FixedBitSet.getBits() exposes the backing array (length == ceilDiv(span, 64)).
            // Clone since the SPI contract returns an owned long[].
            return bits.getBits().clone();
        }
    }
}

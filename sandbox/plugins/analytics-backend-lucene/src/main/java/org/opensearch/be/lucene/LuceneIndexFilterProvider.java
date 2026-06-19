/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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
     * @param out destination {@link MemorySegment} to write the packed bitset into
     * @return the number of 64-bit words written into {@code out}
     */
    @Override
    public int collectDocs(LuceneIndexFilterContext context, int key, int minDoc, int maxDoc, MemorySegment out) {
        return context.getCollectorManager().collectDocs(key, minDoc, maxDoc, out);
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

    private static final SegmentCollector EMPTY_COLLECTOR = (min, max, out) -> {
        if (max <= min) {
            return 0;
        }
        int wordCount = (max - min + 63) >>> 6;
        for (int i = 0; i < wordCount; i++) {
            out.setAtIndex(ValueLayout.JAVA_LONG, i, 0L);
        }
        return wordCount;
    };

    /**
     * Per-segment cursor over matching docs.
     *
     * <p>Forward-only: successive {@link #collectDocs(int, int, MemorySegment)} calls MUST use
     * non-decreasing, non-overlapping {@code [minDoc, maxDoc)} ranges. The
     * Lucene {@link DocIdSetIterator} is a one-shot cursor and cannot seek
     * backwards.
     *
     * <p>Bit layout: the {@code out} {@link MemorySegment} receives a packed bitset where
     * word {@code j} bit {@code i} (LSB-first) represents the doc at relative
     * position {@code j*64 + i} within the caller's {@code [minDoc, maxDoc)}
     * range. That is, bit {@code k} represents absolute doc id
     * {@code minDoc + k}. Word count is always {@code ceilDiv(maxDoc - minDoc, 64)}
     * regardless of how many bits are set.
     */
    private static final class LuceneSegmentCollector implements SegmentCollector {
        private static final Logger logger = LogManager.getLogger(LuceneSegmentCollector.class);
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
        public int collectDocs(int minDoc, int maxDoc, MemorySegment out) {
            if (maxDoc <= minDoc) {
                return 0;
            }
            // Use FixedBitSet for cache-friendly heap-array bit manipulation,
            // then bulk-copy into the native MemorySegment at the boundary.
            int span = maxDoc - minDoc;
            FixedBitSet bits = new FixedBitSet(span);

            int scanFrom = Math.max(minDoc, partitionMinDoc);
            int scanTo = Math.min(maxDoc, partitionMaxDoc);

            if (scanFrom < scanTo) {
                try {
                    int docId = currentDoc;
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        if (docId < scanFrom) {
                            docId = iterator.advance(scanFrom);
                        }
                        while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
                            bits.set(docId - minDoc);
                            docId = iterator.nextDoc();
                        }
                        currentDoc = docId;
                    }
                } catch (IOException e) {
                    logger.warn("IOException during collectDocs, returning partial bitset", e);
                }
            }

            // Single bulk copy: heap long[] → native MemorySegment.
            long[] words = bits.getBits();
            int wordCount = (span + 63) >>> 6;
            MemorySegment.copy(words, 0, out, ValueLayout.JAVA_LONG, 0, wordCount);
            return wordCount;
        }
    }
}

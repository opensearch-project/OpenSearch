/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.Ranges;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Range collector implementation that supports sub-aggregations by collecting doc IDs.
 */
public class SubAggRangeCollector extends AbstractRangeCollector {

    private static final Logger logger = LogManager.getLogger(SubAggRangeCollector.class);

    private final Function<Integer, Long> getBucketOrd;

    private final BucketCollector collectableSubAggregators;
    private final LeafReaderContext leafCtx;

    private final FixedBitSet bitSet;
    private final BitDocIdSet bitDocIdSet;

    public SubAggRangeCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        FilterRewriteOptimizationContext.OptimizeResult result,
        Function<Integer, Long> getBucketOrd,
        FilterRewriteOptimizationContext.SubAggCollectorParam subAggCollectorParam
    ) {
        super(ranges, incrementRangeDocCount, maxNumNonZeroRange, activeIndex, result);
        this.getBucketOrd = getBucketOrd;
        this.collectableSubAggregators = subAggCollectorParam.collectableSubAggregators();
        this.leafCtx = subAggCollectorParam.leafCtx();
        int numDocs = leafCtx.reader().maxDoc();
        bitSet = new FixedBitSet(numDocs);
        bitDocIdSet = new BitDocIdSet(bitSet);
    }

    @Override
    public boolean hasSubAgg() {
        return true;
    }

    @Override
    public void collectDocId(int docId) {
        bitSet.set(docId);
    }

    @Override
    public void collectDocIdSet(DocIdSetIterator iter) throws IOException {
        bitSet.or(iter);
    }

    @Override
    public void finalizePreviousRange() {
        if (counter > 0) {
            incrementRangeDocCount.accept(activeIndex, counter);
            counter = 0;
        }

        long bucketOrd = getBucketOrd.apply(activeIndex);
        logger.trace("finalize range {} with bucket ordinal {}", activeIndex, bucketOrd);

        // trigger the sub agg collection for this range
        try {
            DocIdSetIterator iterator = bitDocIdSet.iterator();
            // build a new leaf collector for each bucket
            LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(leafCtx);
            while (iterator.nextDoc() != NO_MORE_DOCS) {
                int currentDoc = iterator.docID();
                sub.collect(currentDoc, bucketOrd);
            }
            logger.trace("collected sub aggregation for bucket {}", bucketOrd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        bitSet.clear();
    }
}

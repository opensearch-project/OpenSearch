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
import org.apache.lucene.search.DocIdStreamHelper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.Ranges;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Range collector implementation that supports sub-aggregations by collecting doc IDs.
 */
public class SubAggRangeCollector extends SimpleRangeCollector {

    private static final Logger logger = LogManager.getLogger(SubAggRangeCollector.class);

    private final Function<Integer, Long> getBucketOrd;

    private final BucketCollector collectableSubAggregators;
    private final LeafReaderContext leafCtx;

    private final Bits liveDocs;
    private final FixedBitSet bitSet;

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
        this.liveDocs = leafCtx.reader().getLiveDocs();
        bitSet = new FixedBitSet(leafCtx.reader().maxDoc());
    }

    @Override
    public boolean hasSubAgg() {
        return true;
    }

    private boolean isDocLive(int docId) {
        return liveDocs == null || liveDocs.get(docId);
    }

    @Override
    public void countNode(int count) {
        throw new UnsupportedOperationException("countNode should be unreachable");
    }

    @Override
    public void count() {
        throw new UnsupportedOperationException("countNode should be unreachable");
    }

    @Override
    public void collectDocId(int docId) {
        if (isDocLive(docId)) {
            counter++;
            bitSet.set(docId);
        }
    }

    @Override
    public void collectDocIdSet(DocIdSetIterator iter) throws IOException {
        // Explicitly OR iter intoBitSet to filter out deleted docs
        iter.nextDoc();
        for (int doc = iter.docID(); doc < DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
            if (isDocLive(doc)) {
                counter++;
                bitSet.set(doc);
            }
        }
    }

    @Override
    public void finalizePreviousRange() {
        super.finalizePreviousRange();

        long bucketOrd = getBucketOrd.apply(activeIndex);
        logger.trace("finalize range {} with bucket ordinal {}", activeIndex, bucketOrd);

        // trigger the sub agg collection for this range
        try {
            // build a new leaf collector for each bucket
            LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(leafCtx);
            sub.collect(DocIdStreamHelper.getDocIdStream(bitSet), bucketOrd);
            logger.trace("collected sub aggregation for bucket {}", bucketOrd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        bitSet.clear();
    }
}

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
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.Ranges;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Range collector implementation that supports sub-aggregations by collecting doc IDs.
 */
public class SubAggRangeCollector extends AbstractRangeCollector {

    private static final Logger logger = LogManager.getLogger(SubAggRangeCollector.class);

    private DocIdSetBuilder builder = null;
    private final Supplier<DocIdSetBuilder> disBuilderSupplier;

    private DocIdSetBuilder.BulkAdder currentAdder;
    private final Function<Integer, Long> getBucketOrd;
    private int lastGrowCount;

    private final BucketCollector collectableSubAggregators;
    private final LeafReaderContext leafCtx;

    public SubAggRangeCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        Supplier<DocIdSetBuilder> disBuilderSupplier,
        Function<Integer, Long> getBucketOrd,
        FilterRewriteOptimizationContext.OptimizeResult result,
        BucketCollector collectableSubAggregators,
        LeafReaderContext leafCtx
    ) {
        super(ranges, incrementRangeDocCount, maxNumNonZeroRange, activeIndex, result);
        this.disBuilderSupplier = disBuilderSupplier;
        this.getBucketOrd = getBucketOrd;
        this.collectableSubAggregators = collectableSubAggregators;
        this.leafCtx = leafCtx;
    }

    @Override
    public boolean hasSubAgg() {
        return true;
    }

    @Override
    public void grow(int count) {
        if (builder == null) {
            builder = disBuilderSupplier.get();
        }
        logger.trace("grow range {} with count {}", activeIndex, count);
        currentAdder = builder.grow(count);
        lastGrowCount = count;
    }

    @Override
    public void collectDocId(int docId) {
        currentAdder.add(docId);
    }

    @Override
    public void collectDocIdSet(DocIdSetIterator iter) throws IOException {
        currentAdder.add(iter);
    }

    @Override
    public void finalizePreviousRange() {
        if (counter > 0) {
            incrementRangeDocCount.accept(activeIndex, counter);
            counter = 0;
        }

        if (currentAdder != null) {
            assert builder != null;
            long bucketOrd = getBucketOrd.apply(activeIndex);
            logger.trace("finalize range {} with bucket ordinal {}", activeIndex, bucketOrd);

            // trigger the sub agg collection for this range
            try {
                DocIdSetIterator iterator = builder.build().iterator();
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

            currentAdder = null;
            builder = null;
        }
    }

    @Override
    public boolean iterateRangeEnd(byte[] value, boolean inLeaf) {
        boolean shouldStop = super.iterateRangeEnd(value, inLeaf);
        // edge case: if finalizePreviousRange is called within the leaf node
        // currentAdder is reset and grow would not be called immediately
        // here we reuse previous grow count
        if (!shouldStop && inLeaf && currentAdder == null) {
            grow(lastGrowCount);
        }
        return shouldStop;
    }
}

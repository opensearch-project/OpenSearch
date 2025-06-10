/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.Ranges;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Simple range collector implementation that only counts documents without collecting doc IDs.
 */
public class SimpleRangeCollector extends AbstractRangeCollector {

    public SimpleRangeCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        FilterRewriteOptimizationContext.OptimizeResult result
    ) {
        super(ranges, incrementRangeDocCount, maxNumNonZeroRange, activeIndex, result);
    }

    @Override
    public boolean hasSubAgg() {
        return false;
    }

    @Override
    public void countNode(int count) {
        counter += count;
    }

    @Override
    public void count() {
        counter++;
    }

    @Override
    public void collectDocId(int docId) {
        throw new UnsupportedOperationException("collectDocId should be unreachable");
    }

    @Override
    public void collectDocIdSet(DocIdSetIterator iter) throws IOException {
        throw new UnsupportedOperationException("collectDocIdSet should be unreachable");
    }

    @Override
    public void finalizePreviousRange() {
        if (counter > 0) {
            incrementRangeDocCount.accept(activeIndex, counter);
            counter = 0;
        }
    }
}

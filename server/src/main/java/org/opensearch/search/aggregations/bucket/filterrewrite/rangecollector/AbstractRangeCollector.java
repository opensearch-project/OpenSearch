/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector;

import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.Ranges;

import java.util.function.BiConsumer;

/**
 * Abstract class for range collectors.
 */
public abstract class AbstractRangeCollector implements RangeCollector {
    protected final Ranges ranges;
    protected int activeIndex;
    protected final BiConsumer<Integer, Integer> incrementRangeDocCount;
    protected final int maxNumNonZeroRange;
    protected final FilterRewriteOptimizationContext.OptimizeResult result;
    private int visitedRange = 0;
    protected int counter = 0;

    public AbstractRangeCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        FilterRewriteOptimizationContext.OptimizeResult result
    ) {
        this.ranges = ranges;
        this.activeIndex = activeIndex;
        this.incrementRangeDocCount = incrementRangeDocCount;
        this.maxNumNonZeroRange = maxNumNonZeroRange;
        this.result = result;
    }

    @Override
    public boolean iterateRangeEnd(byte[] value, boolean inLeaf) {
        while (!withinUpperBound(value)) {
            if (++activeIndex >= ranges.getSize()) {
                return true;
            }
        }
        visitedRange++;
        return visitedRange > maxNumNonZeroRange;
    }

    @Override
    public boolean withinLowerBound(byte[] value) {
        return ranges.withinLowerBound(value, ranges.getLowers()[activeIndex]);
    }

    @Override
    public boolean withinUpperBound(byte[] value) {
        return ranges.withinUpperBound(value, ranges.getUppers()[activeIndex]);
    }

    @Override
    public boolean withinRange(byte[] value) {
        return withinLowerBound(value) && withinUpperBound(value);
    }

    @Override
    public void visitInner() {
        result.visitInner();
    }

    @Override
    public void visitLeaf() {
        result.visitLeaf();
    }

    @Override
    public FilterRewriteOptimizationContext.OptimizeResult getResult() {
        return result;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.filterrewrite;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.aggregations.bucket.composite.RoundingValuesSource;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

import static org.opensearch.search.optimization.filterrewrite.TreeTraversal.multiRangesTraverse;

/**
 * For composite aggregation to do optimization when it only has a single date histogram source
 */
public abstract class CompositeAggregatorBridge extends DateHistogramAggregatorBridge {
    protected boolean canOptimize(CompositeValuesSourceConfig[] sourceConfigs) {
        if (sourceConfigs.length != 1 || !(sourceConfigs[0].valuesSource() instanceof RoundingValuesSource)) return false;
        return canOptimize(sourceConfigs[0].missingBucket(), sourceConfigs[0].hasScript(), sourceConfigs[0].fieldType());
    }

    private boolean canOptimize(boolean missing, boolean hasScript, MappedFieldType fieldType) {
        if (!missing && !hasScript) {
            if (fieldType instanceof DateFieldMapper.DateFieldType) {
                if (fieldType.isSearchable()) {
                    this.fieldType = fieldType;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public final void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount, final LeafBucketCollector sub) throws IOException {
        DateFieldMapper.DateFieldType fieldType = getFieldType();
        TreeTraversal.RangeAwareIntersectVisitor treeVisitor;
        if (sub != null) {
            treeVisitor = new TreeTraversal.DocCollectRangeAwareIntersectVisitor(values.getPointTree(), optimizationContext.getRanges(), getSize(), (activeIndex, docID) -> {
                long rangeStart = LongPoint.decodeDimension(optimizationContext.getRanges().lowers[activeIndex], 0);
                rangeStart = fieldType.convertNanosToMillis(rangeStart);
                long ord = getBucketOrd(bucketOrdProducer().apply(rangeStart));

                try {
                    incrementDocCount.accept(ord, (long) 1);
                    sub.collect(docID, ord);
                } catch ( IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        } else {
            treeVisitor = new TreeTraversal.DocCountRangeAwareIntersectVisitor(values.getPointTree(), optimizationContext.getRanges(), getSize(), (activeIndex, docCount) -> {
                long rangeStart = LongPoint.decodeDimension(optimizationContext.getRanges().lowers[activeIndex], 0);
                rangeStart = fieldType.convertNanosToMillis(rangeStart);
                long ord = getBucketOrd(bucketOrdProducer().apply(rangeStart));
                incrementDocCount.accept(ord, (long) docCount);
            });
        }

        optimizationContext.consumeDebugInfo(multiRangesTraverse(treeVisitor));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.ranges;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Different aggregations may have different pre-conditions, range building logic, etc.
 */
interface AggregationFunctionProvider {
    boolean isRewriteable(Object parent, int subAggLength);

    OptimizationContext.Ranges buildRanges(SearchContext ctx, MappedFieldType fieldType) throws IOException;

    OptimizationContext.Ranges buildRanges(LeafReaderContext leaf, SearchContext ctx, MappedFieldType fieldType) throws IOException;

    OptimizationContext.DebugInfo tryFastFilterAggregation(
        PointValues values,
        OptimizationContext.Ranges ranges,
        BiConsumer<Long, Long> incrementDocCount,
        Function<Object, Long> bucketOrd
    ) throws IOException;
}

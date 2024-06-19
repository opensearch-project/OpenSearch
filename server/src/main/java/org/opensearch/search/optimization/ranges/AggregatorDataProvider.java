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
 * To do optimization, we may need access to some data from Aggregator
 * You can implement this interface as an inner class of any aggregator to provide the data
 * The business logic other than providing data can be put into a base abstract class
 *
 * @opensearch.internal
 */
public interface AggregatorDataProvider {
    boolean canOptimize();

    OptimizationContext.Ranges buildRanges(SearchContext ctx, MappedFieldType fieldType) throws IOException;

    OptimizationContext.Ranges buildRanges(LeafReaderContext leaf, SearchContext ctx, MappedFieldType fieldType) throws IOException;

    OptimizationContext.DebugInfo tryFastFilterAggregation(
        PointValues values,
        OptimizationContext.Ranges ranges,
        BiConsumer<Long, Long> incrementDocCount,
        Function<Object, Long> bucketOrd
    ) throws IOException;
}

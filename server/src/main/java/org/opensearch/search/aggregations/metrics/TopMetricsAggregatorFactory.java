/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.SubSearchContext;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Aggregation Factory for top_metrics agg.
 *
 * @opensearch.internal
 */
class TopMetricsAggregatorFactory extends AggregatorFactory {

    private final List<String> metricFields;
    private final int size;
    private final Optional<SortAndFormats> sort;
    private final boolean scoreSortFallback;

    TopMetricsAggregatorFactory(
        String name,
        List<String> metricFields,
        int size,
        Optional<SortAndFormats> sort,
        boolean scoreSortFallback,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
        this.metricFields = metricFields;
        this.size = size;
        this.sort = sort;
        this.scoreSortFallback = scoreSortFallback;
    }

    @Override
    protected Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        SubSearchContext subSearchContext = new SubSearchContext(searchContext);
        subSearchContext.parsedQuery(searchContext.parsedQuery());
        subSearchContext.from(0);
        subSearchContext.size(size);
        subSearchContext.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        subSearchContext.explain(false);
        subSearchContext.version(false);
        subSearchContext.seqNoAndPrimaryTerm(false);
        if (sort.isPresent()) {
            SortAndFormats sortAndFormats = sort.get();
            subSearchContext.sort(sortAndFormats);
            subSearchContext.trackScores(sortAndFormats.sort.needsScores());
        } else if (scoreSortFallback) {
            subSearchContext.trackScores(true);
        } else {
            throw new IllegalStateException("[top_metrics] requires a valid sort but none could be resolved");
        }

        List<FieldAndFormat> docValueFields = new ArrayList<>(metricFields.size());
        for (String fieldName : metricFields) {
            docValueFields.add(new FieldAndFormat(fieldName, null));
        }
        FetchDocValuesContext docValuesContext = FetchDocValuesContext.create(
            searchContext.mapperService()::simpleMatchToFullName,
            searchContext.mapperService().getIndexSettings().getMaxDocvalueFields(),
            docValueFields
        );
        subSearchContext.docValuesContext(docValuesContext);

        return new TopMetricsAggregator(searchContext.fetchPhase(), subSearchContext, name, metricFields, searchContext, parent, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}

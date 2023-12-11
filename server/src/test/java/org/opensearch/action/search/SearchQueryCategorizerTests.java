/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class SearchQueryCategorizerTests extends OpenSearchTestCase {

    private static final String MULTI_TERMS_AGGREGATION = "multi_terms";

    private MetricsRegistry metricsRegistry;

    private SearchQueryCategorizer searchQueryCategorizer;

    @Before
    public void setup() {
        metricsRegistry = mock(MetricsRegistry.class);
        when(metricsRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenAnswer(
            invocation -> mock(Counter.class)
        );
        searchQueryCategorizer = new SearchQueryCategorizer(metricsRegistry);
    }

    public void testAggregationsQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(
            new MultiTermsAggregationBuilder("agg1").terms(
                Arrays.asList(
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("username").build(),
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("rating").build()
                )
            )
        );
        sourceBuilder.size(0);

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.aggCounter).add(eq(1.0d), any(Tags.class));

        // Now, capture the arguments passed to the aggCounter.add method
        ArgumentCaptor<Double> valueCaptor = ArgumentCaptor.forClass(Double.class);
        ArgumentCaptor<Tags> tagsCaptor = ArgumentCaptor.forClass(Tags.class);

        // Verify that aggCounter.add was called with the expected arguments
        verify(searchQueryCategorizer.searchQueryCounters.aggCounter).add(valueCaptor.capture(), tagsCaptor.capture());

        // Assert the captured values
        double capturedValue = valueCaptor.getValue();
        Tags capturedTag = tagsCaptor.getValue();

        // Assert your expectations on the captured values
        assertEquals(1.0d, capturedValue, 0.0001);
        assertEquals(MULTI_TERMS_AGGREGATION, capturedTag.toString());
    }

    public void testBoolQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new BoolQueryBuilder().must(new MatchQueryBuilder("searchText", "fox")));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.boolCounter).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.matchCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testFunctionScoreQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new FunctionScoreQueryBuilder(QueryBuilders.prefixQuery("text", "bro")));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.functionScoreCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testMatchQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "php"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.matchCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testMatchPhraseQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchPhraseQuery("tags", "php"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.matchPhrasePrefixCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testMultiMatchQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new MultiMatchQueryBuilder("foo bar", "myField"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.multiMatchCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testOtherQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        BoostingQueryBuilder queryBuilder = new BoostingQueryBuilder(
            new TermQueryBuilder("unmapped_field", "foo"),
            new MatchNoneQueryBuilder()
        );
        sourceBuilder.query(queryBuilder);

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.otherQueryCounter, times(2)).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.termCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testQueryStringQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("foo:*");
        sourceBuilder.query(queryBuilder);

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.queryStringQueryCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testRangeQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("date");
        rangeQuery.gte("1970-01-01");
        rangeQuery.lt("1982-01-01");
        sourceBuilder.query(rangeQuery);

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.rangeCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testRegexQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new RegexpQueryBuilder("field", "text"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.regexCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testSortQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort(new ScoreSortBuilder());

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.matchCounter).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.sortCounter, times(2)).add(eq(1.0d), any(Tags.class));
    }

    public void testTermQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.termQuery("field", "value2"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.termCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testWildcardQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(new WildcardQueryBuilder("field", "text"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.wildcardCounter).add(eq(1.0d), any(Tags.class));
    }

    public void testComplexQuery() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("field", "value2");
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("tags", "php");
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("field", "text");
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().must(termQueryBuilder)
            .filter(matchQueryBuilder)
            .should(regexpQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.aggregation(new RangeAggregationBuilder("agg1").field("num"));

        searchQueryCategorizer.categorize(sourceBuilder);

        verify(searchQueryCategorizer.searchQueryCounters.termCounter).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.matchCounter).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.regexCounter).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.boolCounter).add(eq(1.0d), any(Tags.class));
        verify(searchQueryCategorizer.searchQueryCounters.aggCounter).add(eq(1.0d));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.TestSearchContext;

import java.io.IOException;

public class AggregationSetupTests extends OpenSearchSingleNodeTestCase {
    protected IndexService index;

    protected SearchContext context;

    protected final String globalNonGlobalAggs = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}, "
        + "\"all_products\": {\"global\": {}, \"aggs\": {\"avg_price\": {\"avg\": { \"field\": \"f\"}}}}}";

    protected final String multipleNonGlobalAggs = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}, "
        + "\"avg_price\": {\"avg\": { \"field\": \"f\"}}}";

    protected final String globalAgg = "{ \"all_products\": {\"global\": {}, \"aggs\": {\"avg_price\": {\"avg\": { \"field\": \"f\"}}}}}";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        index = createIndex("idx");
        client().prepareIndex("idx").setId("1").setSource("f", 5).execute().get();
        client().admin().indices().prepareRefresh("idx").get();
        context = createSearchContext(index);
        final AggregationBuilder dummyAggBuilder = AggregationBuilders.avg(randomAlphaOfLengthBetween(5, 20)).field("foo");
        final SearchRequest searchRequest = new SearchRequest(
            new String[] { index.index().getName() },
            new SearchSourceBuilder().aggregation(dummyAggBuilder)
        ).allowPartialSearchResults(false);
        ((TestSearchContext) context).setShardSearchRequest(
            new ShardSearchRequest(
                OriginalIndices.NONE,
                searchRequest,
                new ShardId(index.index(), 0),
                1,
                null,
                1.0f,
                0,
                null,
                Strings.EMPTY_ARRAY
            )
        );
        ((TestSearchContext) context).setConcurrentSegmentSearchEnabled(true);
    }

    protected AggregatorFactories getAggregationFactories(String agg) throws IOException {
        try (XContentParser aggParser = createParser(JsonXContent.jsonXContent, agg)) {
            aggParser.nextToken();
            return AggregatorFactories.parseAggregators(aggParser).build(context.getQueryShardContext(), null);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationInitializationException;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BaseAggregationTestCase;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

import static org.hamcrest.Matchers.containsString;

public class TopMetricsTests extends BaseAggregationTestCase<TopMetricsAggregationBuilder> {

    @Override
    protected TopMetricsAggregationBuilder createTestAggregatorBuilder() {
        TopMetricsAggregationBuilder builder = new TopMetricsAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        int metricCount = randomIntBetween(1, 3);
        for (int i = 0; i < metricCount; i++) {
            builder.metricField(randomAlphaOfLengthBetween(5, 20));
        }
        builder.sort(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
        if (randomBoolean()) {
            builder.size(randomIntBetween(1, 10));
        }
        return builder;
    }

    public void testFailWithSubAgg() throws Exception {
        String source = "{\n"
            + "  \"terms_agg\": {\n"
            + "    \"terms\": { \"field\": \"tag\" },\n"
            + "    \"aggs\": {\n"
            + "      \"tm\": {\n"
            + "        \"top_metrics\": {\n"
            + "          \"metrics\": { \"field\": \"price\" },\n"
            + "          \"sort\": { \"timestamp\": \"desc\" }\n"
            + "        },\n"
            + "        \"aggs\": {\n"
            + "          \"max_price\": { \"max\": { \"field\": \"price\" } }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(AggregationInitializationException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Aggregator [tm] of type [top_metrics] cannot accept sub-aggregations"));
    }
}

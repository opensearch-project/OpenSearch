/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Locale;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.aggregations.AggregationBuilders.topMetrics;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TopMetricsIT extends OpenSearchIntegTestCase {

    public void testTopMetricsFieldSortEndToEnd() throws Exception {
        String index = createAndPopulateIndex();

        SearchResponse response = client().prepareSearch(index)
            .setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                topMetrics("tm").metricField("metric_field").sort(SortBuilders.fieldSort("sort_field").order(SortOrder.DESC)).size(1)
            )
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 3);

        InternalTopMetrics topMetricsAggregation = response.getAggregations().get("tm");
        assertThat(topMetricsAggregation, notNullValue());
        assertEquals(List.of("metric_field"), topMetricsAggregation.getMetricFields());
        assertEquals(1, topMetricsAggregation.getTopHits().getHits().getHits().length);
        assertThat(topMetricsAggregation.getTopHits().getHits().getAt(0).getId(), equalTo("3"));
        DocumentField metricField = topMetricsAggregation.getTopHits().getHits().getAt(0).field("metric_field");
        assertThat(metricField, notNullValue());
        assertThat(metricField.getValue(), equalTo("metric-c"));
    }

    public void testTopMetricsScoreSortEndToEnd() throws Exception {
        String index = createAndPopulateIndex();

        SearchResponse response = client().prepareSearch(index)
            .setQuery(QueryBuilders.matchQuery("body", "alpha"))
            .addAggregation(topMetrics("tm").metricField("metric_field").sort(SortBuilders.scoreSort().order(SortOrder.DESC)).size(1))
            .get();

        assertSearchResponse(response);
        assertHitCount(response, 2);

        InternalTopMetrics topMetricsAggregation = response.getAggregations().get("tm");
        assertThat(topMetricsAggregation, notNullValue());
        assertEquals(1, topMetricsAggregation.getTopHits().getHits().getHits().length);
        assertThat(topMetricsAggregation.getTopHits().getHits().getAt(0).getId(), equalTo("3"));
    }

    private String createAndPopulateIndex() throws Exception {
        String index = "top-metrics-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(index).setMapping("sort_field", "type=long", "metric_field", "type=keyword", "body", "type=text"));
        ensureGreen(index);

        client().prepareIndex(index)
            .setId("1")
            .setSource(
                jsonBuilder().startObject().field("sort_field", 10).field("metric_field", "metric-a").field("body", "alpha").endObject()
            )
            .get();
        client().prepareIndex(index)
            .setId("2")
            .setSource(
                jsonBuilder().startObject().field("sort_field", 20).field("metric_field", "metric-b").field("body", "beta").endObject()
            )
            .get();
        client().prepareIndex(index)
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("sort_field", 30)
                    .field("metric_field", "metric-c")
                    .field("body", "alpha alpha alpha")
                    .endObject()
            )
            .get();

        refresh(index);
        ensureSearchable(index);
        return index;
    }
}

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

package org.opensearch.search.aggregations;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.missing.Missing;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.missing;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class CombiIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public CombiIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_ALL).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_AUTO).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_NONE).build() }
        );
    }

    /**
     * Making sure that if there are multiple aggregations, working on the same field, yet require different
     * value source type, they can all still work. It used to fail as we used to cache the ValueSource by the
     * field name. If the cached value source was of type "bytes" and another aggregation on the field required to see
     * it as "numeric", it didn't work. Now we cache the Value Sources by a custom key (field name + ValueSource type)
     * so there's no conflict there.
     */
    public void testMultipleAggsOnSameField_WithDifferentRequiredValueSourceType() throws Exception {

        createIndex("idx");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomInt(30)];
        final Map<Integer, Integer> values = new HashMap<>();
        long missingValues = 0;
        for (int i = 0; i < builders.length; i++) {
            String name = "name_" + randomIntBetween(1, 10);
            if (rarely()) {
                missingValues++;
                builders[i] = client().prepareIndex("idx").setSource(jsonBuilder().startObject().field("name", name).endObject());
            } else {
                int value = randomIntBetween(1, 10);
                values.put(value, values.getOrDefault(value, 0) + 1);
                builders[i] = client().prepareIndex("idx")
                    .setSource(jsonBuilder().startObject().field("name", name).field("value", value).endObject());
            }
        }
        indexRandom(true, builders);
        ensureSearchable();

        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(missing("missing_values").field("value"))
            .addAggregation(terms("values").field("value").collectMode(aggCollectionMode))
            .get();

        assertSearchResponse(response);

        Aggregations aggs = response.getAggregations();

        Missing missing = aggs.get("missing_values");
        assertNotNull(missing);
        assertThat(missing.getDocCount(), equalTo(missingValues));

        Terms terms = aggs.get("values");
        assertNotNull(terms);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(values.size()));
        for (Terms.Bucket bucket : buckets) {
            values.remove(((Number) bucket.getKey()).intValue());
        }
        assertTrue(values.isEmpty());
    }

    /**
     * Some top aggs (eg. date_/histogram) that are executed on unmapped fields, will generate an estimate count of buckets - zero.
     * when the sub aggregator is then created, it will take this estimation into account. This used to cause
     * and an ArrayIndexOutOfBoundsException...
     */
    public void testSubAggregationForTopAggregationOnUnmappedField() throws Exception {

        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("value")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
        ).get();

        ensureSearchable("idx");

        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        SearchResponse searchResponse = client().prepareSearch("idx")
            .addAggregation(
                histogram("values").field("value1").interval(1).subAggregation(terms("names").field("name").collectMode(aggCollectionMode))
            )
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, Matchers.equalTo(0L));
        Histogram values = searchResponse.getAggregations().get("values");
        assertThat(values, notNullValue());
        assertThat(values.getBuckets().isEmpty(), is(true));
    }
}

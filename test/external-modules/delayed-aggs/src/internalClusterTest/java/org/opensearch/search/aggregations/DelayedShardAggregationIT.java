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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DelayedShardAggregationIT extends ParameterizedOpenSearchIntegTestCase {

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DelayedShardAggregationPlugin.class);
    }

    public void testSimple() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index"));
        float expectedMax = Float.MIN_VALUE;
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            float rand = randomFloat();
            expectedMax = Math.max(rand, expectedMax);
            reqs.add(client().prepareIndex("index").setSource("number", rand));
        }
        indexRandom(true, reqs);
        SearchResponse response = client().prepareSearch("index")
            .addAggregation(
                new DelayedShardAggregationBuilder("delay", TimeValue.timeValueMillis(10)).subAggregation(
                    new MaxAggregationBuilder("max").field("number")
                )
            )
            .get();
        Aggregations aggs = response.getAggregations();
        assertThat(aggs.get("delay"), instanceOf(InternalFilter.class));
        InternalFilter filter = aggs.get("delay");
        InternalMax max = filter.getAggregations().get("max");
        assertThat((float) max.getValue(), equalTo(expectedMax));
    }
}

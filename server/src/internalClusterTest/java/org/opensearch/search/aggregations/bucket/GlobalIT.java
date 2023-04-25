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

package org.opensearch.search.aggregations.bucket;

import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.global.Global;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.aggregations.AggregationBuilders.global;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class GlobalIT extends OpenSearchIntegTestCase {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        numDocs = randomIntBetween(3, 20);
        for (int i = 0; i < numDocs / 2; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setId("" + i + 1)
                    .setSource(jsonBuilder().startObject().field("value", i + 1).field("tag", "tag1").endObject())
            );
        }
        for (int i = numDocs / 2; i < numDocs; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setId("" + i + 1)
                    .setSource(
                        jsonBuilder().startObject().field("value", i + 1).field("tag", "tag2").field("name", "name" + i + 1).endObject()
                    )
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testWithStatsSubAggregatorAndProfileEnabled() throws Exception {
        testWithStatsSubAggregator(true);
    }

    public void testWithStatsSubAggregatorAndProfileDisabled() throws Exception {
        testWithStatsSubAggregator(false);
    }

    private void testWithStatsSubAggregator(boolean profileEnabled) throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.termQuery("tag", "tag1"))
            .addAggregation(global("global").subAggregation(stats("value_stats").field("value")))
            .setProfile(profileEnabled)
            .get();

        assertSearchResponse(response);

        Global global = response.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat((long) ((InternalAggregation) global).getProperty("_count"), equalTo((long) numDocs));
        assertThat(global.getAggregations().asList().isEmpty(), is(false));

        Stats stats = global.getAggregations().get("value_stats");
        assertThat((Stats) ((InternalAggregation) global).getProperty("value_stats"), sameInstance(stats));
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("value_stats"));
        long sum = 0;
        for (int i = 0; i < numDocs; ++i) {
            sum += i + 1;
        }
        assertThat(stats.getAvg(), equalTo((double) sum / numDocs));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo((double) numDocs));
        assertThat(stats.getCount(), equalTo((long) numDocs));
        assertThat(stats.getSum(), equalTo((double) sum));
    }

    public void testNonTopLevel() throws Exception {
        try {
            client().prepareSearch("idx")
                .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                .addAggregation(global("global").subAggregation(global("inner_global")))
                .get();

            fail(
                "expected to fail executing non-top-level global aggregator. global aggregations are only allowed as top level"
                    + "aggregations"
            );

        } catch (OpenSearchException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }
}

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

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SumLongIT extends OpenSearchIntegTestCase {
    public void testLargeLongValues() throws InterruptedException {
        prepareCreate("longidx").setMapping("value", "type=long").get();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("longidx").setSource("value", -922337202685477588L));
        builders.add(client().prepareIndex("longidx").setSource("value", 922337202685477587L));
        builders.add(client().prepareIndex("longidx").setSource("value", 1000L));
        builders.add(client().prepareIndex("longidx").setSource("value", -500L));

        indexRandom(true, builders);
        ensureSearchable();

        SearchResponse searchResponse = client().prepareSearch("longidx")
            .setQuery(matchAllQuery())
            .addAggregation(sum("sum").field("value"))
            .get();

        assertHitCount(searchResponse, 4);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) (-9223372026854775808L + 9223372026854775807L + 1000L - 500L)));
    }
}

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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.OpensearchException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.test.ESIntegTestCase;

import java.util.stream.IntStream;

import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;

public class CardinalityWithRequestBreakerIT extends ESIntegTestCase {

    /**
     * Test that searches using cardinality aggregations returns all request breaker memory.
     */
    public void testRequestBreaker() throws Exception {
        final String requestBreaker = randomIntBetween(1, 10000) + "kb";
        logger.info("--> Using request breaker setting: {}", requestBreaker);

        indexRandom(true, IntStream.range(0, randomIntBetween(10, 1000))
            .mapToObj(i ->
                client().prepareIndex("test", "_doc").setId("id_" + i)
                    .setSource(org.opensearch.common.collect.Map.of("field0", randomAlphaOfLength(5), "field1", randomAlphaOfLength(5)))
                ).toArray(IndexRequestBuilder[]::new));

        client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(),
                requestBreaker))
            .get();

        try {
            client().prepareSearch("test")
                .addAggregation(terms("terms").field("field0.keyword")
                    .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("cardinality", randomBoolean()))
                    .subAggregation(cardinality("cardinality").precisionThreshold(randomLongBetween(1, 40000)).field("field1.keyword")))
                .get();
        } catch (OpensearchException e) {
            if (ExceptionsHelper.unwrap(e, CircuitBreakingException.class) == null) {
                throw e;
            }
        }

        client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()))
            .get();

        // validation done by InternalTestCluster.ensureEstimatedStats()
    }
}

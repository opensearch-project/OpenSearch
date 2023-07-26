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

import org.opensearch.search.aggregations.bucket.global.GlobalAggregator;
import org.opensearch.search.aggregations.bucket.terms.NumericTermsAggregator;

import java.io.IOException;
import java.util.List;

public class AggregationCollectorTests extends AggregationSetupTests {

    public void testNeedsScores() throws Exception {
        // simple field aggregation, no scores needed
        String fieldAgg = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}}";
        assertFalse(needsScores(fieldAgg));

        // agg on a script => scores are needed
        // TODO: can we use a mock script service here?
        // String scriptAgg = "{ \"my_terms\": {\"terms\": {\"script\": \"doc['f'].value\"}}}";
        // assertTrue(needsScores(index, scriptAgg));
        //
        // String subScriptAgg = "{ \"my_outer_terms\": { \"terms\": { \"field\": \"f\" }, \"aggs\": " + scriptAgg + "}}";
        // assertTrue(needsScores(index, subScriptAgg));

        // make sure the information is propagated to sub aggregations
        String subFieldAgg = "{ \"my_outer_terms\": { \"terms\": { \"field\": \"f\" }, \"aggs\": " + fieldAgg + "}}";
        assertFalse(needsScores(subFieldAgg));

        // top_hits is a particular example of an aggregation that needs scores
        String topHitsAgg = "{ \"my_hits\": {\"top_hits\": {}}}";
        assertTrue(needsScores(topHitsAgg));
    }

    public void testNonGlobalTopLevelAggregators() throws Exception {
        // simple field aggregation
        String fieldAgg = "{ \"my_terms\": {\"terms\": {\"field\": \"f\"}}}";
        final List<Aggregator> aggregators = createNonGlobalAggregators(fieldAgg);
        final List<Aggregator> topLevelAggregators = createTopLevelAggregators(fieldAgg);
        assertEquals(topLevelAggregators.size(), aggregators.size());
        assertEquals(topLevelAggregators.get(0).name(), aggregators.get(0).name());
        assertTrue(aggregators.get(0) instanceof NumericTermsAggregator);
    }

    public void testGlobalAggregators() throws Exception {
        // global aggregation
        final List<Aggregator> aggregators = createGlobalAggregators(globalAgg);
        final List<Aggregator> topLevelAggregators = createTopLevelAggregators(globalAgg);
        assertEquals(topLevelAggregators.size(), aggregators.size());
        assertEquals(topLevelAggregators.get(0).name(), aggregators.get(0).name());
        assertTrue(aggregators.get(0) instanceof GlobalAggregator);
    }

    private boolean needsScores(String agg) throws IOException {
        final List<Aggregator> aggregators = createTopLevelAggregators(agg);
        assertEquals(1, aggregators.size());
        return aggregators.get(0).scoreMode().needsScores();
    }

    private List<Aggregator> createTopLevelAggregators(String agg) throws IOException {
        final AggregatorFactories factories = getAggregationFactories(agg);
        return factories.createTopLevelAggregators(context);
    }

    private List<Aggregator> createNonGlobalAggregators(String agg) throws IOException {
        final AggregatorFactories factories = getAggregationFactories(agg);
        return factories.createTopLevelNonGlobalAggregators(context);
    }

    private List<Aggregator> createGlobalAggregators(String agg) throws IOException {
        final AggregatorFactories factories = getAggregationFactories(agg);
        return factories.createTopLevelGlobalAggregators(context);
    }
}

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

import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.RareTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregatorFactory;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class AggregationsIntegrationIT extends OpenSearchIntegTestCase {

    static int numDocs;

    private static final String LARGE_STRING = String.join("", Collections.nCopies(2000, "a"));
    private static final String LARGE_STRING_EXCEPTION_MESSAGE = "The length of regex ["
        + LARGE_STRING.length()
        + "] used in the request has exceeded the allowed maximum";

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("index").addMapping("type", "f", "type=keyword").get());
        numDocs = randomIntBetween(1, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index", "type").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);
    }

    public void testScroll() {
        final int size = randomIntBetween(1, 4);
        SearchResponse response = client().prepareSearch("index")
            .setSize(size)
            .setScroll(TimeValue.timeValueMinutes(1))
            .addAggregation(terms("f").field("f"))
            .get();
        assertSearchResponse(response);
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        Terms terms = aggregations.get("f");
        assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());

        int total = response.getHits().getHits().length;
        while (response.getHits().getHits().length > 0) {
            response = client().prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(1)).get();
            assertSearchResponse(response);
            assertNull(response.getAggregations());
            total += response.getHits().getHits().length;
        }
        clearScroll(response.getScrollId());
        assertEquals(numDocs, total);
    }

    public void testLargeRegExTermsAggregation() {
        for (TermsAggregatorFactory.ExecutionMode executionMode : TermsAggregatorFactory.ExecutionMode.values()) {
            TermsAggregationBuilder termsAggregation = terms("my_terms").field("f")
                .includeExclude(getLargeStringInclude())
                .executionHint(executionMode.toString());
            runLargeStringAggregationTest(termsAggregation);
        }
    }

    public void testLargeRegExSignificantTermsAggregation() {
        for (SignificantTermsAggregatorFactory.ExecutionMode executionMode : SignificantTermsAggregatorFactory.ExecutionMode.values()) {
            SignificantTermsAggregationBuilder significantTerms = new SignificantTermsAggregationBuilder("my_terms").field("f")
                .includeExclude(getLargeStringInclude())
                .executionHint(executionMode.toString());
            runLargeStringAggregationTest(significantTerms);
        }
    }

    public void testLargeRegExRareTermsAggregation() {
        // currently this only supports "map" as an execution hint
        RareTermsAggregationBuilder rareTerms = new RareTermsAggregationBuilder("my_terms").field("f")
            .includeExclude(getLargeStringInclude())
            .maxDocCount(2);
        runLargeStringAggregationTest(rareTerms);
    }

    private IncludeExclude getLargeStringInclude() {
        return new IncludeExclude(LARGE_STRING, null);
    }

    private void runLargeStringAggregationTest(AggregationBuilder aggregation) {
        boolean exceptionThrown = false;
        IncludeExclude include = new IncludeExclude(LARGE_STRING, null);
        try {
            client().prepareSearch("index").addAggregation(aggregation).get();
        } catch (SearchPhaseExecutionException ex) {
            exceptionThrown = true;
            Throwable nestedException = ex.getCause();
            assertNotNull(nestedException);
            assertTrue(nestedException instanceof OpenSearchException);
            assertNotNull(nestedException.getCause());
            assertTrue(nestedException.getCause() instanceof IllegalArgumentException);
            String actualExceptionMessage = nestedException.getCause().getMessage();
            assertTrue(actualExceptionMessage.startsWith(LARGE_STRING_EXCEPTION_MESSAGE));
        }
        assertTrue("Exception should have been thrown", exceptionThrown);
    }
}

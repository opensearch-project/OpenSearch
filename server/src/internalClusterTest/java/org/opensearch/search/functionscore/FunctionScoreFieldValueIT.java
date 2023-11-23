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

package org.opensearch.search.functionscore;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.search.SearchHit;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertOrderedSearchHits;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for the {@code field_value_factor} function in a function_score query.
 */
public class FunctionScoreFieldValueIT extends ParameterizedOpenSearchIntegTestCase {

    public FunctionScoreFieldValueIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    public void testFieldValueFactor() throws IOException, InterruptedException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", randomFrom(new String[] { "short", "float", "long", "integer", "double" }))
                    .endObject()
                    .startObject("body")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            ).get()
        );

        client().prepareIndex("test").setId("1").setSource("test", 5, "body", "foo").get();
        client().prepareIndex("test").setId("2").setSource("test", 17, "body", "foo").get();
        client().prepareIndex("test").setId("3").setSource("body", "bar").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        // document 2 scores higher because 17 > 5
        SearchResponse response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(functionScoreQuery(simpleQueryStringQuery("foo"), fieldValueFactorFunction("test")))
            .get();
        assertOrderedSearchHits(response, "2", "1");

        // try again, but this time explicitly use the do-nothing modifier
        response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(
                functionScoreQuery(
                    simpleQueryStringQuery("foo"),
                    fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.NONE)
                )
            )
            .get();
        assertOrderedSearchHits(response, "2", "1");

        // document 1 scores higher because 1/5 > 1/17
        response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(
                functionScoreQuery(
                    simpleQueryStringQuery("foo"),
                    fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL)
                )
            )
            .get();
        assertOrderedSearchHits(response, "1", "2");

        // doc 3 doesn't have a "test" field, so an exception will be thrown
        try {
            response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("test")))
                .get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // We are expecting an exception, because 3 has no field
        }

        // doc 3 doesn't have a "test" field but we're defaulting it to 100 so it should be last
        response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(
                functionScoreQuery(
                    matchAllQuery(),
                    fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100)
                )
            )
            .get();
        assertOrderedSearchHits(response, "1", "2", "3");

        // field is not mapped but we're defaulting it to 100 so all documents should have the same score
        response = client().prepareSearch("test")
            .setExplain(randomBoolean())
            .setQuery(
                functionScoreQuery(
                    matchAllQuery(),
                    fieldValueFactorFunction("notmapped").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).missing(100)
                )
            )
            .get();
        assertEquals(response.getHits().getAt(0).getScore(), response.getHits().getAt(2).getScore(), 0);

        client().prepareIndex("test").setId("2").setSource("test", -1, "body", "foo").get();
        refresh();

        // -1 divided by 0 is infinity, which should provoke an exception.
        try {
            response = client().prepareSearch("test")
                .setExplain(randomBoolean())
                .setQuery(
                    functionScoreQuery(
                        simpleQueryStringQuery("foo"),
                        fieldValueFactorFunction("test").modifier(FieldValueFactorFunction.Modifier.RECIPROCAL).factor(0)
                    )
                )
                .get();
            assertFailures(response);
        } catch (SearchPhaseExecutionException e) {
            // This is fine, the query will throw an exception if executed
            // locally, instead of just having failures
        }
    }

    public void testFieldValueFactorExplain() throws IOException, InterruptedException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("test")
                    .field("type", randomFrom(new String[] { "short", "float", "long", "integer", "double" }))
                    .endObject()
                    .startObject("body")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            ).get()
        );

        client().prepareIndex("test").setId("1").setSource("test", 5, "body", "foo").get();
        client().prepareIndex("test").setId("2").setSource("test", 17, "body", "foo").get();
        client().prepareIndex("test").setId("3").setSource("body", "bar").get();

        refresh();
        indexRandomForConcurrentSearch("test");

        // document 2 scores higher because 17 > 5
        final String functionName = "func1";
        final String queryName = "query";
        SearchResponse response = client().prepareSearch("test")
            .setExplain(true)
            .setQuery(
                functionScoreQuery(simpleQueryStringQuery("foo").queryName(queryName), fieldValueFactorFunction("test", functionName))
            )
            .get();
        assertOrderedSearchHits(response, "2", "1");
        SearchHit firstHit = response.getHits().getAt(0);
        assertThat(firstHit.getExplanation().getDetails(), arrayWithSize(2));
        // "description": "sum of: (_name: query)"
        assertThat(firstHit.getExplanation().getDetails()[0].getDescription(), containsString("_name: " + queryName));
        // "description": "field value function(_name: func1): none(doc['test'].value * factor=1.0)"
        assertThat(firstHit.getExplanation().getDetails()[1].toString(), containsString("_name: " + functionName));
    }
}

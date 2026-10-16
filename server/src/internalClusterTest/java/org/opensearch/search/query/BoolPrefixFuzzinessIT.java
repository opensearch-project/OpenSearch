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

package org.opensearch.search.query;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class BoolPrefixFuzzinessIT extends OpenSearchIntegTestCase {

    public void testBoolPrefixWithFuzziness() {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                    .put("index.analysis.analyzer.my_analyzer.filter", "lowercase")
                    .build()
            ).setMapping(
                "my_field",
                "type=text,analyzer=my_analyzer"
            )
        );

        client().prepareIndex("test").setId("1").setSource("my_field", "license").get();
        refresh();
        
        // Debug: Check if the document exists
        SearchResponse checkResponse = client().prepareSearch("test").get();
        logger.info("Document check response: {}", checkResponse);

        // Test with multi_match query
        MultiMatchQueryBuilder multiMatchQuery = new MultiMatchQueryBuilder("lisence", "my_field")
            .type(MultiMatchQueryBuilder.Type.BOOL_PREFIX)
            .fuzziness(Fuzziness.ONE);
        logger.info("Multi-match query: {}", multiMatchQuery);
        
        // Try a direct fuzzy query to see if it works
        QueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery("my_field", "lisence").fuzziness(Fuzziness.ONE);
        SearchResponse fuzzyResponse = client().prepareSearch("test")
            .setQuery(fuzzyQuery)
            .get();
        logger.info("Response for direct fuzzy query: {}", fuzzyResponse);
        
        // Now try the multi_match query
        SearchResponse response = client().prepareSearch("test")
            .setQuery(multiMatchQuery)
            .get();
        logger.info("Response for multi_match query: {}", response);
        assertHitCount(response, 1L);

        // Test with match_bool_prefix query
        response = client().prepareSearch("test")
            .setQuery(
                new MatchBoolPrefixQueryBuilder("my_field", "lisence")
                    .fuzziness(Fuzziness.ONE)
            )
            .get();
        logger.info("Response for match_bool_prefix query: {}", response);
        assertHitCount(response, 1L);

        // Test with multi_match query and multiple terms
        response = client().prepareSearch("test")
            .setQuery(
                new MultiMatchQueryBuilder("opensarch lisence", "my_field")
                    .type(MultiMatchQueryBuilder.Type.BOOL_PREFIX)
                    .fuzziness(Fuzziness.ONE)
            )
            .get();
        assertHitCount(response, 1L);

        // Test with match_bool_prefix query and multiple terms
        response = client().prepareSearch("test")
            .setQuery(
                new MatchBoolPrefixQueryBuilder("my_field", "opensarch lisence")
                    .fuzziness(Fuzziness.ONE)
            )
            .get();
        assertHitCount(response, 1L);
    }
}

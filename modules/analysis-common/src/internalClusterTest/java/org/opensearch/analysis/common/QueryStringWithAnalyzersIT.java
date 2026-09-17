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

package org.opensearch.analysis.common;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.Operator;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class QueryStringWithAnalyzersIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public QueryStringWithAnalyzersIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisModulePlugin.class);
    }

    /**
     * Validates that we properly split fields using the word delimiter filter in query_string.
     */
    public void testCustomWordDelimiterQueryString() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put("analysis.analyzer.my_analyzer.type", "custom")
                        .put("analysis.analyzer.my_analyzer.tokenizer", "whitespace")
                        .put("analysis.analyzer.my_analyzer.filter", "custom_word_delimiter")
                        .put("analysis.filter.custom_word_delimiter.type", "word_delimiter")
                        .put("analysis.filter.custom_word_delimiter.generate_word_parts", "true")
                        .put("analysis.filter.custom_word_delimiter.generate_number_parts", "false")
                        .put("analysis.filter.custom_word_delimiter.catenate_numbers", "true")
                        .put("analysis.filter.custom_word_delimiter.catenate_words", "false")
                        .put("analysis.filter.custom_word_delimiter.split_on_case_change", "false")
                        .put("analysis.filter.custom_word_delimiter.split_on_numerics", "false")
                        .put("analysis.filter.custom_word_delimiter.stem_english_possessive", "false")
                )
                .setMapping("field1", "type=text,analyzer=my_analyzer", "field2", "type=text,analyzer=my_analyzer")
        );

        client().prepareIndex("test").setId("1").setSource("field1", "foo bar baz", "field2", "not needed").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(queryStringQuery("foo.baz").defaultOperator(Operator.AND).field("field1").field("field2"))
            .get();
        assertHitCount(response, 1L);
    }

    private static final String DIGITS_INDEX = "digits_analyzer_idx";

    private void createDigitsOnlyAnalyzerIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DIGITS_INDEX)
                .setSettings(
                    Settings.builder()
                        .put("analysis.char_filter.strip_nondigits.type", "pattern_replace")
                        .put("analysis.char_filter.strip_nondigits.pattern", "\\D")
                        .put("analysis.char_filter.strip_nondigits.replacement", "")
                        .put("analysis.filter.remove_empty_tokens.type", "length")
                        .put("analysis.filter.remove_empty_tokens.min", 1)
                        .put("analysis.filter.replace_empty_with_null.type", "pattern_replace")
                        .put("analysis.filter.replace_empty_with_null.pattern", "^$")
                        .put("analysis.filter.replace_empty_with_null.replacement", "<NULL>")
                        .put("analysis.analyzer.digits_only.type", "custom")
                        .put("analysis.analyzer.digits_only.char_filter", "strip_nondigits")
                        .put("analysis.analyzer.digits_only.tokenizer", "keyword")
                        .put("analysis.analyzer.digits_only.filter", "remove_empty_tokens")
                        .put("analysis.analyzer.digits_only_search.type", "custom")
                        .put("analysis.analyzer.digits_only_search.char_filter", "strip_nondigits")
                        .put("analysis.analyzer.digits_only_search.tokenizer", "keyword")
                        .put("analysis.analyzer.digits_only_search.filter", "replace_empty_with_null")
                )
                .setMapping("number_field", "type=text,analyzer=digits_only,search_analyzer=digits_only_search")
        );

        client().prepareIndex(DIGITS_INDEX).setId("1").setSource("number_field", "1234").get();
        refresh();
    }

    /**
     * Issue #21280: when the analyzer strips every literal character of a wildcard term, the
     * empty token would otherwise be compiled into a match-all automaton and silently widen
     * the search to an exists query. Covers both the explicit analyzer override and the
     * field-configured search_analyzer paths reported in the issue.
     */
    public void testWildcardWithStrippedLiteralsReturnsNoHits() {
        createDigitsOnlyAnalyzerIndex();

        assertHitCount(
            client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("*asdf*").field("number_field").analyzer("digits_only")).get(),
            0L
        );
        assertHitCount(
            client().prepareSearch(DIGITS_INDEX)
                .setQuery(queryStringQuery("*asdf*").field("number_field").analyzer("digits_only_search"))
                .get(),
            0L
        );
        assertHitCount(client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("*asdf*").field("number_field")).get(), 0L);
        assertHitCount(
            client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("*asdf*").field("number_field").analyzeWildcard(true)).get(),
            0L
        );
    }

    public void testWildcardPreservesUserTypedMatchAll() {
        createDigitsOnlyAnalyzerIndex();

        assertHitCount(client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("**").field("number_field")).get(), 1L);
        assertHitCount(client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("***").field("number_field")).get(), 1L);
    }

    public void testWildcardWithSurvivingLiteralsStillMatches() {
        createDigitsOnlyAnalyzerIndex();

        assertHitCount(
            client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("*123*").field("number_field").analyzer("digits_only")).get(),
            1L
        );
        assertHitCount(client().prepareSearch(DIGITS_INDEX).setQuery(queryStringQuery("123*").field("number_field")).get(), 1L);
    }
}

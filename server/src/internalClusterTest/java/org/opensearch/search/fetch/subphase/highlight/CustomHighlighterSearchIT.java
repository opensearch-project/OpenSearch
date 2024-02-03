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

package org.opensearch.search.fetch.subphase.highlight;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHighlight;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test for highlighters registered by a plugin.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class CustomHighlighterSearchIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public CustomHighlighterSearchIT(Settings staticSettings) {
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
        return Arrays.asList(CustomHighlighterPlugin.class);
    }

    @Before
    protected void setup() throws Exception {
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("name", "arbitrary content", "other_name", "foo", "other_other_name", "bar"),
            client().prepareIndex("test").setId("2").setSource("other_name", "foo", "other_other_name", "bar")
        );
    }

    public void testThatCustomHighlightersAreSupported() throws IOException {
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .highlighter(new HighlightBuilder().field("name").highlighterType("test-custom"))
            .get();
        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
    }

    public void testThatCustomHighlighterCanBeConfiguredPerField() throws Exception {
        HighlightBuilder.Field highlightConfig = new HighlightBuilder.Field("name");
        highlightConfig.highlighterType("test-custom");
        Map<String, Object> options = new HashMap<>();
        options.put("myFieldOption", "someValue");
        highlightConfig.options(options);

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .highlighter(new HighlightBuilder().field(highlightConfig))
            .get();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 0, "name", 1, equalTo("field:myFieldOption:someValue"));
    }

    public void testThatCustomHighlighterCanBeConfiguredGlobally() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("myGlobalOption", "someValue");

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .highlighter(new HighlightBuilder().field("name").highlighterType("test-custom").options(options))
            .get();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 0, "name", 1, equalTo("field:myGlobalOption:someValue"));
    }

    public void testThatCustomHighlighterReceivesFieldsInOrder() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).should(QueryBuilders.termQuery("name", "arbitrary")))
            .highlighter(
                new HighlightBuilder().highlighterType("test-custom")
                    .field("name")
                    .field("other_name")
                    .field("other_other_name")
                    .useExplicitFieldOrder(true)
            )
            .get();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 0, "other_name", 0, equalTo("standard response for other_name at position 2"));
        assertHighlight(searchResponse, 0, "other_other_name", 0, equalTo("standard response for other_other_name at position 3"));
        assertHighlight(searchResponse, 1, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 1, "other_name", 0, equalTo("standard response for other_name at position 2"));
        assertHighlight(searchResponse, 1, "other_other_name", 0, equalTo("standard response for other_other_name at position 3"));
    }
}

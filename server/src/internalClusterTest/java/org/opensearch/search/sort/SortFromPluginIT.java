/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.plugin.CustomSortBuilder;
import org.opensearch.search.sort.plugin.CustomSortPlugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class SortFromPluginIT extends ParameterizedOpenSearchIntegTestCase {
    public SortFromPluginIT(Settings settings) {
        super(settings);
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomSortPlugin.class, InternalSettingsPlugin.class);
    }

    public void testPluginSort() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch("test").addSort(new CustomSortBuilder("field", SortOrder.ASC)).get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test").addSort(new CustomSortBuilder("field", SortOrder.DESC)).get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPluginSortXContent() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();
        indexRandomForConcurrentSearch("test");

        // builder -> json -> builder
        SearchResponse searchResponse = client().prepareSearch("test")
            .setSource(
                SearchSourceBuilder.fromXContent(
                    createParser(
                        JsonXContent.jsonXContent,
                        new SearchSourceBuilder().sort(new CustomSortBuilder("field", SortOrder.ASC)).toString()
                    )
                )
            )
            .get();

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setSource(
                SearchSourceBuilder.fromXContent(
                    createParser(
                        JsonXContent.jsonXContent,
                        new SearchSourceBuilder().sort(new CustomSortBuilder("field", SortOrder.DESC)).toString()
                    )
                )
            )
            .get();

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }
}

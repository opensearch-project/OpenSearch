/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.TestEnvironment;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class SearchPipelineCommonModulePluginTests extends OpenSearchTestCase {

    public void testRequestProcessorAllowlist() throws IOException {
        final String key = SearchPipelineCommonModulePlugin.REQUEST_PROCESSORS_ALLOWLIST_SETTING.getKey();
        runAllowlistTest(key, List.of(), SearchPipelineCommonModulePlugin::getRequestProcessors);
        runAllowlistTest(key, List.of("filter_query"), SearchPipelineCommonModulePlugin::getRequestProcessors);
        runAllowlistTest(key, List.of("script"), SearchPipelineCommonModulePlugin::getRequestProcessors);
        runAllowlistTest(key, List.of("oversample", "script"), SearchPipelineCommonModulePlugin::getRequestProcessors);
        runAllowlistTest(key, List.of("filter_query", "script", "oversample"), SearchPipelineCommonModulePlugin::getRequestProcessors);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> runAllowlistTest(key, List.of("foo"), SearchPipelineCommonModulePlugin::getRequestProcessors)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("foo"));
    }

    public void testResponseProcessorAllowlist() throws IOException {
        final String key = SearchPipelineCommonModulePlugin.RESPONSE_PROCESSORS_ALLOWLIST_SETTING.getKey();
        runAllowlistTest(key, List.of(), SearchPipelineCommonModulePlugin::getResponseProcessors);
        runAllowlistTest(key, List.of("rename_field"), SearchPipelineCommonModulePlugin::getResponseProcessors);
        runAllowlistTest(key, List.of("truncate_hits"), SearchPipelineCommonModulePlugin::getResponseProcessors);
        runAllowlistTest(key, List.of("collapse", "truncate_hits"), SearchPipelineCommonModulePlugin::getResponseProcessors);
        runAllowlistTest(
            key,
            List.of("rename_field", "truncate_hits", "collapse"),
            SearchPipelineCommonModulePlugin::getResponseProcessors
        );

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> runAllowlistTest(key, List.of("foo"), SearchPipelineCommonModulePlugin::getResponseProcessors)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("foo"));
    }

    public void testSearchPhaseResultsProcessorAllowlist() throws IOException {
        final String key = SearchPipelineCommonModulePlugin.SEARCH_PHASE_RESULTS_PROCESSORS_ALLOWLIST_SETTING.getKey();
        runAllowlistTest(key, List.of(), SearchPipelineCommonModulePlugin::getSearchPhaseResultsProcessors);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> runAllowlistTest(key, List.of("foo"), SearchPipelineCommonModulePlugin::getSearchPhaseResultsProcessors)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("foo"));
    }

    private void runAllowlistTest(
        String settingKey,
        List<String> allowlist,
        BiFunction<SearchPipelineCommonModulePlugin, SearchPipelinePlugin.Parameters, Map<String, ?>> function
    ) throws IOException {
        final Settings settings = Settings.builder().putList(settingKey, allowlist).build();
        try (SearchPipelineCommonModulePlugin plugin = new SearchPipelineCommonModulePlugin()) {
            assertEquals(Set.copyOf(allowlist), function.apply(plugin, createParameters(settings)).keySet());
        }
    }

    public void testAllowlistNotSpecified() throws IOException {
        final Settings settings = Settings.EMPTY;
        try (SearchPipelineCommonModulePlugin plugin = new SearchPipelineCommonModulePlugin()) {
            assertEquals(Set.of("oversample", "filter_query", "script"), plugin.getRequestProcessors(createParameters(settings)).keySet());
            assertEquals(
                Set.of("rename_field", "truncate_hits", "collapse", "split"),
                plugin.getResponseProcessors(createParameters(settings)).keySet()
            );
            assertEquals(Set.of(), plugin.getSearchPhaseResultsProcessors(createParameters(settings)).keySet());
        }
    }

    private static SearchPipelinePlugin.Parameters createParameters(Settings settings) {
        return new SearchPipelinePlugin.Parameters(
            TestEnvironment.newEnvironment(Settings.builder().put(settings).put("path.home", "").build()),
            null,
            null,
            null,
            () -> 0L,
            (a, b) -> null,
            null,
            null,
            $ -> {},
            null
        );
    }
}

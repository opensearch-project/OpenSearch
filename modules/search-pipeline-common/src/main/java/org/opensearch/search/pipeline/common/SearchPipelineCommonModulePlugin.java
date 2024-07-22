/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Plugin providing common search request/response processors for use in search pipelines.
 */
public class SearchPipelineCommonModulePlugin extends Plugin implements SearchPipelinePlugin {

    static final Setting<List<String>> REQUEST_PROCESSORS_ALLOWLIST_SETTING = Setting.listSetting(
        "search.pipeline.common.request.processors.allowed",
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    static final Setting<List<String>> RESPONSE_PROCESSORS_ALLOWLIST_SETTING = Setting.listSetting(
        "search.pipeline.common.response.processors.allowed",
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    static final Setting<List<String>> SEARCH_PHASE_RESULTS_PROCESSORS_ALLOWLIST_SETTING = Setting.listSetting(
        "search.pipeline.common.search.phase.results.processors.allowed",
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * No constructor needed, but build complains if we don't have a constructor with JavaDoc.
     */
    public SearchPipelineCommonModulePlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            REQUEST_PROCESSORS_ALLOWLIST_SETTING,
            RESPONSE_PROCESSORS_ALLOWLIST_SETTING,
            SEARCH_PHASE_RESULTS_PROCESSORS_ALLOWLIST_SETTING
        );
    }

    /**
     * Returns a map of processor factories.
     *
     * @param parameters The parameters required for creating the processor factories.
     * @return A map of processor factories, where the keys are the processor types and the values are the corresponding factory instances.
     */
    @Override
    public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
        return filterForAllowlistSetting(
            REQUEST_PROCESSORS_ALLOWLIST_SETTING,
            parameters.env.settings(),
            Map.of(
                FilterQueryRequestProcessor.TYPE,
                new FilterQueryRequestProcessor.Factory(parameters.namedXContentRegistry),
                ScriptRequestProcessor.TYPE,
                new ScriptRequestProcessor.Factory(parameters.scriptService),
                OversampleRequestProcessor.TYPE,
                new OversampleRequestProcessor.Factory()
            )
        );
    }

    @Override
    public Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
        return filterForAllowlistSetting(
            RESPONSE_PROCESSORS_ALLOWLIST_SETTING,
            parameters.env.settings(),
            Map.of(
                RenameFieldResponseProcessor.TYPE,
                new RenameFieldResponseProcessor.Factory(),
                TruncateHitsResponseProcessor.TYPE,
                new TruncateHitsResponseProcessor.Factory(),
                CollapseResponseProcessor.TYPE,
                new CollapseResponseProcessor.Factory(),
                SplitResponseProcessor.TYPE,
                new SplitResponseProcessor.Factory()
            )
        );
    }

    @Override
    public Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Parameters parameters) {
        return filterForAllowlistSetting(SEARCH_PHASE_RESULTS_PROCESSORS_ALLOWLIST_SETTING, parameters.env.settings(), Map.of());
    }

    private <T extends Processor> Map<String, Processor.Factory<T>> filterForAllowlistSetting(
        Setting<List<String>> allowlistSetting,
        Settings settings,
        Map<String, Processor.Factory<T>> map
    ) {
        if (allowlistSetting.exists(settings) == false) {
            return Map.copyOf(map);
        }
        final Set<String> allowlist = Set.copyOf(allowlistSetting.get(settings));
        // Assert that no unknown processors are defined in the allowlist
        final Set<String> unknownAllowlistProcessors = allowlist.stream()
            .filter(p -> map.containsKey(p) == false)
            .collect(Collectors.toUnmodifiableSet());
        if (unknownAllowlistProcessors.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Processor(s) " + unknownAllowlistProcessors + " were defined in [" + allowlistSetting.getKey() + "] but do not exist"
            );
        }
        return map.entrySet()
            .stream()
            .filter(e -> allowlist.contains(e.getKey()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

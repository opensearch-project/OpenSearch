/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;

import org.opensearch.script.Script;
import org.opensearch.script.ScriptException;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.SearchScript;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.common.helpers.SearchRequestMap;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that evaluates a script with a search request in its context
 * and then returns the modified search request.
 */
public final class ScriptRequestProcessor extends AbstractProcessor implements SearchRequestProcessor {
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "script";

    private final Script script;
    private final ScriptService scriptService;
    private final SearchScript precompiledSearchScript;

    /**
     * Processor that evaluates a script with a search request in its context
     *
     * @param tag The processor's tag.
     * @param description The processor's description.
     * @param ignoreFailure The option to ignore failure
     * @param script The {@link Script} to execute.
     * @param precompiledSearchScript The {@link Script} precompiled
     * @param scriptService The {@link ScriptService} used to execute the script.
     */
    ScriptRequestProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        Script script,
        @Nullable SearchScript precompiledSearchScript,
        ScriptService scriptService
    ) {
        super(tag, description, ignoreFailure);
        this.script = script;
        this.precompiledSearchScript = precompiledSearchScript;
        this.scriptService = scriptService;
    }

    /**
     * Executes the script with the search request in context.
     *
     * @param request The search request passed into the script context.
     * @return The modified search request.
     * @throws Exception if an error occurs while processing the request.
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        // assert request is not null and source is not null
        if (request == null || request.source() == null) {
            throw new IllegalArgumentException("search request must not be null");
        }
        final SearchScript searchScript;
        if (precompiledSearchScript == null) {
            SearchScript.Factory factory = scriptService.compile(script, SearchScript.CONTEXT);
            searchScript = factory.newInstance(script.getParams());
        } else {
            searchScript = precompiledSearchScript;
        }
        // execute the script with the search request in context
        searchScript.execute(Map.of("_source", new SearchRequestMap(request)));
        return request;
    }

    /**
     * Returns the type of the processor.
     *
     * @return The processor type.
     */
    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Returns the script used by the processor.
     *
     * @return The script.
     */
    Script getScript() {
        return script;
    }

    /**
     * Returns the precompiled search script used by the processor.
     *
     * @return The precompiled search script.
     */
    SearchScript getPrecompiledSearchScript() {
        return precompiledSearchScript;
    }

    /**
     * Factory class for creating {@link ScriptRequestProcessor}.
     */
    public static final class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final List<String> SCRIPT_CONFIG_KEYS = List.of("id", "source", "inline", "lang", "params", "options");

        private final ScriptService scriptService;

        /**
         * Constructs a new Factory instance with the specified {@link ScriptService}.
         *
         * @param scriptService The {@link ScriptService} used to execute scripts.
         */
        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ScriptRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> registry,
            String processorTag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            Map<String, Object> scriptConfig = new HashMap<>();
            for (String key : SCRIPT_CONFIG_KEYS) {
                Object val = config.remove(key);
                if (val != null) {
                    scriptConfig.put(key, val);
                }
            }
            try (
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(scriptConfig);
                InputStream stream = BytesReference.bytes(builder).streamInput();
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
            ) {
                Script script = Script.parse(parser);

                // verify script is able to be compiled before successfully creating processor.
                SearchScript searchScript = null;
                try {
                    final SearchScript.Factory factory = scriptService.compile(script, SearchScript.CONTEXT);
                    if (ScriptType.INLINE.equals(script.getType())) {
                        searchScript = factory.newInstance(script.getParams());
                    }
                } catch (ScriptException e) {
                    throw newConfigurationException(TYPE, processorTag, null, e);
                }
                return new ScriptRequestProcessor(processorTag, description, ignoreFailure, script, searchScript, scriptService);
            }
        }
    }
}

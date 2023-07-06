/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.script.ScriptService;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.threadpool.Scheduler;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * An extension point for {@link Plugin} implementation to add custom search pipeline processors.
 *
 * @opensearch.api
 */
public interface SearchPipelinePlugin {
    /**
     * Returns additional search pipeline request processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.search.pipeline.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Returns additional search pipeline response processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.search.pipeline.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Returns additional search pipeline search phase results processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.search.pipeline.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Infrastructure class that holds services that can be used by processor factories to create processor instances
     * and that gets passed around to all {@link SearchPipelinePlugin}s.
     */
    class Parameters {

        /**
         * Useful to provide access to the node's environment like config directory to processor factories.
         */
        public final Environment env;

        /**
         * Provides processors script support.
         */
        public final ScriptService scriptService;

        /**
         * Provide analyzer support
         */
        public final AnalysisRegistry analysisRegistry;

        /**
         * Allows processors to read headers set by {@link org.opensearch.action.support.ActionFilter}
         * instances that have run while handling the current search.
         */
        public final ThreadContext threadContext;

        public final LongSupplier relativeTimeSupplier;

        public final SearchPipelineService searchPipelineService;

        public final Consumer<Runnable> genericExecutor;

        public final NamedXContentRegistry namedXContentRegistry;

        /**
         * Provides scheduler support
         */
        public final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler;

        /**
         * Provides access to the node's cluster client
         */
        public final Client client;

        public Parameters(
            Environment env,
            ScriptService scriptService,
            AnalysisRegistry analysisRegistry,
            ThreadContext threadContext,
            LongSupplier relativeTimeSupplier,
            BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler,
            SearchPipelineService searchPipelineService,
            Client client,
            Consumer<Runnable> genericExecutor,
            NamedXContentRegistry namedXContentRegistry
        ) {
            this.env = env;
            this.scriptService = scriptService;
            this.threadContext = threadContext;
            this.analysisRegistry = analysisRegistry;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            this.searchPipelineService = searchPipelineService;
            this.client = client;
            this.genericExecutor = genericExecutor;
            this.namedXContentRegistry = namedXContentRegistry;
        }

    }
}

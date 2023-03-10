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

package org.opensearch.search.pipeline;

import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A processor implementation may modify the request or response from a search call.
 * Whether changes are made and what exactly is modified is up to the implementation.
 * <p>
 * Processors may get called concurrently and thus need to be thread-safe.
 *
 *
 * TODO: Refactor {@link org.opensearch.ingest.Processor} to extend this interface, and specialize to IngestProcessor.
 *
 * @opensearch.internal
 */
public interface Processor {

    /**
     * Gets the type of processor
     */
    String getType();

    /**
     * Gets the tag of a processor.
     */
    String getTag();

    /**
     * Gets the description of a processor.
     */
    String getDescription();

    /**
     * A factory that knows how to construct a processor based on a map of maps.
     */
    interface Factory {

        /**
         * Creates a processor based on the specified map of maps config.
         *
         * @param processorFactories Other processors which may be created inside this processor
         * @param tag                The tag for the processor
         * @param description        A short description of what this processor does
         * @param config             The configuration for the processor
         *
         *                           <b>Note:</b> Implementations are responsible for removing the used configuration
         *                           keys, so that after creation the config map should be empty.
         */
        Processor create(Map<String, Factory> processorFactories, String tag, String description, Map<String, Object> config)
            throws Exception;
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

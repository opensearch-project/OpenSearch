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

package org.opensearch.ingest;

import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A processor implementation may modify the data belonging to a document.
 * Whether changes are made and what exactly is modified is up to the implementation.
 * <p>
 * Processors may get called concurrently and thus need to be thread-safe.
 *
 * @opensearch.internal
 */
public interface Processor {

    /**
     * Introspect and potentially modify the incoming data.
     * <p>
     * Expert method: only override this method if a processor implementation needs to make an asynchronous call,
     * otherwise just overwrite {@link #execute(IngestDocument)}.
     */
    default void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        final IngestDocument result;
        try {
            result = execute(ingestDocument);
        } catch (Exception e) {
            handler.accept(null, e);
            return;
        }
        handler.accept(result, null);
    }

    /**
     * Introspect and potentially modify the incoming data.
     *
     * @return If <code>null</code> is returned then the current document will be dropped and not be indexed,
     *         otherwise this document will be kept and indexed
     */
    IngestDocument execute(IngestDocument ingestDocument) throws Exception;

    /**
     * Process batched documents and they could be potentially modified by processors.
     * Only override this method if the processor can benefit from processing documents in batches, otherwise, please
     * use default implementation.
     *
     * @param ingestDocumentWrappers a list of wrapped IngestDocument
     * @param handler callback with IngestDocument result and exception wrapped in IngestDocumentWrapper.
     */
    default void batchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        if (ingestDocumentWrappers.isEmpty()) {
            handler.accept(Collections.emptyList());
            return;
        }
        int size = ingestDocumentWrappers.size();
        AtomicInteger counter = new AtomicInteger(size);
        AtomicArray<IngestDocumentWrapper> results = new AtomicArray<>(size);
        for (int i = 0; i < size; ++i) {
            innerExecute(i, ingestDocumentWrappers.get(i), results, counter, handler);
        }
    }

    private void innerExecute(
        int slot,
        IngestDocumentWrapper ingestDocumentWrapper,
        AtomicArray<IngestDocumentWrapper> results,
        AtomicInteger counter,
        Consumer<List<IngestDocumentWrapper>> handler
    ) {
        execute(ingestDocumentWrapper.getIngestDocument(), (doc, ex) -> {
            results.set(slot, new IngestDocumentWrapper(ingestDocumentWrapper.getSlot(), doc, ex));
            if (counter.decrementAndGet() == 0) {
                handler.accept(results.asList());
            }
        });
    }

    /**
     * Gets the type of a processor
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
         *  @param processorFactories Other processors which may be created inside this processor
         * @param tag The tag for the processor
         * @param description A short description of what this processor does
         * @param config The configuration for the processor
         *
         * <b>Note:</b> Implementations are responsible for removing the used configuration keys, so that after
         */
        Processor create(Map<String, Factory> processorFactories, String tag, String description, Map<String, Object> config)
            throws Exception;
    }

    /**
     * Infrastructure class that holds services that can be used by processor factories to create processor instances
     * and that gets passed around to all {@link org.opensearch.plugins.IngestPlugin}s.
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
         * instances that have run prior to in ingest.
         */
        public final ThreadContext threadContext;

        public final LongSupplier relativeTimeSupplier;

        public final IngestService ingestService;

        public final Consumer<Runnable> genericExecutor;

        /**
         * Provides scheduler support
         */
        public final BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler;

        /**
         * Provides access to the node client
         */
        public final Client client;

        public final IndicesService indicesService;

        public Parameters(
            Environment env,
            ScriptService scriptService,
            AnalysisRegistry analysisRegistry,
            ThreadContext threadContext,
            LongSupplier relativeTimeSupplier,
            BiFunction<Long, Runnable, Scheduler.ScheduledCancellable> scheduler,
            IngestService ingestService,
            Client client,
            Consumer<Runnable> genericExecutor,
            IndicesService indicesService
        ) {
            this.env = env;
            this.scriptService = scriptService;
            this.threadContext = threadContext;
            this.analysisRegistry = analysisRegistry;
            this.relativeTimeSupplier = relativeTimeSupplier;
            this.scheduler = scheduler;
            this.ingestService = ingestService;
            this.client = client;
            this.genericExecutor = genericExecutor;
            this.indicesService = indicesService;
        }

    }
}

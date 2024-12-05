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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.ingest;

import org.opensearch.action.ActionRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ingest.CompoundProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Pipeline;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.opensearch.ingest.TrackingResultProcessor.decorate;

/**
 * Service to simulate pipeline execution
 *
 * @opensearch.internal
 */
class SimulateExecutionService {

    private static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;
    private final IngestService ingestService;

    SimulateExecutionService(ThreadPool threadPool, IngestService ingestService) {
        this.threadPool = threadPool;
        this.ingestService = ingestService;
    }

    void executeDocument(
        Pipeline pipeline,
        IngestDocument ingestDocument,
        boolean verbose,
        BiConsumer<SimulateDocumentResult, Exception> handler
    ) {
        if (verbose) {
            List<SimulateProcessorResult> processorResultList = new CopyOnWriteArrayList<>();
            CompoundProcessor verbosePipelineProcessor = decorate(pipeline.getCompoundProcessor(), null, processorResultList);
            Pipeline verbosePipeline = new Pipeline(
                pipeline.getId(),
                pipeline.getDescription(),
                pipeline.getVersion(),
                verbosePipelineProcessor
            );
            ingestDocument.executePipeline(verbosePipeline, (result, e) -> {
                handler.accept(new SimulateDocumentVerboseResult(processorResultList), e);
            });
        } else {
            ingestDocument.executePipeline(pipeline, (result, e) -> {
                if (e == null) {
                    handler.accept(new SimulateDocumentBaseResult(result), null);
                } else {
                    handler.accept(new SimulateDocumentBaseResult(e), null);
                }
            });
        }
    }

    public void execute(SimulatePipelineRequest.Parsed request, ActionListener<SimulatePipelineResponse> listener) {

        ingestService.validateProcessorCountForIngestPipeline(request.getPipeline());

        threadPool.executor(THREAD_POOL_NAME).execute(ActionRunnable.wrap(listener, l -> {
            final AtomicInteger counter = new AtomicInteger();
            final List<SimulateDocumentResult> responses = new CopyOnWriteArrayList<>(
                new SimulateDocumentBaseResult[request.getDocuments().size()]
            );

            if (request.getDocuments().isEmpty()) {
                l.onResponse(new SimulatePipelineResponse(request.getPipeline().getId(), request.isVerbose(), responses));
                return;
            }

            int iter = 0;
            for (IngestDocument ingestDocument : request.getDocuments()) {
                final int index = iter;
                executeDocument(request.getPipeline(), ingestDocument, request.isVerbose(), (response, e) -> {
                    if (response != null) {
                        responses.set(index, response);
                    }
                    if (counter.incrementAndGet() == request.getDocuments().size()) {
                        listener.onResponse(new SimulatePipelineResponse(request.getPipeline().getId(), request.isVerbose(), responses));
                    }
                });
                iter++;
            }
        }));
    }
}

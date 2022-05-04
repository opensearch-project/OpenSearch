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

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.ingest.IngestService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Perform the action of simulating a pipeline
 *
 * @opensearch.internal
 */
public class SimulatePipelineTransportAction extends HandledTransportAction<SimulatePipelineRequest, SimulatePipelineResponse> {

    private final IngestService ingestService;
    private final SimulateExecutionService executionService;

    @Inject
    public SimulatePipelineTransportAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IngestService ingestService
    ) {
        super(
            SimulatePipelineAction.NAME,
            transportService,
            actionFilters,
            (Writeable.Reader<SimulatePipelineRequest>) SimulatePipelineRequest::new
        );
        this.ingestService = ingestService;
        this.executionService = new SimulateExecutionService(threadPool);
    }

    @Override
    protected void doExecute(Task task, SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
        final Map<String, Object> source = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();

        final SimulatePipelineRequest.Parsed simulateRequest;
        try {
            if (request.getId() != null) {
                simulateRequest = SimulatePipelineRequest.parseWithPipelineId(request.getId(), source, request.isVerbose(), ingestService);
            } else {
                simulateRequest = SimulatePipelineRequest.parse(source, request.isVerbose(), ingestService);
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        executionService.execute(simulateRequest, listener);
    }
}

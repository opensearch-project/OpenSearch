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

package org.opensearch.action.support;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

/**
 * A TransportAction that self registers a handler into the transport service
 *
 * @opensearch.internal
 */
public abstract class HandledTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends TransportAction<
    Request,
    Response> {

    protected HandledTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader
    ) {
        this(actionName, true, transportService, actionFilters, requestReader);
    }

    protected HandledTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        String executor
    ) {
        this(actionName, true, transportService, actionFilters, requestReader, executor);
    }

    protected HandledTransportAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader
    ) {
        this(actionName, canTripCircuitBreaker, transportService, actionFilters, requestReader, ThreadPool.Names.SAME);
    }

    protected HandledTransportAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        String executor
    ) {
        super(actionName, actionFilters, transportService.getTaskManager());
        transportService.registerRequestHandler(actionName, executor, false, canTripCircuitBreaker, requestReader, new TransportHandler());
    }

    /**
     * Inner transport handler
     *
     * @opensearch.internal
     */
    class TransportHandler implements TransportRequestHandler<Request> {
        @Override
        public final void messageReceived(final Request request, final TransportChannel channel, Task task) {
            // We already got the task created on the network layer - no need to create it again on the transport layer
            execute(task, request, new ChannelActionListener<>(channel, actionName, request));
        }
    }

}

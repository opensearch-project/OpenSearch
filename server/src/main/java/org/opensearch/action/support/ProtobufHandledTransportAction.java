/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support;

import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufTransportRequestHandler;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;

/**
 * A ProtobufTransportAction that self registers a handler into the transport service
*
* @opensearch.internal
*/
public abstract class ProtobufHandledTransportAction<Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> extends
    ProtobufTransportAction<Request, Response> {

    protected ProtobufHandledTransportAction(
        String actionName,
        TransportService transportService,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> requestReader
    ) {
        this(actionName, true, transportService, actionFilters, requestReader);
    }

    protected ProtobufHandledTransportAction(
        String actionName,
        TransportService transportService,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> requestReader,
        String executor
    ) {
        this(actionName, true, transportService, actionFilters, requestReader, executor);
    }

    protected ProtobufHandledTransportAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> requestReader
    ) {
        this(actionName, canTripCircuitBreaker, transportService, actionFilters, requestReader, ThreadPool.Names.SAME);
    }

    protected ProtobufHandledTransportAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> requestReader,
        String executor
    ) {
        super(actionName, actionFilters, transportService.getTaskManager());
        transportService.registerRequestHandlerProtobuf(
            actionName,
            executor,
            false,
            canTripCircuitBreaker,
            requestReader,
            new TransportHandler()
        );
    }

    /**
     * Inner transport handler
    *
    * @opensearch.internal
    */
    class TransportHandler implements ProtobufTransportRequestHandler<Request> {
        @Override
        public final void messageReceived(final Request request, final TransportChannel channel, ProtobufTask task) {
            // We already got the task created on the network layer - no need to create it again on the transport layer
            execute(task, request, new ProtobufChannelActionListener<>(channel, actionName, request));
        }
    }

}

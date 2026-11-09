/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;

/**
 * Demonstrates streaming transport action that sends multiple responses for a single request
 */
public class TransportStreamDataAction extends TransportAction<StreamDataRequest, StreamDataResponse> {

    private static final Logger logger = LogManager.getLogger(TransportStreamDataAction.class);

    /**
     * Constructor - registers streaming handler
     * @param streamTransportService the stream transport service
     * @param actionFilters action filters
     */
    @Inject
    public TransportStreamDataAction(StreamTransportService streamTransportService, ActionFilters actionFilters) {
        super(StreamDataAction.NAME, actionFilters, streamTransportService.getTaskManager());

        // Register handler for streaming requests
        streamTransportService.registerRequestHandler(
            StreamDataAction.NAME,
            ThreadPool.Names.GENERIC,
            StreamDataRequest::new,
            this::handleStreamRequest
        );
    }

    @Override
    protected void doExecute(Task task, StreamDataRequest request, ActionListener<StreamDataResponse> listener) {
        listener.onFailure(new UnsupportedOperationException("Use StreamTransportService for streaming requests"));
    }

    /**
     * Handles streaming request by sending multiple batched responses
     */
    private void handleStreamRequest(StreamDataRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            // Send multiple responses
            for (int i = 1; i <= request.getCount(); i++) {
                StreamDataResponse response = new StreamDataResponse("Stream data item " + i, i, i == request.getCount());

                channel.sendResponseBatch(response);

                if (i < request.getCount() && request.getDelayMs() > 0) {
                    Thread.sleep(request.getDelayMs());
                }
            }

            channel.completeStream();

        } catch (StreamException e) {
            if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                logger.info("Client cancelled stream: {}", e.getMessage());
            } else {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }
}

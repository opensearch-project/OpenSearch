/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.basic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledStreamTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;

/**
 * Demonstrates streaming transport action that sends multiple responses for a single request.
 *
 * This action handles server-side streaming logic and is registered with StreamTransportService.
 */
public class StreamTransportDataAction extends HandledStreamTransportAction<StreamDataRequest, StreamDataResponse> {

    private static final Logger logger = LogManager.getLogger(StreamTransportDataAction.class);

    /**
     * Constructor - registers streaming handler via parent class
     * @param streamTransportService the stream transport service
     * @param transportService the regular transport service
     * @param actionFilters action filters
     */
    @Inject
    public StreamTransportDataAction(
        StreamTransportService streamTransportService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(StreamDataAction.NAME, transportService, streamTransportService, actionFilters, StreamDataRequest::new);
    }

    @Override
    protected void executeStream(Task task, StreamDataRequest request, TransportChannel channel) throws IOException {
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * A listener that sends the response back to the channel in streaming fashion
 *
 * @opensearch.internal
 */
public class StreamChannelActionListener<Response extends TransportResponse, Request extends TransportRequest>
    implements
        ActionListener<Response> {

    private final TransportChannel channel;
    private final Request request;
    private final String actionName;

    public StreamChannelActionListener(TransportChannel channel, String actionName, Request request) {
        this.channel = channel;
        this.request = request;
        this.actionName = actionName;
    }

    @Override
    public void onResponse(Response response) {
        try {
            // placeholder for batching
            channel.sendResponseBatch(response);
        } finally {
            // this can be removed once batching is supported
            channel.completeStream();
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            channel.sendResponse(e);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}

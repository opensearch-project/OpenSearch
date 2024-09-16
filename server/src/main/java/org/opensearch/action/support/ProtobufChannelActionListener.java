/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support;

import org.opensearch.action.ActionListener;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.core.transport.TransportResponse;

/**
 * Listener for transport channel actions
*
* @opensearch.internal
*/
public final class ProtobufChannelActionListener<Response extends TransportResponse, Request extends TransportRequest>
    implements
        ActionListener<Response> {

    private final TransportChannel channel;
    private final Request request;
    private final String actionName;

    public ProtobufChannelActionListener(TransportChannel channel, String actionName, Request request) {
        this.channel = channel;
        this.request = request;
        this.actionName = actionName;
    }

    @Override
    public void onResponse(Response response) {
        try {
            channel.sendResponse(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        TransportChannel.sendErrorResponse(channel, actionName, request, e);
    }
}

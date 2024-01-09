/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.resthandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.correlation.EventsCorrelationPlugin;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationAction;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationRequest;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Rest action for indexing an event and its correlations
 */
public class RestIndexCorrelationAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestIndexCorrelationAction.class);

    /**
     * Default constructor
     */
    public RestIndexCorrelationAction() {}

    @Override
    public String getName() {
        return "index_correlation_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, EventsCorrelationPlugin.CORRELATION_EVENTS_BASE_URI));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        log.debug(String.format(Locale.ROOT, "%s %s", request.method(), EventsCorrelationPlugin.CORRELATION_EVENTS_BASE_URI));

        XContentParser xcp = request.contentParser();
        IndexCorrelationRequest correlationRequest = IndexCorrelationRequest.parse(xcp);

        return channel -> client.doExecute(IndexCorrelationAction.INSTANCE, correlationRequest, indexCorrelationResponse(channel));
    }

    private RestResponseListener<IndexCorrelationResponse> indexCorrelationResponse(RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(IndexCorrelationResponse indexCorrelationResponse) throws Exception {
                return new BytesRestResponse(
                    indexCorrelationResponse.getStatus(),
                    indexCorrelationResponse.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                );
            }
        };
    }
}

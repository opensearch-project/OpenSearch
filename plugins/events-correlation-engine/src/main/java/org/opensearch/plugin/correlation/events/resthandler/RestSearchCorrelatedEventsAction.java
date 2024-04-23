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
import org.opensearch.plugin.correlation.EventsCorrelationPlugin;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsAction;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsRequest;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsResponse;
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
 * Rest action for searching correlated events
 *
 * @opensearch.api
 */
public class RestSearchCorrelatedEventsAction extends BaseRestHandler {

    private static final Logger log = LogManager.getLogger(RestSearchCorrelatedEventsAction.class);

    /**
     * Default constructor
     */
    public RestSearchCorrelatedEventsAction() {}

    @Override
    public String getName() {
        return "search_correlated_events_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, EventsCorrelationPlugin.CORRELATION_EVENTS_BASE_URI));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        log.debug(String.format(Locale.ROOT, "%s %s", request.method(), EventsCorrelationPlugin.CORRELATION_EVENTS_BASE_URI));

        String index = request.param("index");
        String event = request.param("event");
        String timestampField = request.param("timestamp_field");
        Long timeWindow = request.paramAsLong("time_window", 300000L);
        int noOfNearbyEvents = request.paramAsInt("nearby_events", 5);
        log.info("hit here1-" + index + "-" + event);

        SearchCorrelatedEventsRequest correlatedEventsRequest = new SearchCorrelatedEventsRequest(
            index,
            event,
            timestampField,
            timeWindow,
            noOfNearbyEvents
        );
        return channel -> client.doExecute(
            SearchCorrelatedEventsAction.INSTANCE,
            correlatedEventsRequest,
            searchCorrelatedEventsResponse(channel)
        );
    }

    private RestResponseListener<SearchCorrelatedEventsResponse> searchCorrelatedEventsResponse(RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(SearchCorrelatedEventsResponse searchCorrelatedEventsResponse) throws Exception {
                return new BytesRestResponse(
                    searchCorrelatedEventsResponse.getStatus(),
                    searchCorrelatedEventsResponse.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                );
            }
        };
    }
}

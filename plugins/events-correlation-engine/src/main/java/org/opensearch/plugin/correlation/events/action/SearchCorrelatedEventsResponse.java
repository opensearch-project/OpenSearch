/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.correlation.events.model.EventWithScore;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Transport response for searching correlated events
 *
 * @opensearch.internal
 */
public class SearchCorrelatedEventsResponse extends ActionResponse implements ToXContentObject {

    private List<EventWithScore> events;

    private RestStatus status;

    private static final String EVENTS = "events";

    /**
     * Parameterized ctor of SearchCorrelatedEventsResponse
     * @param events list of neighboring events with scores
     * @param status REST status of the request
     */
    public SearchCorrelatedEventsResponse(List<EventWithScore> events, RestStatus status) {
        super();
        this.events = events;
        this.status = status;
    }

    /**
     * StreamInput ctor of SearchCorrelatedEventsResponse
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public SearchCorrelatedEventsResponse(StreamInput sin) throws IOException {
        this(Collections.unmodifiableList(sin.readList(EventWithScore::new)), sin.readEnum(RestStatus.class));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field(EVENTS, events).endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(events);
        out.writeEnum(status);
    }

    /**
     * get correlated events
     * @return correlated events
     */
    public List<EventWithScore> getEvents() {
        return events;
    }

    /**
     * get REST status
     * @return REST status
     */
    public RestStatus getStatus() {
        return status;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport request to search correlated events
 *
 * @opensearch.internal
 */
public class SearchCorrelatedEventsRequest extends ActionRequest {

    private String index;

    private String event;

    private String timestampField;

    private Long timeWindow;

    private Integer nearbyEvents;

    /**
     * Parameterized ctor of SearchCorrelatedEventsRequest
     * @param index index of the event for which correlations are searched
     * @param event event for which correlations are searched
     * @param timestampField timestamp field in the index
     * @param timeWindow time window dimension of correlation
     * @param nearbyEvents number of nearby correlated events
     */
    public SearchCorrelatedEventsRequest(String index, String event, String timestampField, Long timeWindow, Integer nearbyEvents) {
        super();
        this.index = index;
        this.event = event;
        this.timestampField = timestampField;
        this.timeWindow = timeWindow;
        this.nearbyEvents = nearbyEvents;
    }

    /**
     * StreamInput ctor of SearchCorrelatedEventsRequest
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public SearchCorrelatedEventsRequest(StreamInput sin) throws IOException {
        this(sin.readString(), sin.readString(), sin.readString(), sin.readLong(), sin.readInt());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(event);
        out.writeString(timestampField);
        out.writeLong(timeWindow);
        out.writeInt(nearbyEvents);
    }

    /**
     * get index of the event for which correlations are searched
     * @return index of the event for which correlations are searched
     */
    public String getIndex() {
        return index;
    }

    /**
     * get event for which correlations are searched
     * @return event for which correlations are searched
     */
    public String getEvent() {
        return event;
    }

    /**
     * get timestamp field of the index whose event correlations are searched
     * @return timestamp field of the index whose event correlations are searched
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * get time window dimension of correlation
     * @return time window dimension of correlation
     */
    public Long getTimeWindow() {
        return timeWindow;
    }

    /**
     * get number of nearby correlated events
     * @return number of nearby correlated events
     */
    public Integer getNearbyEvents() {
        return nearbyEvents;
    }
}

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
import java.util.List;
import java.util.Map;

/**
 * Transport request to store correlations
 *
 * @opensearch.internal
 */
public class StoreCorrelationRequest extends ActionRequest {

    private String index;

    private String event;

    private Long timestamp;

    private Map<String, List<String>> eventsAdjacencyList;

    private List<String> tags;

    /**
     * Parameterized ctor of StoreCorrelationRequest
     * @param index index of the event which is being correlated.
     * @param event event which is being correlated.
     * @param timestamp timestamp of the correlated event
     * @param eventsAdjacencyList the adjacency list of events which are correlated with the input event
     * @param tags optional tags for event to be correlated.
     */
    public StoreCorrelationRequest(
        String index,
        String event,
        Long timestamp,
        Map<String, List<String>> eventsAdjacencyList,
        List<String> tags
    ) {
        super();
        this.index = index;
        this.event = event;
        this.timestamp = timestamp;
        this.eventsAdjacencyList = eventsAdjacencyList;
        this.tags = tags;
    }

    /**
     * StreamInput ctor of StoreCorrelationRequest
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public StoreCorrelationRequest(StreamInput sin) throws IOException {
        this(
            sin.readString(),
            sin.readString(),
            sin.readLong(),
            sin.readMap(StreamInput::readString, StreamInput::readStringList),
            sin.readStringList()
        );
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(event);
        out.writeLong(timestamp);
        out.writeMap(eventsAdjacencyList, StreamOutput::writeString, StreamOutput::writeStringCollection);
        out.writeStringCollection(tags);
    }

    /**
     * get input event which is being correlated
     * @return input event which is being correlated
     */
    public String getEvent() {
        return event;
    }

    /**
     * get index of event which is being correlated
     * @return index of event which is being correlated
     */
    public String getIndex() {
        return index;
    }

    /**
     * get timestamp of correlated event
     * @return timestamp of correlated event
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * get optional tags of correlated event
     * @return optional tags of correlated event
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * get adjacency list of correlated events for the input event
     * @return adjacency list of correlated events for the input event
     */
    public Map<String, List<String>> getEventsAdjacencyList() {
        return eventsAdjacencyList;
    }
}

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
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A request to index correlations
 *
 * @opensearch.api
 */
public class IndexCorrelationRequest extends ActionRequest {

    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField EVENT_FIELD = new ParseField("event");
    private static final ParseField STORE_FIELD = new ParseField("store");
    private static final ObjectParser<IndexCorrelationRequest, Void> PARSER = new ObjectParser<>(
        "IndexCorrelationRequest",
        IndexCorrelationRequest::new
    );

    static {
        PARSER.declareString(IndexCorrelationRequest::setIndex, INDEX_FIELD);
        PARSER.declareString(IndexCorrelationRequest::setEvent, EVENT_FIELD);
        PARSER.declareBoolean(IndexCorrelationRequest::setStore, STORE_FIELD);
    }

    private String index;

    private String event;

    private Boolean store;

    private IndexCorrelationRequest() {}

    /**
     * Parameterized ctor for IndexCorrelationRequest
     * @param index index for correlation
     * @param event event from index which needs to be correlated
     * @param store an optional param which is used to decide whether to store correlations in vectordb or not.
     */
    public IndexCorrelationRequest(String index, String event, Boolean store) {
        super();
        this.index = index;
        this.event = event;
        this.store = store;
    }

    /**
     * StreamInput ctor of IndexCorrelationRequest
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public IndexCorrelationRequest(StreamInput sin) throws IOException {
        this(sin.readString(), sin.readString(), sin.readBoolean());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(event);
        out.writeBoolean(store);
    }

    /**
     * Parse into IndexCorrelationRequest
     * @param xcp XContentParser
     * @return IndexCorrelationRequest
     */
    public static IndexCorrelationRequest parse(XContentParser xcp) {
        return PARSER.apply(xcp, null);
    }

    /**
     * set index for correlation
     * @param index index for correlation
     */
    public void setIndex(String index) {
        this.index = index;
    }

    /**
     * get index for correlation
     * @return index for correlation
     */
    public String getIndex() {
        return index;
    }

    /**
     * set event to be correlated
     * @param event event to be correlated
     */
    public void setEvent(String event) {
        this.event = event;
    }

    /**
     * get event to be correlated
     * @return event to be correlated
     */
    public String getEvent() {
        return event;
    }

    /**
     * set boolean param to check if correlations stored
     * @param store boolean param to check if correlations stored
     */
    public void setStore(Boolean store) {
        this.store = store;
    }

    /**
     * boolean param to check if correlations stored
     * @return boolean param to check if correlations stored
     */
    public Boolean getStore() {
        return store;
    }
}

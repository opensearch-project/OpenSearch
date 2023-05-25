/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import org.opensearch.client.ProtobufOpenSearchClient;
import org.opensearch.common.unit.TimeValue;

import java.util.Objects;

/**
 * Base Action Request Builder
*
* @opensearch.api
*/
public abstract class ProtobufActionRequestBuilder<Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> {

    protected final ProtobufActionType<Response> action;
    protected final Request request;
    protected final ProtobufOpenSearchClient client;

    protected ProtobufActionRequestBuilder(ProtobufOpenSearchClient client, ProtobufActionType<Response> action, Request request) {
        Objects.requireNonNull(action, "action must not be null");
        this.action = action;
        this.request = request;
        this.client = client;
    }

    public Request request() {
        return this.request;
    }

    public ActionFuture<Response> execute() {
        return client.execute(action, request);
    }

    /**
     * Short version of execute().actionGet().
    */
    public Response get() {
        return execute().actionGet();
    }

    /**
     * Short version of execute().actionGet().
    */
    public Response get(TimeValue timeout) {
        return execute().actionGet(timeout);
    }

    /**
     * Short version of execute().actionGet().
    */
    public Response get(String timeout) {
        return execute().actionGet(timeout);
    }

    public void execute(ActionListener<Response> listener) {
        client.execute(action, request, listener);
    }
}

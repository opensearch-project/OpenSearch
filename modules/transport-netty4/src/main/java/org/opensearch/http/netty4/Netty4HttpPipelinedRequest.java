/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpPipelinedRequest;

/**
 * Creates reference-counted pipelined responses before they enter Netty, including writes rejected before pipelining.
 */
final class Netty4HttpPipelinedRequest extends HttpPipelinedRequest {

    private final Netty4HttpRequest request;

    Netty4HttpPipelinedRequest(int sequence, Netty4HttpRequest request) {
        super(sequence, request);
        this.request = request;
    }

    @Override
    public Netty4HttpPipelinedResponse createResponse(RestStatus status, BytesReference content) {
        return new Netty4HttpPipelinedResponse(getSequence(), request.createResponse(status, content));
    }
}

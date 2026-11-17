/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * A handler whose downstream response preparation fails after request bytes have been released, reproducing the
 * boundary from opensearch-project/OpenSearch#22311 without needing a multi-hundred-megabyte response.
 * <p>
 * A response-channel adapter may transform a response before writing it to the wire. If that transformation throws,
 * {@code RestController.ResourceHandlingHttpChannel} has already released the request-byte reservation but no response
 * has been accepted. The failure listener must therefore be able to send the error over the same channel.
 * <p>
 * This handler recreates that sequence through the real HTTP stack. It completes asynchronously through
 * {@link RestResponseListener}, and {@link RestResponse#content()} throws the same structured 413 produced by the
 * oversized UTF-16 guard when {@code DefaultRestChannel} resolves the body. Nothing reaches the client on the first
 * attempt; the client receives only the follow-up failure response.
 */
public class TestResponseFailureRestAction extends BaseRestHandler {

    static final String ROUTE = "/_test/response_serialization_failure";
    static final int MAX_UTF16_LENGTH_FOR_UTF8 = Integer.MAX_VALUE / 3;
    static final int OVERSIZED_UTF16_LENGTH = MAX_UTF16_LENGTH_FOR_UTF8 + 1;
    static final String FAILURE_MESSAGE = "UTF16 string length ["
        + OVERSIZED_UTF16_LENGTH
        + "] exceeds maximum ["
        + MAX_UTF16_LENGTH_FOR_UTF8
        + "] that can be UTF-8 encoded without integer overflow";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, ROUTE));
    }

    @Override
    public String getName() {
        return "test_response_serialization_failure_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        // Complete through a real transport action so the failure happens in a listener callback, after
        // prepareRequest has returned. This matters: RestController's catch-all is no longer on the stack, so the
        // only thing that can respond to the client is RestActionListener#onFailure sending over the same channel.
        return channel -> client.admin()
            .cluster()
            .health(new ClusterHealthRequest().local(true), new RestResponseListener<ClusterHealthResponse>(channel) {
                @Override
                public RestResponse buildResponse(ClusterHealthResponse clusterHealthResponse) {
                    return new UnserializableRestResponse();
                }
            });
    }

    /**
     * A response that fails while its body is being resolved by the downstream channel.
     */
    private static final class UnserializableRestResponse extends RestResponse {

        @Override
        public String contentType() {
            return BytesRestResponse.TEXT_CONTENT_TYPE;
        }

        @Override
        public BytesReference content() {
            final IllegalArgumentException cause = new IllegalArgumentException(FAILURE_MESSAGE);
            throw new OpenSearchStatusException(cause.getMessage(), RestStatus.REQUEST_ENTITY_TOO_LARGE, cause);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }
    }
}

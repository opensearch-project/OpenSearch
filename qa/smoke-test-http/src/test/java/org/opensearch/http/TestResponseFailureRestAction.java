/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

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
 * A handler whose response body cannot be serialized, reproducing the shape of
 * opensearch-project/OpenSearch#22311 without needing an oversized response.
 * <p>
 * In the reported failure a search response was serialized into a string large enough to overflow
 * {@code UnicodeUtil#maxUTF8Length}, and the resulting exception was raised from inside the channel's
 * {@code sendResponse} - after {@code RestController.ResourceHandlingHttpChannel} had already closed the channel and
 * before any bytes were written. The failure listener then tried to send an error response over the same channel,
 * which failed a second time, so the client received no status line at all and hung until its proxy timed out.
 * <p>
 * This handler recreates that sequence exactly: the response is completed asynchronously through
 * {@link RestResponseListener} (the same listener the search path uses), and {@link RestResponse#content()} throws when
 * the channel resolves the body. Resolving the body is the last thing {@code DefaultRestChannel#sendResponse} does
 * before writing to the wire, so nothing reaches the client on the first attempt.
 */
public class TestResponseFailureRestAction extends BaseRestHandler {

    static final String ROUTE = "/_test/response_serialization_failure";
    static final String FAILURE_MESSAGE = "simulated integer overflow while serializing the response body";

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
     * A response that fails while its body is being resolved by the channel.
     */
    private static final class UnserializableRestResponse extends RestResponse {

        @Override
        public String contentType() {
            return BytesRestResponse.TEXT_CONTENT_TYPE;
        }

        @Override
        public BytesReference content() {
            // ArithmeticException mirrors what Lucene's UnicodeUtil#maxUTF8Length throws on overflow.
            throw new ArithmeticException(FAILURE_MESSAGE);
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }
    }
}

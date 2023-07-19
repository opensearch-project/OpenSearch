/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;

import java.util.List;

/**
 * A subclass of {@link BytesRestResponse} which also tracks consumed parameters and content.
 *
 * @opensearch.api
 */
public class ExtensionRestResponse extends BytesRestResponse {

    private final List<String> consumedParams;
    private final boolean contentConsumed;

    /**
     * Creates a new response based on {@link XContentBuilder}.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param builder  The builder for the response.
     */
    public ExtensionRestResponse(RestRequest request, RestStatus status, XContentBuilder builder) {
        super(status, builder);
        this.consumedParams = request.consumedParams();
        this.contentConsumed = request.isContentConsumed();
    }

    /**
     * Creates a new plain text response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param content  A plain text response string.
     */
    public ExtensionRestResponse(RestRequest request, RestStatus status, String content) {
        super(status, content);
        this.consumedParams = request.consumedParams();
        this.contentConsumed = request.isContentConsumed();
    }

    /**
     * Creates a new plain text response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param contentType  The content type of the response string.
     * @param content  A response string.
     */
    public ExtensionRestResponse(RestRequest request, RestStatus status, String contentType, String content) {
        super(status, contentType, content);
        this.consumedParams = request.consumedParams();
        this.contentConsumed = request.isContentConsumed();
    }

    /**
     * Creates a binary response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param contentType  The content type of the response bytes.
     * @param content  Response bytes.
     */
    public ExtensionRestResponse(RestRequest request, RestStatus status, String contentType, byte[] content) {
        super(status, contentType, content);
        this.consumedParams = request.consumedParams();
        this.contentConsumed = request.isContentConsumed();
    }

    /**
     * Creates a binary response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param contentType  The content type of the response bytes.
     * @param content  Response bytes.
     */
    public ExtensionRestResponse(RestRequest request, RestStatus status, String contentType, BytesReference content) {
        super(status, contentType, content);
        this.consumedParams = request.consumedParams();
        this.contentConsumed = request.isContentConsumed();
    }

    /**
     * Gets the list of consumed parameters. These are needed to consume the parameters of the original request.
     *
     * @return the list of consumed params.
     */
    public List<String> getConsumedParams() {
        return consumedParams;
    }

    /**
     * Reports whether content was consumed.
     *
     * @return true if the content was consumed, false otherwise.
     */
    public boolean isContentConsumed() {
        return contentConsumed;
    }
}

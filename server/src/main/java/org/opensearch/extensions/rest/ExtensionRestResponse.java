/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestStatus;
import java.util.List;

/**
 * A subclass of {@link BytesRestResponse} which processes the consumed parameters into a custom header.
 *
 * @opensearch.api
 */
public class ExtensionRestResponse extends BytesRestResponse {

    /**
     * Key passed in {@link BytesRestResponse} headers to identify parameters consumed by the handler. For internal use.
     */
    static final String CONSUMED_PARAMS_KEY = "extension.consumed.parameters";
    /**
     * Key passed in {@link BytesRestResponse} headers to identify content consumed by the handler. For internal use.
     */
    static final String CONSUMED_CONTENT_KEY = "extension.consumed.content";

    /**
     * Creates a new response based on {@link XContentBuilder}.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param builder  The builder for the response.
     */
    public ExtensionRestResponse(ExtensionRestRequest request, RestStatus status, XContentBuilder builder) {
        super(status, builder);
        addConsumedHeaders(request.consumedParams(), request.isContentConsumed());
    }

    /**
     * Creates a new plain text response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param content  A plain text response string.
     */
    public ExtensionRestResponse(ExtensionRestRequest request, RestStatus status, String content) {
        super(status, content);
        addConsumedHeaders(request.consumedParams(), request.isContentConsumed());
    }

    /**
     * Creates a new plain text response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param contentType  The content type of the response string.
     * @param content  A response string.
     */
    public ExtensionRestResponse(ExtensionRestRequest request, RestStatus status, String contentType, String content) {
        super(status, contentType, content);
        addConsumedHeaders(request.consumedParams(), request.isContentConsumed());
    }

    /**
     * Creates a binary response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param contentType  The content type of the response bytes.
     * @param content  Response bytes.
     */
    public ExtensionRestResponse(ExtensionRestRequest request, RestStatus status, String contentType, byte[] content) {
        super(status, contentType, content);
        addConsumedHeaders(request.consumedParams(), request.isContentConsumed());
    }

    /**
     * Creates a binary response.
     *
     * @param request  the REST request being responded to.
     * @param status  The REST status.
     * @param contentType  The content type of the response bytes.
     * @param content  Response bytes.
     */
    public ExtensionRestResponse(ExtensionRestRequest request, RestStatus status, String contentType, BytesReference content) {
        super(status, contentType, content);
        addConsumedHeaders(request.consumedParams(), request.isContentConsumed());
    }

    private void addConsumedHeaders(List<String> consumedParams, boolean contentConusmed) {
        consumedParams.stream().forEach(p -> addHeader(CONSUMED_PARAMS_KEY, p));
        addHeader(CONSUMED_CONTENT_KEY, Boolean.toString(contentConusmed));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http;

import org.opensearch.Build;
import org.opensearch.action.ActionListener;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.opensearch.common.network.CloseableChannel;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.core.rest.RestStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.tasks.Task.X_OPAQUE_ID;

/**
 * The default rest channel for incoming requests. This class implements the basic logic for sending a rest
 * response. It will set necessary headers nad ensure that bytes are released after the response is sent.
 *
 * @opensearch.internal
 */
public class DefaultRestChannel extends AbstractRestChannel implements RestChannel {

    static final String CLOSE = "close";
    static final String CONNECTION = "connection";
    static final String KEEP_ALIVE = "keep-alive";
    static final String CONTENT_TYPE = "content-type";
    static final String CONTENT_LENGTH = "content-length";
    static final String SET_COOKIE = "set-cookie";
    static final String SERVER_VERSION = "X-OpenSearch-Version";
    static final String SERVER_VERSION_VALUE = "OpenSearch/"
        + Build.CURRENT.getQualifiedVersion()
        + " ("
        + Build.CURRENT.getDistribution()
        + ")";

    private final HttpRequest httpRequest;
    private final BigArrays bigArrays;
    private final HttpHandlingSettings settings;
    private final ThreadContext threadContext;
    private final HttpChannel httpChannel;
    private final CorsHandler corsHandler;
    private final Map<String, List<String>> SERVER_VERSION_HEADER = Map.of(SERVER_VERSION, List.of(SERVER_VERSION_VALUE));

    @Nullable
    private final HttpTracer tracerLog;

    DefaultRestChannel(
        HttpChannel httpChannel,
        HttpRequest httpRequest,
        RestRequest request,
        BigArrays bigArrays,
        HttpHandlingSettings settings,
        ThreadContext threadContext,
        CorsHandler corsHandler,
        @Nullable HttpTracer tracerLog
    ) {
        super(request, settings.getDetailedErrorsEnabled());
        this.httpChannel = httpChannel;
        this.httpRequest = httpRequest;
        this.bigArrays = bigArrays;
        this.settings = settings;
        this.threadContext = threadContext;
        this.corsHandler = corsHandler;
        this.tracerLog = tracerLog;
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void sendResponse(RestResponse restResponse) {
        // We're sending a response so we know we won't be needing the request content again and release it
        Releasables.closeWhileHandlingException(httpRequest::release);

        final ArrayList<Releasable> toClose = new ArrayList<>(3);
        if (HttpUtils.shouldCloseConnection(httpRequest)) {
            toClose.add(() -> CloseableChannel.closeChannel(httpChannel));
        }

        boolean success = false;
        String opaque = null;
        String contentLength = null;
        try {
            final BytesReference content = restResponse.content();
            if (content instanceof Releasable) {
                toClose.add((Releasable) content);
            }

            BytesReference finalContent = content;
            try {
                if (request.method() == RestRequest.Method.HEAD) {
                    finalContent = BytesArray.EMPTY;
                }
            } catch (IllegalArgumentException ignored) {
                assert restResponse.status() == RestStatus.METHOD_NOT_ALLOWED
                    : "request HTTP method is unsupported but HTTP status is not METHOD_NOT_ALLOWED(405)";
            }

            final HttpResponse httpResponse = httpRequest.createResponse(restResponse.status(), finalContent);

            corsHandler.setCorsResponseHeaders(httpRequest, httpResponse);

            opaque = request.header(X_OPAQUE_ID);
            if (opaque != null) {
                setHeaderField(httpResponse, X_OPAQUE_ID, opaque);
            }

            // Add all custom headers
            addCustomHeaders(httpResponse, restResponse.getHeaders());
            addCustomHeaders(httpResponse, threadContext.getResponseHeaders());

            addCustomHeaders(httpResponse, SERVER_VERSION_HEADER);

            // If our response doesn't specify a content-type header, set one
            setHeaderField(httpResponse, CONTENT_TYPE, restResponse.contentType(), false);
            // If our response has no content-length, calculate and set one
            contentLength = String.valueOf(restResponse.content().length());
            setHeaderField(httpResponse, CONTENT_LENGTH, contentLength, false);

            addCookies(httpResponse);

            BytesStreamOutput bytesStreamOutput = bytesOutputOrNull();
            if (bytesStreamOutput instanceof ReleasableBytesStreamOutput) {
                toClose.add((Releasable) bytesStreamOutput);
            }

            ActionListener<Void> listener = ActionListener.wrap(() -> Releasables.close(toClose));
            httpChannel.sendResponse(httpResponse, listener);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
            }
            if (tracerLog != null) {
                tracerLog.traceResponse(restResponse, httpChannel, contentLength, opaque, request.getRequestId(), success);
            }
        }
    }

    private void setHeaderField(HttpResponse response, String headerField, String value) {
        setHeaderField(response, headerField, value, true);
    }

    private void setHeaderField(HttpResponse response, String headerField, String value, boolean override) {
        if (override || !response.containsHeader(headerField)) {
            response.addHeader(headerField, value);
        }
    }

    private void addCustomHeaders(HttpResponse response, Map<String, List<String>> customHeaders) {
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    setHeaderField(response, headerEntry.getKey(), headerValue);
                }
            }
        }
    }

    private void addCookies(HttpResponse response) {
        if (settings.isResetCookies()) {
            List<String> cookies = request.getHttpRequest().strictCookies();
            if (cookies.isEmpty() == false) {
                for (String cookie : cookies) {
                    response.addHeader(SET_COOKIE, cookie);
                }
            }
        }
    }
}

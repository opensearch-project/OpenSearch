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

package org.opensearch.internal.httpclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class that exposes static methods to unify the way requests are logged.
 * Includes trace logging to log complete requests and responses in curl format.
 * Useful for debugging, manually sending logged requests via curl and checking their responses.
 * Trace logging is a feature that all the language clients provide.
 */
final class RequestLogger {

    private static final Log tracer = LogFactory.getLog("tracer");

    private RequestLogger() {}

    /**
     * Logs a streaming request that yielded a streaming response
     */
    static void logStreamingResponse(
        Log logger,
        HttpRequest request,
        HttpHost host,
        HttpResponse<Flow.Publisher<List<ByteBuffer>>> httpResponse
    ) {
        logResponse(logger, request, host, httpResponse, List.of());
    }

    /**
     * Logs a request that yielded a response
     */
    static void logResponse(Log logger, HttpRequest request, HttpHost host, HttpResponse<List<ByteBuffer>> httpResponse) {
        logResponse(logger, request, host, httpResponse, httpResponse.body());
    }

    /**
     * Logs a request that failed
     */
    static void logFailedRequest(Log logger, Function<Node, HttpRequest> request, Node node, Exception e) {
        if (logger.isDebugEnabled()) {
            final HttpRequest r = request.apply(node);
            logger.debug("request [" + r.method() + " " + node.getHost() + r.uri() + "] failed", e);
        }
        if (tracer.isTraceEnabled()) {
            String traceRequest;
            try {
                traceRequest = buildTraceRequest(request, node);
            } catch (IOException e1) {
                tracer.trace("error while reading request for trace purposes", e);
                traceRequest = "";
            }
            tracer.trace(traceRequest);
        }
    }

    static String buildWarningMessage(HttpRequest request, HttpHost host, List<String> warnings) {
        StringBuilder message = new StringBuilder("request [").append(request.method())
            .append(" ")
            .append(host)
            .append(request.uri())
            .append("] returned ")
            .append(warnings.size())
            .append(" warnings: ");
        for (int i = 0; i < warnings.size(); i++) {
            if (i > 0) {
                message.append(",");
            }
            message.append("[").append(warnings.get(i)).append("]");
        }
        return message.toString();
    }

    /**
     * Creates curl output for given request
     */
    static String buildTraceRequest(Function<Node, HttpRequest> request, Node node) throws IOException {
        final HttpRequest r = request.apply(node);
        return buildTraceRequest(r, node.getHost());
    }

    static String buildTraceRequest(HttpRequest request, HttpHost host) throws IOException {
        String requestLine = "curl -iX " + request.method() + " '" + request.uri() + "'";
        final String body = BodyUtils.getBodyAsString(request).block();
        if (body != null) {
            requestLine += " -d '";
            requestLine += body + "'";
        }
        return requestLine;
    }

    /**
     * Creates curl output for given response
     */
    static String buildTraceResponse(HttpResponse<?> httpResponse, List<ByteBuffer> body) throws IOException {
        StringBuilder responseLine = new StringBuilder();
        responseLine.append("# ").append(new StatusLine(httpResponse));
        for (Map.Entry<String, List<String>> header : httpResponse.headers().map().entrySet()) {
            responseLine.append("\n# ")
                .append(header.getKey())
                .append(": ")
                .append(header.getValue().stream().collect(Collectors.joining(",")));
        }
        responseLine.append("\n#");

        if (body != null && body.isEmpty() == false) {
            for (ByteBuffer chunk : body) {
                responseLine.append("\n# ").append(StandardCharsets.UTF_8.decode(chunk).toString());
            }
        }

        return responseLine.toString();
    }

    /**
     * Logs a request that yielded a response
     */
    private static void logResponse(Log logger, HttpRequest request, HttpHost host, HttpResponse<?> httpResponse, List<ByteBuffer> body) {
        if (logger.isDebugEnabled()) {
            logger.debug("request [" + request.method() + " " + request.uri() + "] returned [" + new StatusLine(httpResponse) + "]");
        }
        if (logger.isWarnEnabled()) {
            List<String> warnings = httpResponse.headers().allValues("Warning");
            if (warnings != null && warnings.size() > 0) {
                logger.warn(buildWarningMessage(request, host, warnings));
            }
        }
        if (tracer.isTraceEnabled()) {
            String requestLine;
            try {
                requestLine = buildTraceRequest(request, host);
            } catch (IOException e) {
                requestLine = "";
                tracer.trace("error while reading request for trace purposes", e);
            }
            String responseLine;
            try {
                responseLine = buildTraceResponse(httpResponse, body);
            } catch (IOException e) {
                responseLine = "";
                tracer.trace("error while reading response for trace purposes", e);
            }
            tracer.trace(requestLine + '\n' + responseLine);
        }
    }

}

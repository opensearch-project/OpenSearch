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

package org.opensearch.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.BufferedHttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.StatusLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
     * Logs a request that yielded a response
     */
    static void logResponse(Log logger, HttpUriRequest request, HttpHost host, ClassicHttpResponse httpResponse) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "request [" + request.getMethod() + " " + host + getUri(request) + "] returned [" + new StatusLine(httpResponse) + "]"
            );
        }
        if (logger.isWarnEnabled()) {
            Header[] warnings = httpResponse.getHeaders("Warning");
            if (warnings != null && warnings.length > 0) {
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
                responseLine = buildTraceResponse(httpResponse);
            } catch (IOException e) {
                responseLine = "";
                tracer.trace("error while reading response for trace purposes", e);
            }
            tracer.trace(requestLine + '\n' + responseLine);
        }
    }

    /**
     * Logs a request that failed
     */
    static void logFailedRequest(Log logger, HttpUriRequest request, Node node, Exception e) {
        if (logger.isDebugEnabled()) {
            logger.debug("request [" + request.getMethod() + " " + node.getHost() + getUri(request) + "] failed", e);
        }
        if (tracer.isTraceEnabled()) {
            String traceRequest;
            try {
                traceRequest = buildTraceRequest(request, node.getHost());
            } catch (IOException e1) {
                tracer.trace("error while reading request for trace purposes", e);
                traceRequest = "";
            }
            tracer.trace(traceRequest);
        }
    }

    static String buildWarningMessage(HttpUriRequest request, HttpHost host, Header[] warnings) {
        StringBuilder message = new StringBuilder("request [").append(request.getMethod())
            .append(" ")
            .append(host)
            .append(getUri(request))
            .append("] returned ")
            .append(warnings.length)
            .append(" warnings: ");
        for (int i = 0; i < warnings.length; i++) {
            if (i > 0) {
                message.append(",");
            }
            message.append("[").append(warnings[i].getValue()).append("]");
        }
        return message.toString();
    }

    /**
     * Creates curl output for given request
     */
    static String buildTraceRequest(HttpUriRequest request, HttpHost host) throws IOException {
        String requestLine = "curl -iX " + request.getMethod() + " '" + host + getUri(request) + "'";
        if (request.getEntity() != null) {
            requestLine += " -d '";
            HttpEntity entity = request.getEntity();
            if (entity.isRepeatable() == false) {
                entity = new BufferedHttpEntity(request.getEntity());
                request.setEntity(entity);
            }
            try {
                requestLine += EntityUtils.toString(entity, StandardCharsets.UTF_8) + "'";
            } catch (final ParseException ex) {
                throw new IOException(ex);
            }
        }
        return requestLine;
    }

    /**
     * Creates curl output for given response
     */
    static String buildTraceResponse(ClassicHttpResponse httpResponse) throws IOException {
        StringBuilder responseLine = new StringBuilder();
        responseLine.append("# ").append(new StatusLine(httpResponse));
        for (Header header : httpResponse.getHeaders()) {
            responseLine.append("\n# ").append(header.getName()).append(": ").append(header.getValue());
        }
        responseLine.append("\n#");
        HttpEntity entity = httpResponse.getEntity();
        if (entity != null) {
            if (entity.isRepeatable() == false) {
                entity = new BufferedHttpEntity(entity);
            }
            httpResponse.setEntity(entity);
            ContentType contentType = ContentType.parse(entity.getContentType());
            Charset charset = StandardCharsets.UTF_8;
            if (contentType != null && contentType.getCharset() != null) {
                charset = contentType.getCharset();
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), charset))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    responseLine.append("\n# ").append(line);
                }
            }
        }
        return responseLine.toString();
    }

    private static String getUri(HttpUriRequest request) {
        final String uri = request.getRequestUri();
        if (uri == null) {
            return "/";
        } else if (!uri.startsWith("/")) {
            return "/" + uri;
        } else {
            return uri;
        }
    }
}

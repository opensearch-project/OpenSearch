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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test.rest;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.rest.RestRequest;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeRestRequest extends RestRequest {

    public FakeRestRequest() {
        this(
            NamedXContentRegistry.EMPTY,
            new FakeHttpRequest(Method.GET, "", BytesArray.EMPTY, new HashMap<>()),
            new HashMap<>(),
            new FakeHttpChannel(null)
        );
    }

    public FakeRestRequest(Map<String, String> params) {
        this(
            NamedXContentRegistry.EMPTY,
            new FakeHttpRequest(Method.GET, "", BytesArray.EMPTY, new HashMap<>()),
            params,
            new FakeHttpChannel(null)
        );
    }

    private FakeRestRequest(
        NamedXContentRegistry xContentRegistry,
        HttpRequest httpRequest,
        Map<String, String> params,
        HttpChannel httpChannel
    ) {
        super(xContentRegistry, params, httpRequest.uri(), httpRequest.getHeaders(), httpRequest, httpChannel);
    }

    private static class FakeHttpRequest implements HttpRequest {

        private final Method method;
        private final String uri;
        private final BytesReference content;
        private final Map<String, List<String>> headers;
        private final Exception inboundException;

        private FakeHttpRequest(Method method, String uri, BytesReference content, Map<String, List<String>> headers) {
            this(method, uri, content, headers, null);
        }

        private FakeHttpRequest(
            Method method,
            String uri,
            BytesReference content,
            Map<String, List<String>> headers,
            Exception inboundException
        ) {
            this.method = method;
            this.uri = uri;
            this.content = content;
            this.headers = headers;
            this.inboundException = inboundException;
        }

        @Override
        public Method method() {
            return method;
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public BytesReference content() {
            return content;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public List<String> strictCookies() {
            return Collections.emptyList();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            headers.remove(header);
            return this;
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            Map<String, String> headers = new HashMap<>();
            return new HttpResponse() {
                @Override
                public void addHeader(String name, String value) {
                    headers.put(name, value);
                }

                @Override
                public boolean containsHeader(String name) {
                    return headers.containsKey(name);
                }
            };
        }

        @Override
        public void release() {}

        @Override
        public HttpRequest releaseAndCopy() {
            return this;
        }

        @Override
        public Exception getInboundException() {
            return inboundException;
        }
    }

    private static class FakeHttpChannel implements HttpChannel {

        private final InetSocketAddress remoteAddress;

        private FakeHttpChannel(InetSocketAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {

        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return remoteAddress;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {

        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {

        }
    }

    public static class Builder {
        private final NamedXContentRegistry xContentRegistry;

        private Map<String, List<String>> headers = new HashMap<>();

        private Map<String, String> params = new HashMap<>();

        private BytesReference content = BytesArray.EMPTY;

        private String path = "/";

        private Method method = Method.GET;

        private InetSocketAddress address = null;

        private Exception inboundException;

        public Builder(NamedXContentRegistry xContentRegistry) {
            this.xContentRegistry = xContentRegistry;
        }

        public Builder withHeaders(Map<String, List<String>> headers) {
            this.headers = headers;
            return this;
        }

        public Builder withParams(Map<String, String> params) {
            this.params = params;
            return this;
        }

        public Builder withContent(BytesReference content, MediaType mediaType) {
            this.content = content;
            if (mediaType != null) {
                headers.put("Content-Type", Collections.singletonList(mediaType.mediaType()));
            }
            return this;
        }

        public Builder withPath(String path) {
            this.path = path;
            return this;
        }

        public Builder withMethod(Method method) {
            this.method = method;
            return this;
        }

        public Builder withRemoteAddress(InetSocketAddress address) {
            this.address = address;
            return this;
        }

        public Builder withInboundException(Exception exception) {
            this.inboundException = exception;
            return this;
        }

        public FakeRestRequest build() {
            FakeHttpRequest fakeHttpRequest = new FakeHttpRequest(method, path, content, headers, inboundException);
            return new FakeRestRequest(xContentRegistry, fakeHttpRequest, params, new FakeHttpChannel(address));
        }
    }

    public static String requestToString(RestRequest restRequest) {
        return "method=" + restRequest.method() + ",path=" + restRequest.rawPath();
    }
}

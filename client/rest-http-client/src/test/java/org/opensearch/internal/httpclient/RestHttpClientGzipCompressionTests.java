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

package org.opensearch.internal.httpclient;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RestHttpClientGzipCompressionTests extends RestHttpClientTestCase {

    private static HttpServer httpServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/", new GzipResponseHandler());
        httpServer.start();
    }

    @AfterClass
    public static void stopHttpServers() throws IOException {
        httpServer.stop(0);
        httpServer = null;
    }

    /**
     * A response handler that accepts gzip-encoded data and replies request and response encoding values
     * followed by the request body. The response is compressed if "Accept-Encoding" is "gzip".
     */
    private static class GzipResponseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // Decode body (if any)
            String contentEncoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
            InputStream body = exchange.getRequestBody();
            if ("gzip".equals(contentEncoding)) {
                body = new GZIPInputStream(body);
            }
            byte[] bytes = body.readAllBytes();
            boolean compress = "gzip".equals(exchange.getRequestHeaders().getFirst("Accept-Encoding"));
            if (compress) {
                exchange.getResponseHeaders().add("Content-Encoding", "gzip");
            }

            exchange.sendResponseHeaders(200, 0);

            // Encode response if needed
            OutputStream out = exchange.getResponseBody();
            if (compress) {
                out = new GZIPOutputStream(out);
            }

            // Outputs <request-encoding|null>#<response-encoding|null>#<request-body>
            out.write(String.valueOf(contentEncoding).getBytes(StandardCharsets.UTF_8));
            out.write('#');
            out.write((compress ? "gzip" : "null").getBytes(StandardCharsets.UTF_8));
            out.write('#');
            out.write(bytes);
            out.close();

            exchange.close();
        }
    }

    private RestHttpClient createClient(boolean enableCompression) {
        InetSocketAddress address = httpServer.getAddress();
        return RestHttpClient.builder(new HttpHost("http", address.getHostString(), address.getPort()))
            .setCompressionEnabled(enableCompression)
            .build();
    }

    public void testGzipHeaderSync() throws Exception {
        try (RestHttpClient restClient = createClient(false)) {
            // Send non-compressed request, expect compressed response
            Request request = Request.newRequest("POST", "/")
                .withEntity(BodyPublishers.ofString("plain request, gzip response"))
                .withOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Accept-Encoding", "gzip"))
                .build();

            Response response = restClient.performRequest(request);

            String content = BodyUtils.getBodyAsString(response.entity());
            Assert.assertEquals("null#gzip#plain request, gzip response", content);
        }
    }

    public void testGzipHeaderAsync() throws Exception {
        try (RestHttpClient restClient = createClient(false)) {
            // Send non-compressed request, expect compressed response
            Request request = Request.newRequest("POST", "/")
                .withEntity(BodyPublishers.ofString("plain request, gzip response"))
                .withOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Accept-Encoding", "gzip"))
                .build();

            FutureResponse futureResponse = new FutureResponse();
            restClient.performRequestAsync(request, futureResponse);
            Response response = futureResponse.get();

            String content = BodyUtils.getBodyAsString(response.entity());
            Assert.assertEquals("null#gzip#plain request, gzip response", content);
        }
    }

    public void testCompressingClientSync() throws Exception {
        try (RestHttpClient restClient = createClient(true)) {
            Request request = Request.newRequest("POST", "/").withEntity(BodyPublishers.ofString("compressing client")).build();

            Response response = restClient.performRequest(request);

            String content = BodyUtils.getBodyAsString(response.entity());
            Assert.assertEquals("gzip#gzip#compressing client", content);
        }
    }

    public void testCompressingClientAsync() throws Exception {
        InetSocketAddress address = httpServer.getAddress();
        try (
            RestHttpClient restClient = RestHttpClient.builder(new HttpHost("http", address.getHostString(), address.getPort()))
                .setCompressionEnabled(true)
                .build()
        ) {
            Request request = Request.newRequest("POST", "/").withEntity(BodyPublishers.ofString("compressing client")).build();

            FutureResponse futureResponse = new FutureResponse();
            restClient.performRequestAsync(request, futureResponse);
            Response response = futureResponse.get();

            // Server should report it had a compressed request and sent back a compressed response
            String content = BodyUtils.getBodyAsString(response.entity());
            Assert.assertEquals("gzip#gzip#compressing client", content);
        }
    }

    public static class FutureResponse extends CompletableFuture<Response> implements ResponseListener {
        @Override
        public void onSuccess(Response response) {
            this.complete(response);
        }

        @Override
        public void onFailure(Exception exception) {
            this.completeExceptionally(exception);
        }
    }
}

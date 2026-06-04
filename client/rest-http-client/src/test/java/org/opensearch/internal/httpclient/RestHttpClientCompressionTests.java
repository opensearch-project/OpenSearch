/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RestHttpClientCompressionTests extends RestHttpClientTestCase {

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
            String contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
            InputStream body = exchange.getRequestBody();
            boolean compressedRequest = false;
            if ("gzip".equals(contentEncoding)) {
                body = new GZIPInputStream(body);
                compressedRequest = true;
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
            out.write(((compressedRequest == true && contentLength != null) ? contentLength : "null").getBytes(StandardCharsets.UTF_8));
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
            .setHttpClientConfigCallback(builder -> builder.version(Version.HTTP_1_1))
            .build();
    }

    public void testCompressingClientWithContentLengthSync() throws Exception {
        try (RestHttpClient restClient = createClient(true)) {
            Request request = new Request("POST", "/");
            request.setEntity(BodyPublishers.ofString("compressing client"));

            Response response = restClient.performRequest(request);

            String content = BodyUtils.getBodyAsString(response.getEntity());
            // Content-Encoding#Accept-Encoding#Content-Length#Content
            // With HttpClient, we don't sent Content-Length so it is always null
            Assert.assertEquals("gzip#gzip#null#compressing client", content);
        }
    }

    public void testCompressingClientContentLengthAsync() throws Exception {
        try (RestHttpClient restClient = createClient(true)) {
            Request request = new Request("POST", "/");
            request.setEntity(BodyPublishers.ofString("compressing client"));

            FutureResponse futureResponse = new FutureResponse();
            restClient.performRequestAsync(request, futureResponse);
            Response response = futureResponse.get();

            // Server should report it had a compressed request and sent back a compressed response
            String content = BodyUtils.getBodyAsString(response.getEntity());

            // Content-Encoding#Accept-Encoding#Content-Length#Content
            // With HttpClient, we don't sent Content-Length so it is always null
            Assert.assertEquals("gzip#gzip#null#compressing client", content);
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

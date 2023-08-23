/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RestClientCompressionTests extends RestClientTestCase {

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
            byte[] bytes = readAll(body);
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
            out.write((compressedRequest ? contentLength : "null").getBytes(StandardCharsets.UTF_8));
            out.write('#');
            out.write(bytes);
            out.close();

            exchange.close();
        }
    }

    /**
     * Read all bytes of an input stream and close it.
     */
    private static byte[] readAll(InputStream in) throws IOException {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int len = 0;
        while ((len = in.read(buffer)) > 0) {
            bos.write(buffer, 0, len);
        }
        in.close();
        return bos.toByteArray();
    }

    private RestClient createClient(boolean enableCompression, boolean chunkedEnabled) {
        InetSocketAddress address = httpServer.getAddress();
        return RestClient.builder(new HttpHost(address.getHostString(), address.getPort(), "http"))
            .setCompressionEnabled(enableCompression)
            .setChunkedEnabled(chunkedEnabled)
            .build();
    }

    public void testCompressingClientWithContentLengthSync() throws Exception {
        RestClient restClient = createClient(true, false);

        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("compressing client", ContentType.TEXT_PLAIN));

        Response response = restClient.performRequest(request);

        HttpEntity entity = response.getEntity();
        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);
        // Content-Encoding#Accept-Encoding#Content-Length#Content
        Assert.assertEquals("gzip#gzip#38#compressing client", content);

        restClient.close();
    }

    public void testCompressingClientContentLengthAsync() throws Exception {
        InetSocketAddress address = httpServer.getAddress();
        RestClient restClient = createClient(true, false);

        Request request = new Request("POST", "/");
        request.setEntity(new StringEntity("compressing client", ContentType.TEXT_PLAIN));

        FutureResponse futureResponse = new FutureResponse();
        restClient.performRequestAsync(request, futureResponse);
        Response response = futureResponse.get();

        // Server should report it had a compressed request and sent back a compressed response
        HttpEntity entity = response.getEntity();
        String content = new String(readAll(entity.getContent()), StandardCharsets.UTF_8);

        // Content-Encoding#Accept-Encoding#Content-Length#Content
        Assert.assertEquals("gzip#gzip#38#compressing client", content);

        restClient.close();
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

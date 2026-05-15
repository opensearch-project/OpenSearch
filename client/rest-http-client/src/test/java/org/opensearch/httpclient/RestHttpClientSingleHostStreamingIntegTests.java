/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.httpclient;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;

/**
 * Integration test to check interaction between {@link RestHttpClient} and {@link HttpClient}.
 * Works against a real http server, one single host, use streaming.
 */
public class RestHttpClientSingleHostStreamingIntegTests extends RestHttpClientTestCase {

    private HttpServer httpServer;
    private RestHttpClient restClient;
    private String pathPrefix;
    private Map<String, List<String>> defaultHeaders;
    private ExecutorService httpClientExecutor;

    @Before
    public void startHttpServer() throws Exception {
        pathPrefix = randomBoolean() ? "/testPathPrefix/" + randomAsciiLettersOfLengthBetween(1, 5) : "";
        httpServer = createHttpServer();
        httpClientExecutor = Executors.newWorkStealingPool();
        defaultHeaders = RestClientTestUtil.randomHeaders(getRandom(), "Header-default");
        restClient = createRestClient();
    }

    private HttpServer createHttpServer() throws Exception {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        // returns a different status code depending on the path
        for (int statusCode : RestClientTestUtil.getAllStatusCodes()) {
            httpServer.createContext(pathPrefix + "/" + statusCode, new ResponseHandler(statusCode));
        }
        return httpServer;
    }

    private static class ResponseHandler implements HttpHandler {
        private final int statusCode;

        ResponseHandler(int statusCode) {
            this.statusCode = statusCode;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            Headers requestHeaders = httpExchange.getRequestHeaders();
            Headers responseHeaders = httpExchange.getResponseHeaders();
            for (Map.Entry<String, List<String>> header : requestHeaders.entrySet()) {
                responseHeaders.put(header.getKey(), header.getValue());
            }
            try {
                httpExchange.sendResponseHeaders(statusCode, 0);
                httpExchange.getRequestBody().transferTo(httpExchange.getResponseBody());
                httpExchange.getResponseBody().flush();
            } finally {
                httpExchange.getRequestBody().close();
                httpExchange.getResponseBody().close();
                httpExchange.close();
            }
        }
    }

    private RestHttpClient createRestClient() {
        final HttpHost httpHost = new HttpHost("http", httpServer.getAddress().getHostString(), httpServer.getAddress().getPort());
        final RestHttpClientBuilder restClientBuilder = RestHttpClient.builder(httpHost)
            .setDefaultHeaders(defaultHeaders)
            .setCompressionEnabled(randomBoolean());
        if (pathPrefix.length() > 0) {
            restClientBuilder.setPathPrefix(pathPrefix);
        }

        restClientBuilder.setHttpClientConfigCallback(new RestHttpClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpClient.Builder customizeHttpClient(final HttpClient.Builder httpClientBuilder) {
                return httpClientBuilder.executor(httpClientExecutor).connectTimeout(Duration.ofSeconds(15));
            }
        });

        return restClientBuilder.build();
    }

    @After
    public void stopHttpServers() throws IOException, InterruptedException {
        restClient.close();
        restClient = null;
        httpServer.stop(0);
        httpServer = null;

        httpClientExecutor.shutdown();
        if (httpClientExecutor.awaitTermination(30, TimeUnit.SECONDS) == false) {
            httpClientExecutor.shutdownNow();
        }
    }

    /**
     * End to end test for delete with body. We test it explicitly as it is not supported
     * out of the box by {@link HttpClient}.
     * Exercises the test http server ability to send back whatever body it received.
     */
    public void testDeleteWithBody() throws Exception {
        bodyTest("DELETE");
    }

    /**
     * End to end test for get with body. We test it explicitly as it is not supported
     * out of the box by {@link HttpClient}.
     * Exercises the test http server ability to send back whatever body it received.
     */
    public void testGetWithBody() throws Exception {
        bodyTest("GET");
    }

    private StreamingResponse bodyTest(final String method) throws Exception {
        int statusCode = RestClientTestUtil.randomStatusCode(getRandom());
        return bodyTest(restClient, method, statusCode, Map.of());
    }

    private StreamingResponse bodyTest(RestHttpClient restClient, String method, int statusCode, Map<String, List<String>> headers)
        throws Exception {
        String requestBody = "{ \"field\": \"value\" }";
        StreamingRequest request = new StreamingRequest(method, "/" + statusCode, Mono.just(StandardCharsets.UTF_8.encode(requestBody)));
        RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            header.getValue().forEach(v -> options.addHeader(header.getKey(), v));
        }
        request.setOptions(options);
        StreamingResponse esResponse = restClient.streamRequest(request);

        assertEquals(method, esResponse.getRequestLine().getMethod());
        assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
        assertEquals(pathPrefix + "/" + statusCode, esResponse.getRequestLine().getUri());

        if (statusCode >= 200 && statusCode < 400) {
            StepVerifier.create(Flux.from(esResponse.getBody()).map(StandardCharsets.UTF_8::decode).map(CharBuffer::toString))
                .expectNextMatches(s -> s.equals(requestBody))
                .expectComplete()
                .verify(Duration.ofSeconds(5));
        } else {
            StepVerifier.create(Flux.from(esResponse.getBody())).expectError(ResponseException.class).verify(Duration.ofSeconds(5));
        }

        return esResponse;
    }
}

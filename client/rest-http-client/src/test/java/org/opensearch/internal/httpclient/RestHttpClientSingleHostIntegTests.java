/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.http.HttpClient;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Integration test to check interaction between {@link RestHttpClient} and {@link HttpClient}.
 * Works against a real http server, one single host.
 */
public class RestHttpClientSingleHostIntegTests extends RestHttpClientTestCase {

    private HttpServer httpServer;
    private RestHttpClient restClient;
    private String pathPrefix;
    private Map<String, List<String>> defaultHeaders;
    private WaitForCancelHandler waitForCancelHandler;
    private ExecutorService httpClientExecutor;
    private ExecutorService httpServerExecutor;

    @Before
    public void startHttpServer() throws Exception {
        pathPrefix = randomBoolean() ? "/testPathPrefix/" + randomAsciiLettersOfLengthBetween(1, 5) : "";
        httpServer = createHttpServer();
        httpClientExecutor = Executors.newFixedThreadPool(5);
        httpServerExecutor = Executors.newFixedThreadPool(10);
        defaultHeaders = RestClientTestUtil.randomHeaders(getRandom(), "Header-default");
        restClient = createRestClient(false, true);
    }

    private HttpServer createHttpServer() throws Exception {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.setExecutor(httpServerExecutor);
        httpServer.start();
        // returns a different status code depending on the path
        for (int statusCode : RestClientTestUtil.getAllStatusCodes()) {
            httpServer.createContext(pathPrefix + "/" + statusCode, new ResponseHandler(statusCode));
        }
        waitForCancelHandler = new WaitForCancelHandler();
        httpServer.createContext(pathPrefix + "/wait", waitForCancelHandler);
        return httpServer;
    }

    private static class WaitForCancelHandler implements HttpHandler {

        private final CountDownLatch cancelHandlerLatch = new CountDownLatch(1);

        void cancelDone() {
            cancelHandlerLatch.countDown();
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                cancelHandlerLatch.await();
            } catch (InterruptedException ignore) {} finally {
                exchange.sendResponseHeaders(200, 0);
                exchange.close();
            }
        }
    }

    private static class ResponseHandler implements HttpHandler {
        private final int statusCode;

        ResponseHandler(int statusCode) {
            this.statusCode = statusCode;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            // copy request body to response body so we can verify it was sent
            StringBuilder body = new StringBuilder();
            try (InputStreamReader reader = new InputStreamReader(httpExchange.getRequestBody(), StandardCharsets.UTF_8)) {
                char[] buffer = new char[256];
                int read;
                while ((read = reader.read(buffer)) != -1) {
                    body.append(buffer, 0, read);
                }
            }
            // copy request headers to response headers so we can verify they were sent
            Headers requestHeaders = httpExchange.getRequestHeaders();
            Headers responseHeaders = httpExchange.getResponseHeaders();
            for (Map.Entry<String, List<String>> header : requestHeaders.entrySet()) {
                responseHeaders.put(header.getKey(), header.getValue());
            }
            httpExchange.getRequestBody().close();
            httpExchange.sendResponseHeaders(statusCode, body.length() == 0 ? -1 : body.length());
            if (body.length() > 0) {
                try (OutputStream out = httpExchange.getResponseBody()) {
                    out.write(body.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            httpExchange.close();
        }
    }

    private RestHttpClient createRestClient(final boolean useAuth, final boolean usePreemptiveAuth) {
        final HttpHost httpHost = new HttpHost("http", httpServer.getAddress().getHostString(), httpServer.getAddress().getPort());
        final RestHttpClientBuilder restClientBuilder = RestHttpClient.builder(httpHost).setDefaultHeaders(defaultHeaders);
        if (pathPrefix.length() > 0) {
            restClientBuilder.setPathPrefix(pathPrefix);
        }

        if (useAuth) {
            if (usePreemptiveAuth == false) {
                restClientBuilder.setHttpClientConfigCallback(new RestHttpClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpClient.Builder customizeHttpClient(final HttpClient.Builder httpClientBuilder) {
                        return httpClientBuilder.executor(httpClientExecutor).authenticator(new Authenticator() {
                            @Override
                            protected PasswordAuthentication getPasswordAuthentication() {
                                return new PasswordAuthentication("user", "pass".toCharArray());
                            }
                        });
                    }
                });
            } else {
                final String auth = Base64.getEncoder().encodeToString(("user" + ":" + "pass").getBytes(StandardCharsets.UTF_8));
                final Map<String, List<String>> headers = new HashMap<>(defaultHeaders);
                headers.put("Authorization", List.of("Basic " + auth));
                restClientBuilder.setDefaultHeaders(headers);

                restClientBuilder.setHttpClientConfigCallback(new RestHttpClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpClient.Builder customizeHttpClient(final HttpClient.Builder httpClientBuilder) {
                        return httpClientBuilder.executor(httpClientExecutor);
                    }
                });
            }
        } else {
            restClientBuilder.setHttpClientConfigCallback(new RestHttpClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpClient.Builder customizeHttpClient(final HttpClient.Builder httpClientBuilder) {
                    return httpClientBuilder.executor(httpClientExecutor).connectTimeout(Duration.ofSeconds(15));
                }
            });
        }

        return restClientBuilder.build();
    }

    @After
    public void stopHttpServers() throws IOException, InterruptedException {
        restClient.close();
        restClient = null;
        httpServer.stop(0);
        httpServer = null;

        httpServerExecutor.shutdown();
        if (httpServerExecutor.awaitTermination(30, TimeUnit.SECONDS) == false) {
            httpServerExecutor.shutdownNow();
        }

        httpClientExecutor.shutdown();
        if (httpClientExecutor.awaitTermination(30, TimeUnit.SECONDS) == false) {
            httpClientExecutor.shutdownNow();
        }
    }

    /**
     * Tests sending a bunch of async requests works well (e.g. no TimeoutException from the leased pool)
     * See https://github.com/elastic/elasticsearch/issues/24069
     */
    public void testManyAsyncRequests() throws Exception {
        int iters = randomIntBetween(500, 1000);
        final CountDownLatch latch = new CountDownLatch(iters);
        final List<Exception> exceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < iters; i++) {
            Request request = new Request("PUT", "/200");
            request.setEntity(BodyPublishers.ofString("{}"));
            // Add random jitter so HttpServer will not refuse the connections
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(randomLongBetween(1, 5)));
            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception exception) {
                    exceptions.add(exception);
                    latch.countDown();
                }
            });
        }

        assertTrue("timeout waiting for requests to be sent", latch.await(10, TimeUnit.SECONDS));
        if (exceptions.isEmpty() == false) {
            AssertionError error = new AssertionError(
                "expected no failures but got some. see suppressed for first 10 of [" + exceptions.size() + "] failures"
            );
            for (Exception exception : exceptions.subList(0, Math.min(10, exceptions.size()))) {
                error.addSuppressed(exception);
            }
            throw error;
        }
    }

    public void testCancelAsyncRequest() throws Exception {
        Request request = new Request(RestClientTestUtil.randomHttpMethod(getRandom()), "/wait");
        CountDownLatch requestLatch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        Cancellable cancellable = restClient.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                throw new AssertionError("onResponse called unexpectedly");
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                requestLatch.countDown();
            }
        });
        cancellable.cancel();
        waitForCancelHandler.cancelDone();
        assertTrue(requestLatch.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    /**
     * End to end test for headers. We test it explicitly against a real http client as there are different ways
     * to set/add headers to the {@link HttpClient}.
     * Exercises the test http server ability to send back whatever headers it received.
     */
    public void testHeaders() throws Exception {
        for (String method : RestClientTestUtil.getHttpMethods()) {
            final Set<String> standardHeaders = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            standardHeaders.addAll(
                Arrays.asList("Connection", "Host", "User-agent", "Date", "Upgrade", "HTTP2-Settings", "Content-Length")
            );

            final Map<String, List<String>> requestHeaders = RestClientTestUtil.randomHeaders(getRandom(), "Header");
            final int statusCode = RestClientTestUtil.randomStatusCode(getRandom());
            Request request = new Request(method, "/" + statusCode);
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeaders(requestHeaders);
            request.setOptions(options);
            Response esResponse;
            try {
                esResponse = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            } catch (ResponseException e) {
                esResponse = e.getResponse();
            }

            assertEquals(method, esResponse.getRequestLine().getMethod());
            assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
            assertEquals(pathPrefix + "/" + statusCode, esResponse.getRequestLine().getUri());

            assertHeaders(defaultHeaders, requestHeaders, esResponse.getHeaders(), standardHeaders);
            final Set<String> removedHeaders = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (final Map.Entry<String, List<String>> responseHeader : esResponse.getHeaders().map().entrySet()) {
                String name = responseHeader.getKey().toLowerCase(Locale.ROOT);
                // Some headers could be returned multiple times in response, like Connection fe.
                if (name.startsWith("header") == false && removedHeaders.contains(name) == false) {
                    assertTrue("unknown header was returned " + name, standardHeaders.remove(name));
                    removedHeaders.add(name);
                }
            }
            assertTrue("some expected standard headers weren't returned: " + standardHeaders, standardHeaders.isEmpty());
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

    public void testEncodeParams() throws Exception {
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "this/is/the/routing");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=this%2Fis%2Fthe%2Frouting", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "this|is|the|routing");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=this%7Cis%7Cthe%7Crouting", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "routing#1");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=routing%231", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "中文");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=%E4%B8%AD%E6%96%87", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "foo bar");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=foo+bar", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "foo+bar");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=foo%2Bbar", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "foo/bar");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=foo%2Fbar", response.getRequestLine().getUri());
        }
        {
            Request request = new Request("PUT", "/200");
            request.addParameter("routing", "foo^bar");
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(pathPrefix + "/200?routing=foo%5Ebar", response.getRequestLine().getUri());
        }
    }

    /**
     * Verify that credentials are sent on the first request with preemptive auth enabled (default when provided with credentials).
     */
    public void testPreemptiveAuthEnabled() throws Exception {
        final String[] methods = { "POST", "PUT", "GET", "DELETE" };

        try (RestHttpClient restClient = createRestClient(true, true)) {
            for (final String method : methods) {
                final Response response = bodyTest(restClient, method);
                assertThat(response.getHeader("Authorization"), startsWith("Basic"));
            }
        }
    }

    /**
     * Verify that credentials are <em>not</em> sent on the first request with preemptive auth disabled.
     */
    public void testPreemptiveAuthDisabled() throws Exception {
        final String[] methods = { "POST", "PUT", "GET", "DELETE" };

        try (RestHttpClient restClient = createRestClient(true, false)) {
            for (final String method : methods) {
                int statusCode = RestClientTestUtil.randomStatusCode(getRandom());
                if (statusCode == 401) {
                    final IOException ex = assertThrows(IOException.class, () -> bodyTest(restClient, method, statusCode, Map.of()));
                    assertThat(ex.getMessage(), equalTo("WWW-Authenticate header missing for response code 401"));
                } else {
                    final Response response = bodyTest(restClient, method, statusCode, Map.of());
                    assertThat(response.getHeader("Authorization"), nullValue());
                }
            }
        }
    }

    /**
     * Verify that credentials continue to be sent even if a 401 (Unauthorized) response is received
     */
    public void testAuthCredentialsAreNotClearedOnAuthChallenge() throws Exception {
        final String[] methods = { "POST", "PUT", "GET", "DELETE" };

        try (RestHttpClient restClient = createRestClient(true, true)) {
            for (final String method : methods) {
                Map<String, List<String>> realmHeader = Map.of("WWW-Authenticate", List.of("Basic realm=\"test\""));
                final Response response401 = bodyTest(restClient, method, 401, realmHeader);
                assertThat(response401.getHeader("Authorization"), startsWith("Basic"));

                final Response response200 = bodyTest(restClient, method, 200, Map.of());
                assertThat(response200.getHeader("Authorization"), startsWith("Basic"));
            }
        }
    }

    public void testUrlWithoutLeadingSlash() throws Exception {
        if (pathPrefix.length() == 0) {
            Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, new Request("GET", "200"));
            // a trailing slash gets automatically added even if a pathPrefix is not configured (HttpClient uses full URI)
            assertEquals(200, response.getStatusLine().getStatusCode());
        } else {
            {
                Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, new Request("GET", "200"));
                // a trailing slash gets automatically added if a pathPrefix is configured
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
            {
                // pathPrefix is not required to start with '/', will be added automatically
                try (
                    RestHttpClient restClient = RestHttpClient.builder(
                        new HttpHost("http", httpServer.getAddress().getHostString(), httpServer.getAddress().getPort())
                    ).setPathPrefix(pathPrefix.substring(1)).build()
                ) {
                    Response response = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, new Request("GET", "200"));
                    // a trailing slash gets automatically added if a pathPrefix is configured
                    assertEquals(200, response.getStatusLine().getStatusCode());
                }
            }
        }
    }

    private Response bodyTest(final String method) throws Exception {
        return bodyTest(restClient, method);
    }

    private Response bodyTest(final RestHttpClient restClient, final String method) throws Exception {
        int statusCode = RestClientTestUtil.randomStatusCode(getRandom());
        return bodyTest(restClient, method, statusCode, Map.of());
    }

    private Response bodyTest(RestHttpClient restClient, String method, int statusCode, Map<String, List<String>> headers)
        throws Exception {
        String requestBody = "{ \"field\": \"value\" }";
        Request request = new Request(method, "/" + statusCode);
        request.setJsonEntity(requestBody);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            header.getValue().forEach(v -> options.addHeader(header.getKey(), v));
        }
        request.setOptions(options);
        Response esResponse;
        try {
            esResponse = RestHttpClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
        } catch (ResponseException e) {
            esResponse = e.getResponse();
        }
        assertEquals(method, esResponse.getRequestLine().getMethod());
        assertEquals(statusCode, esResponse.getStatusLine().getStatusCode());
        assertEquals(pathPrefix + "/" + statusCode, esResponse.getRequestLine().getUri());
        assertEquals(requestBody, BodyUtils.getBodyAsString(esResponse));

        return esResponse;
    }
}

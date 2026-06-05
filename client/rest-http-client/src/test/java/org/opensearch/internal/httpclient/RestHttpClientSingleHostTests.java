/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.PushPromiseHandler;
import java.net.http.HttpTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.opensearch.internal.httpclient.RestClientTestUtil.getAllErrorStatusCodes;
import static org.opensearch.internal.httpclient.RestClientTestUtil.getHttpMethods;
import static org.opensearch.internal.httpclient.RestClientTestUtil.getOkStatusCodes;
import static org.opensearch.internal.httpclient.RestClientTestUtil.randomStatusCode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for basic functionality of {@link RestHttpClient} against one single host: tests http requests being sent, headers,
 * body, different status codes and corresponding responses/exceptions.
 * Relies on a mock http client to intercept requests and return desired responses based on request path.
 */
public class RestHttpClientSingleHostTests extends RestHttpClientTestCase {
    private static final Log logger = LogFactory.getLog(RestHttpClientSingleHostTests.class);

    private ExecutorService exec = Executors.newFixedThreadPool(1);
    private RestHttpClient restClient;
    private Map<String, List<String>> defaultHeaders;
    private Node node;
    private HttpClient httpClient;
    private HostsTrackingFailureListener failureListener;
    private boolean strictDeprecationMode;
    private LongAdder requests;
    private AtomicReference<HttpRequest> requestProducerCapture;

    @Before
    public void createRestClient() {
        requests = new LongAdder();
        requestProducerCapture = new AtomicReference<>();
        httpClient = mockHttpClient(exec, (requestProducer, bodyHandler) -> {
            requests.increment();
            requestProducerCapture.set(requestProducer);
        });
        defaultHeaders = RestClientTestUtil.randomHeaders(getRandom(), "Header-default");
        node = new Node(new HttpHost("http", "localhost", 9200));
        failureListener = new HostsTrackingFailureListener();
        strictDeprecationMode = randomBoolean();
        restClient = new RestHttpClient(
            this.httpClient,
            defaultHeaders,
            singletonList(node),
            null,
            failureListener,
            NodeSelector.ANY,
            strictDeprecationMode,
            false
        );
    }

    interface HttpClientListener {
        void onExecute(HttpRequest requestProducer, BodyHandler<?> bodyHandler);
    }

    @SuppressWarnings("unchecked")
    static HttpClient mockHttpClient(final ExecutorService exec, final HttpClientListener... listeners) {
        HttpClient httpClient = new HttpClient() {
            @Override
            public Optional<Authenticator> authenticator() {
                return Optional.empty();
            }

            @Override
            public Optional<Duration> connectTimeout() {
                return Optional.empty();
            }

            @Override
            public Optional<CookieHandler> cookieHandler() {
                return Optional.empty();
            }

            @Override
            public Optional<Executor> executor() {
                return Optional.of(exec);
            }

            @Override
            public Redirect followRedirects() {
                return null;
            }

            @Override
            public Optional<ProxySelector> proxy() {
                return Optional.empty();
            }

            @Override
            public SSLContext sslContext() {
                return null;
            }

            @Override
            public SSLParameters sslParameters() {
                return null;
            }

            @Override
            public Version version() {
                return Version.HTTP_1_1;
            }

            @Override
            public boolean awaitTermination(Duration duration) throws InterruptedException {
                return exec.awaitTermination(duration.toMillis(), TimeUnit.MILLISECONDS);
            }

            @Override
            public <T> HttpResponse<T> send(java.net.http.HttpRequest request, BodyHandler<T> responseBodyHandler) throws IOException,
                InterruptedException {
                return sendAsync(request, responseBodyHandler).join();
            }

            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                java.net.http.HttpRequest request,
                BodyHandler<T> responseBodyHandler,
                PushPromiseHandler<T> pushPromiseHandler
            ) {
                return sendAsync(request, responseBodyHandler);
            }

            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, BodyHandler<T> responseBodyHandler) {
                Arrays.stream(listeners).forEach(l -> l.onExecute(request, responseBodyHandler));

                final CompletableFuture<HttpResponse<T>> callback = new CompletableFuture<>();
                exec.submit(() -> {
                    try {
                        HttpResponse<?> httpResponse = responseOrException(request);
                        callback.complete((HttpResponse<T>) httpResponse);
                        return (T) httpResponse;
                    } catch (Exception e) {
                        callback.completeExceptionally(e);
                        return null;
                    }
                });

                return callback;
            }
        };

        return httpClient;
    }

    private static HttpResponse<?> responseOrException(HttpRequest request) throws Exception {
        final HttpHost httpHost = new HttpHost(request.uri().getScheme(), request.uri().getHost(), request.uri().getPort());
        // return the desired status code or exception depending on the path
        switch (request.uri().getPath()) {
            case "/soe":
                throw new SocketTimeoutException(httpHost.toString());
            case "/coe":
                throw new HttpTimeoutException(httpHost.toString());
            case "/ioe":
                throw new IOException(httpHost.toString());
            case "/closed":
                throw new ClosedChannelException();
            case "/handshake":
                throw new SSLHandshakeException("");
            case "/uri":
                throw new URISyntaxException("", "");
            case "/runtime":
                throw new RuntimeException();
            default:
                int statusCode = Integer.parseInt(request.uri().getPath().substring(1));

                // return the same body that was sent
                final Object entity = BodyUtils.getBody(request).map(List::of).cache().block();
                final HttpResponse<?> httpResponse = new HttpResponse<>() {
                    @Override
                    public int statusCode() {
                        return statusCode;
                    }

                    @Override
                    public HttpRequest request() {
                        return request;
                    }

                    @Override
                    public Optional<HttpResponse<Object>> previousResponse() {
                        return Optional.empty();
                    }

                    @Override
                    public HttpHeaders headers() {
                        return request.headers();
                    }

                    @Override
                    public Object body() {
                        return entity;
                    }

                    @Override
                    public Optional<SSLSession> sslSession() {
                        return Optional.empty();
                    }

                    @Override
                    public URI uri() {
                        return request.uri();
                    }

                    @Override
                    public Version version() {
                        return request.version().orElse(Version.HTTP_1_1);
                    }
                };

                return httpResponse;
        }
    }

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        exec.shutdown();
    }

    /**
     * Verifies the content of the {@link HttpRequest} that's internally created and passed through to the http client
     */
    @SuppressWarnings("unchecked")
    public void testInternalHttpRequest() throws Exception {
        int times = 0;
        for (String httpMethod : getHttpMethods()) {
            HttpRequest expectedRequest = performRandomRequest(httpMethod);
            assertThat(requests.intValue(), equalTo(++times));

            HttpRequest actualRequest = requestProducerCapture.get();
            assertEquals(expectedRequest.uri(), actualRequest.uri());
            assertEquals(expectedRequest.method(), actualRequest.method());
            assertEquals(expectedRequest.headers(), actualRequest.headers());

            Object expectedEntity = BodyUtils.getBody(expectedRequest).block();
            if (expectedEntity != null) {
                Object actualEntity = BodyUtils.getBody(actualRequest).block();
                assertEquals(expectedEntity, actualEntity);
            }
        }
    }

    /**
     * End to end test for ok status codes
     */
    public void testOkStatusCodes() throws Exception {
        for (String method : getHttpMethods()) {
            for (int okStatusCode : getOkStatusCodes()) {
                Response response = performRequestSyncOrAsync(restClient, Request.newRequest(method, "/" + okStatusCode).build());
                assertThat(response.getStatusLine().statusCode(), equalTo(okStatusCode));
            }
        }
        failureListener.assertNotCalled();
    }

    /**
     * End to end test for error status codes: they should cause an exception to be thrown, apart from 404 with HEAD requests
     */
    public void testErrorStatusCodes() throws Exception {
        for (String method : getHttpMethods()) {
            Set<Integer> expectedIgnores = new HashSet<>();
            String ignoreParam = "";
            if ("HEAD".equalsIgnoreCase(method)) {
                expectedIgnores.add(404);
            }
            if (randomBoolean()) {
                int numIgnores = randomIntBetween(1, 3);
                for (int i = 0; i < numIgnores; i++) {
                    Integer code = randomFrom(getAllErrorStatusCodes());
                    expectedIgnores.add(code);
                    ignoreParam += code;
                    if (i < numIgnores - 1) {
                        ignoreParam += ",";
                    }
                }
            }
            // error status codes should cause an exception to be thrown
            for (int errorStatusCode : getAllErrorStatusCodes()) {
                try {
                    Request.Builder builder = Request.newRequest(method, "/" + errorStatusCode);
                    if (false == ignoreParam.isEmpty()) {
                        builder = builder.withParameter("ignore", ignoreParam);
                    }
                    Response response = restClient.performRequest(builder.build());
                    if (expectedIgnores.contains(errorStatusCode)) {
                        // no exception gets thrown although we got an error status code, as it was configured to be ignored
                        assertEquals(errorStatusCode, response.getStatusLine().statusCode());
                    } else {
                        fail("request should have failed");
                    }
                } catch (ResponseException e) {
                    if (expectedIgnores.contains(errorStatusCode)) {
                        throw e;
                    }
                    assertEquals(errorStatusCode, e.getResponse().getStatusLine().statusCode());
                    assertExceptionStackContainsCallingMethod(e);
                }
                if (errorStatusCode <= 500 || expectedIgnores.contains(errorStatusCode)) {
                    failureListener.assertNotCalled();
                } else {
                    failureListener.assertCalled(singletonList(node));
                }
            }
        }
    }

    public void testPerformRequestIOExceptions() throws Exception {
        for (String method : getHttpMethods()) {
            // IOExceptions should be let bubble up
            try {
                restClient.performRequest(Request.newRequest(method, "/ioe").build());
                fail("request should have failed");
            } catch (IOException e) {
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
            try {
                restClient.performRequest(Request.newRequest(method, "/coe").build());
                fail("request should have failed");
            } catch (HttpTimeoutException e) {
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
            try {
                restClient.performRequest(Request.newRequest(method, "/soe").build());
                fail("request should have failed");
            } catch (SocketTimeoutException e) {
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
            try {
                restClient.performRequest(Request.newRequest(method, "/closed").build());
                fail("request should have failed");
            } catch (ClosedChannelException e) {
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
            try {
                restClient.performRequest(Request.newRequest(method, "/handshake").build());
                fail("request should have failed");
            } catch (SSLHandshakeException e) {
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
        }
    }

    public void testPerformRequestRuntimeExceptions() throws Exception {
        for (String method : getHttpMethods()) {
            try {
                restClient.performRequest(Request.newRequest(method, "/runtime").build());
                fail("request should have failed");
            } catch (RuntimeException e) {
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
        }
    }

    public void testPerformRequestExceptions() throws Exception {
        for (String method : getHttpMethods()) {
            try {
                restClient.performRequest(Request.newRequest(method, "/uri").build());
                fail("request should have failed");
            } catch (RuntimeException e) {
                assertThat(e.getCause(), instanceOf(URISyntaxException.class));
                // And we do all that so the thrown exception has our method in the stacktrace
                assertExceptionStackContainsCallingMethod(e);
            }
            failureListener.assertCalled(singletonList(node));
        }
    }

    /**
     * End to end test for request and response body. Exercises the mock http client ability to send back
     * whatever body it has received.
     */
    public void testBody() throws Exception {
        String body = "{ \"field\": \"value\" }";
        BodyPublisher entity = BodyPublishers.ofString(body);
        for (String method : Arrays.asList("DELETE", "GET", "PATCH", "POST", "PUT", "TRACE")) {
            for (int okStatusCode : getOkStatusCodes()) {
                Request request = Request.newRequest(method, "/" + okStatusCode).withEntity(entity).build();
                Response response = restClient.performRequest(request);
                assertThat(response.getStatusLine().statusCode(), equalTo(okStatusCode));
                assertThat(BodyUtils.getBodyAsString(response), equalTo(body));
            }
            for (int errorStatusCode : getAllErrorStatusCodes()) {
                Request request = Request.newRequest(method, "/" + errorStatusCode).withEntity(entity).build();
                try {
                    restClient.performRequest(request);
                    fail("request should have failed");
                } catch (ResponseException e) {
                    Response response = e.getResponse();
                    assertThat(response.getStatusLine().statusCode(), equalTo(errorStatusCode));
                    assertThat(BodyUtils.getBodyAsString(response), equalTo(body));
                    assertExceptionStackContainsCallingMethod(e);
                }
            }
        }
    }

    /**
     * End to end test for request and response headers. Exercises the mock http client ability to send back
     * whatever headers it has received.
     */
    public void testHeaders() throws Exception {
        for (String method : getHttpMethods()) {
            final Map<String, List<String>> requestHeaders = RestClientTestUtil.randomHeaders(getRandom(), "Header");
            final int statusCode = randomStatusCode(getRandom());
            Request.Builder builder = Request.newRequest(method, "/" + statusCode);
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            for (Map.Entry<String, List<String>> requestHeader : requestHeaders.entrySet()) {
                requestHeader.getValue().forEach(v -> options.addHeader(requestHeader.getKey(), v));
            }
            builder = builder.withOptions(options);
            Response esResponse;
            try {
                esResponse = performRequestSyncOrAsync(restClient, builder.build());
            } catch (ResponseException e) {
                esResponse = e.getResponse();
            }
            assertThat(esResponse.getStatusLine().statusCode(), equalTo(statusCode));
            assertHeaders(defaultHeaders, requestHeaders, esResponse.getHeaders(), Collections.<String>emptySet());
            assertFalse(esResponse.hasWarnings());
        }
    }

    public void testDeprecationWarnings() throws Exception {
        String chars = randomAsciiAlphanumOfLength(5);
        assertDeprecationWarnings(singletonList("poorly formatted " + chars), singletonList("poorly formatted " + chars));
        assertDeprecationWarnings(singletonList(formatWarningWithoutDate(chars)), singletonList(chars));
        assertDeprecationWarnings(singletonList(formatWarning(chars)), singletonList(chars));
        assertDeprecationWarnings(
            Arrays.asList(formatWarning(chars), "another one", "and another"),
            Arrays.asList(chars, "another one", "and another")
        );
        assertDeprecationWarnings(Arrays.asList("ignorable one", "and another"), Arrays.asList("ignorable one", "and another"));
        assertDeprecationWarnings(singletonList("exact"), singletonList("exact"));
        assertDeprecationWarnings(Collections.<String>emptyList(), Collections.<String>emptyList());

        String proxyWarning = "112 - \"network down\" \"Sat, 25 Aug 2012 23:34:45 GMT\"";
        assertDeprecationWarnings(singletonList(proxyWarning), singletonList(proxyWarning));
    }

    private enum DeprecationWarningOption {
        PERMISSIVE {
            protected WarningsHandler warningsHandler() {
                return WarningsHandler.PERMISSIVE;
            }
        },
        STRICT {
            protected WarningsHandler warningsHandler() {
                return WarningsHandler.STRICT;
            }
        },
        FILTERED {
            protected WarningsHandler warningsHandler() {
                return new WarningsHandler() {
                    @Override
                    public boolean warningsShouldFailRequest(List<String> warnings) {
                        for (String warning : warnings) {
                            if (false == warning.startsWith("ignorable")) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
            }
        },
        EXACT {
            protected WarningsHandler warningsHandler() {
                return new WarningsHandler() {
                    @Override
                    public boolean warningsShouldFailRequest(List<String> warnings) {
                        return false == warnings.equals(Arrays.asList("exact"));
                    }
                };
            }
        };

        protected abstract WarningsHandler warningsHandler();
    }

    private void assertDeprecationWarnings(List<String> warningHeaderTexts, List<String> warningBodyTexts) throws Exception {
        String method = randomFrom(getHttpMethods());
        Request.Builder builder = Request.newRequest(method, "/200");
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        for (String warningHeaderText : warningHeaderTexts) {
            options.addHeader("Warning", warningHeaderText);
        }

        final boolean expectFailure;
        if (randomBoolean()) {
            logger.info("checking strictWarningsMode=[" + strictDeprecationMode + "] and warnings=" + warningBodyTexts);
            expectFailure = strictDeprecationMode && false == warningBodyTexts.isEmpty();
        } else {
            DeprecationWarningOption warningOption = randomFrom(DeprecationWarningOption.values());
            logger.info("checking warningOption=" + warningOption + " and warnings=" + warningBodyTexts);
            options.setWarningsHandler(warningOption.warningsHandler());
            expectFailure = warningOption.warningsHandler().warningsShouldFailRequest(warningBodyTexts);
        }
        builder = builder.withOptions(options);

        Response response;
        if (expectFailure) {
            try {
                performRequestSyncOrAsync(restClient, builder.build());
                fail("expected WarningFailureException from warnings");
                return;
            } catch (WarningFailureException e) {
                if (false == warningBodyTexts.isEmpty()) {
                    assertThat(e.getMessage(), containsString("\nWarnings: " + warningBodyTexts));
                }
                response = e.getResponse();
            }
        } else {
            response = performRequestSyncOrAsync(restClient, builder.build());
        }
        assertEquals(false == warningBodyTexts.isEmpty(), response.hasWarnings());
        assertEquals(warningBodyTexts, response.getWarnings());
    }

    /**
     * Emulates OpenSearch's HeaderWarningLogger.formatWarning in simple
     * cases. We don't have that available because we're testing against 1.7.
     */
    private static String formatWarningWithoutDate(String warningBody) {
        final String hash = new String(new byte[40], StandardCharsets.UTF_8).replace('\0', 'e');
        return "299 OpenSearch-1.2.2-SNAPSHOT-" + hash + " \"" + warningBody + "\"";
    }

    private static String formatWarning(String warningBody) {
        return formatWarningWithoutDate(warningBody) + " \"Mon, 01 Jan 2001 00:00:00 GMT\"";
    }

    private HttpRequest performRandomRequest(String method) throws Exception {
        String uriAsString = "/" + randomStatusCode(getRandom());
        Request.Builder builder = Request.newRequest(method, uriAsString);

        Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            int numParams = randomIntBetween(1, 3);
            for (int i = 0; i < numParams; i++) {
                String name = "param-" + i;
                String value = randomAsciiAlphanumOfLengthBetween(3, 10);
                builder = builder.withParameter(name, value);
                params.put(name, value);
            }
        }
        if (randomBoolean()) {
            // randomly add some ignore parameter, which doesn't get sent as part of the request
            String ignore = Integer.toString(randomFrom(RestClientTestUtil.getAllErrorStatusCodes()));
            if (randomBoolean()) {
                ignore += "," + randomFrom(RestClientTestUtil.getAllErrorStatusCodes());
            }
            builder = builder.withParameter("ignore", ignore);
        }

        final String additionalQuery = params.entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));

        URI uri = new URI("http", null, "localhost", 9200, uriAsString, additionalQuery, null);
        BodyPublisher bodyPublisher = BodyPublishers.noBody();
        if (getRandom().nextBoolean()) {
            bodyPublisher = BodyPublishers.ofString(randomAsciiAlphanumOfLengthBetween(10, 100));
            builder = builder.withEntity(bodyPublisher);
        }

        HttpRequest.Builder expectedRequest = HttpRequest.newBuilder(uri).method(method, bodyPublisher);
        final Set<String> uniqueNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (randomBoolean()) {
            Map<String, List<String>> headers = RestClientTestUtil.randomHeaders(getRandom(), "Header");
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeaders(headers);
            for (Map.Entry<String, List<String>> header : headers.entrySet()) {
                header.getValue().forEach(v -> expectedRequest.header(header.getKey(), v));
                uniqueNames.add(header.getKey());
            }
            builder = builder.withOptions(options);
        }
        for (Map.Entry<String, List<String>> defaultHeader : defaultHeaders.entrySet()) {
            // request level headers override default headers
            if (uniqueNames.contains(defaultHeader.getKey()) == false) {
                defaultHeader.getValue().forEach(v -> expectedRequest.header(defaultHeader.getKey(), v));
            }
        }
        try {
            performRequestSyncOrAsync(restClient, builder.build());
        } catch (Exception e) {
            // all good
        }
        return expectedRequest.build();
    }

    static Response performRequestSyncOrAsync(RestHttpClient restClient, Request request) throws Exception {
        // randomize between sync and async methods
        if (randomBoolean()) {
            return restClient.performRequest(request);
        } else {
            final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            final AtomicReference<Response> responseRef = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);
            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    responseRef.set(response);
                    latch.countDown();

                }

                @Override
                public void onFailure(Exception exception) {
                    exceptionRef.set(exception);
                    latch.countDown();
                }
            });
            latch.await();
            if (exceptionRef.get() != null) {
                throw exceptionRef.get();
            }
            return responseRef.get();
        }
    }

    /**
     * Asserts that the provided {@linkplain Exception} contains the method
     * that called this <strong>somewhere</strong> on its stack. This is
     * normally the case for synchronous calls but {@link RestHttpClient} performs
     * synchronous calls by performing asynchronous calls and blocking the
     * current thread until the call returns so it has to take special care
     * to make sure that the caller shows up in the exception. We use this
     * assertion to make sure that we don't break that "special care".
     */
    private static void assertExceptionStackContainsCallingMethod(Throwable t) {
        // 0 is getStackTrace
        // 1 is this method
        // 2 is the caller, what we want
        StackTraceElement myMethod = Thread.currentThread().getStackTrace()[2];
        for (StackTraceElement se : t.getStackTrace()) {
            if (se.getClassName().equals(myMethod.getClassName()) && se.getMethodName().equals(myMethod.getMethodName())) {
                return;
            }
        }
        StringWriter stack = new StringWriter();
        t.printStackTrace(new PrintWriter(stack));
        fail("didn't find the calling method (looks like " + myMethod + ") in:\n" + stack);
    }
}

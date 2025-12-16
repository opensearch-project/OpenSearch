/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Map;
import java.util.Optional;

/**
 * A builder for creating {@link HttpServerTransport.Dispatcher} instances
 * with sensible defaults for testing.
 * <p>
 * By default, successful and bad requests will fail with an assertion error,
 * and handler dispatch find no handler. Tests should override these behaviors
 * using the {@code with*} methods.
 */
public class TestDispatcherBuilder {

    private static final Logger logger = LogManager.getLogger(TestDispatcherBuilder.class);

    private DispatchRequest dispatchRequest = (request, channel, threadContext) -> {
        logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
        throw new AssertionError();
    };
    private DispatchBadRequest dispatchBadRequest = (channel, threadContext, cause) -> {
        logger.error(
            new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
            cause
        );
        throw new AssertionError(cause);
    };
    private DispatchHandler dispatchHandler = (uri, rawPath, method, params) -> Optional.empty();

    /**
     * Creates a new builder with default settings for creating {@link HttpServerTransport.Dispatcher} instances
     * in tests.
     * <p>
     * Created dispatcher instances could be used with e.g. {@link AbstractHttpServerTransport}.
     * @return the dispatcher builder instance
     */
    public static TestDispatcherBuilder dispatcherBuilderWithDefaults() {
        return new TestDispatcherBuilder();
    }

    /**
     * Sets the handler for successfully routed {@link RestRequest}s.
     * @param dispatchRequest the dispatch request implementation
     * @return this builder instance
     */
    public TestDispatcherBuilder withDispatchRequest(DispatchRequest dispatchRequest) {
        this.dispatchRequest = dispatchRequest;
        return this;
    }

    /**
     * Sets the handler for bad requests.
     * @param dispatchBadRequest the dispatch bad request implementation
     * @return this builder instance
     */
    public TestDispatcherBuilder withDispatchBadRequest(DispatchBadRequest dispatchBadRequest) {
        this.dispatchBadRequest = dispatchBadRequest;
        return this;
    }

    /**
     * Sets the router for finding the corresponding {@link RestHandler}.
     * @param dispatchHandler the dispatch handler implementation
     * @return this builder instance
     */
    public TestDispatcherBuilder withDispatchHandler(DispatchHandler dispatchHandler) {
        this.dispatchHandler = dispatchHandler;
        return this;
    }

    public HttpServerTransport.Dispatcher build() {
        DispatchRequest builderDispatchRequest = this.dispatchRequest;
        DispatchBadRequest builderDispatchBadRequest = this.dispatchBadRequest;
        DispatchHandler builderDispatchHandler = this.dispatchHandler;
        return new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                builderDispatchRequest.dispatchRequest(request, channel, threadContext);
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                builderDispatchBadRequest.dispatchBadRequest(channel, threadContext, cause);
            }

            @Override
            public Optional<RestHandler> dispatchHandler(
                String uri,
                String rawPath,
                RestRequest.Method method,
                Map<String, String> params
            ) {
                return builderDispatchHandler.dispatchHandler(uri, rawPath, method, params);
            }
        };
    }

    /**
     * Functional interface for {@link HttpServerTransport.Dispatcher#dispatchRequest(RestRequest, RestChannel, ThreadContext)}.
     */
    @FunctionalInterface
    public interface DispatchRequest {
        void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext);
    }

    /**
     * Functional interface for {@link HttpServerTransport.Dispatcher#dispatchBadRequest(RestChannel, ThreadContext, Throwable)}.
     */
    @FunctionalInterface
    public interface DispatchBadRequest {
        void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause);
    }

    /**
     * Functional interface for {@link HttpServerTransport.Dispatcher#dispatchHandler(String, String, RestRequest.Method, Map)}.
     */
    @FunctionalInterface
    public interface DispatchHandler {
        Optional<RestHandler> dispatchHandler(String uri, String rawPath, RestRequest.Method method, Map<String, String> params);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Map;
import java.util.Optional;

/**
 * A builder for creating instances of {@link HttpServerTransport.Dispatcher}
 * with sensible defaults for testing purposes.
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

    public static TestDispatcherBuilder withDefaults() {
        return new TestDispatcherBuilder();
    }

    public TestDispatcherBuilder withDispatchRequest(DispatchRequest dispatchRequest) {
        this.dispatchRequest = dispatchRequest;
        return this;
    }

    public TestDispatcherBuilder withDispatchBadRequest(DispatchBadRequest dispatchBadRequest) {
        this.dispatchBadRequest = dispatchBadRequest;
        return this;
    }

    public TestDispatcherBuilder withDispatchHandler(DispatchHandler dispatchHandler) {
        this.dispatchHandler = dispatchHandler;
        return this;
    }

    public HttpServerTransport.Dispatcher build() {
        DispatchRequest builderDispatchRequest = this.dispatchRequest;
        DispatchBadRequest builderDispatchBadRequest = this.dispatchBadRequest;
        DispatchHandler builderDispatcherHandler = this.dispatchHandler;
        return new HttpServerTransport.Dispatcher() {
            @Override
            public Optional<RestHandler> dispatchHandler(
                String uri,
                String rawPath,
                RestRequest.Method method,
                Map<String, String> params
            ) {
                return builderDispatcherHandler.dispatchHandler(uri, rawPath, method, params);
            }

            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                builderDispatchRequest.dispatchRequest(request, channel, threadContext);
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                builderDispatchBadRequest.dispatchBadRequest(channel, threadContext, cause);
            }
        };
    }

    @FunctionalInterface
    public interface DispatchRequest {
        void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext);
    }

    @FunctionalInterface
    public interface DispatchBadRequest {
        void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause);
    }

    @FunctionalInterface
    public interface DispatchHandler {
        Optional<RestHandler> dispatchHandler(String uri, String rawPath, RestRequest.Method method, Map<String, String> params);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.spi;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class GrpcInterceptorProviderTests extends OpenSearchTestCase {

    public void testBasicProviderImplementation() {
        TestGrpcInterceptorProvider provider = new TestGrpcInterceptorProvider(10);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        List<GrpcInterceptorProvider.OrderedGrpcInterceptor> interceptors = provider.getOrderedGrpcInterceptors(threadContext);
        assertNotNull(interceptors);
        assertEquals(1, interceptors.size());
        assertEquals(10, interceptors.get(0).order());
    }

    public void testProviderReturnsEmptyList() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        GrpcInterceptorProvider provider = new GrpcInterceptorProvider() {
            @Override
            public List<GrpcInterceptorProvider.OrderedGrpcInterceptor> getOrderedGrpcInterceptors(ThreadContext threadContext) {
                return Collections.emptyList();
            }
        };

        List<GrpcInterceptorProvider.OrderedGrpcInterceptor> interceptors = provider.getOrderedGrpcInterceptors(threadContext);
        assertNotNull(interceptors);
        assertTrue(interceptors.isEmpty());
    }

    public void testProviderReceivesThreadContext() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("X-Test-Header", "test-value");

        GrpcInterceptorProvider provider = new GrpcInterceptorProvider() {
            @Override
            public List<GrpcInterceptorProvider.OrderedGrpcInterceptor> getOrderedGrpcInterceptors(ThreadContext ctx) {
                // Verify that the provider receives the ThreadContext
                assertNotNull("ThreadContext should not be null", ctx);
                assertEquals("test-value", ctx.getHeader("X-Test-Header"));
                return Collections.emptyList();
            }
        };

        provider.getOrderedGrpcInterceptors(threadContext);
    }

    private static class TestGrpcInterceptorProvider implements GrpcInterceptorProvider {
        private final int order;

        TestGrpcInterceptorProvider(int order) {
            this.order = order;
        }

        @Override
        public List<GrpcInterceptorProvider.OrderedGrpcInterceptor> getOrderedGrpcInterceptors(ThreadContext threadContext) {
            return Collections.singletonList(createTestInterceptor(order, "test-interceptor"));
        }
    }

    /**
     * Creates a test OrderedGrpcInterceptor
     */
    private static GrpcInterceptorProvider.OrderedGrpcInterceptor createTestInterceptor(int order, String name) {
        return new GrpcInterceptorProvider.OrderedGrpcInterceptor() {
            @Override
            public int order() {
                return order;
            }

            @Override
            public ServerInterceptor getInterceptor() {
                return new ServerInterceptor() {
                    @Override
                    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call,
                        Metadata headers,
                        ServerCallHandler<ReqT, RespT> next
                    ) {
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }
}

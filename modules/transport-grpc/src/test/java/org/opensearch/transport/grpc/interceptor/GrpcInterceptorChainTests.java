/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.GrpcInterceptorProvider.OrderedGrpcInterceptor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcInterceptorChainTests extends OpenSearchTestCase {

    @Mock
    private ServerCall<String, String> mockCall;

    @Mock
    private ServerCallHandler<String, String> mockHandler;

    @Mock
    private ServerCall.Listener<String> mockListener;

    private Metadata headers;
    private ThreadContext threadContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        when(mockHandler.startCall(any(), any())).thenReturn(mockListener);
        headers = new Metadata();
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testEmptyChain() {
        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, Collections.emptyList());
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        // The result is now wrapped in a ThreadContextPreservingListener, not the raw mockListener
        verify(mockHandler).startCall(mockCall, headers);
    }

    public void testSingleSuccessfulInterceptor() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(createTestInterceptor(10, false, null));

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        verify(mockCall, never()).close(any(Status.class), any(Metadata.class));
    }

    public void testMultipleSuccessfulInterceptors() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, false, null),
            createTestInterceptor(20, false, null),
            createTestInterceptor(30, false, null)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        verify(mockCall, never()).close(any(Status.class), any(Metadata.class));
    }

    // FAILURE SCENARIO TESTS
    public void testFirstInterceptorFails() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, true, "First failure"),
            createTestInterceptor(20, false, null),
            createTestInterceptor(30, false, null)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.INTERNAL && status.getDescription().contains("First failure")),
            eq(headers)
        );
    }

    public void testMiddleInterceptorFails() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, false, null),
            createTestInterceptor(20, true, "Middle failure"),
            createTestInterceptor(30, false, null)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.INTERNAL && status.getDescription().contains("Middle failure")),
            eq(headers)
        );
    }

    public void testLastInterceptorFails() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, false, null),
            createTestInterceptor(20, false, null),
            createTestInterceptor(30, true, "Last failure")
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.INTERNAL && status.getDescription().contains("Last failure")),
            eq(headers)
        );
    }

    public void testInterceptorThrowsStatusRuntimeExceptionPermissionDenied() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, false, null),
            createStatusRuntimeExceptionInterceptor(20, Status.PERMISSION_DENIED.withDescription("Unauthorized access")),
            createTestInterceptor(30, false, null)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.PERMISSION_DENIED && status.getDescription().equals("Unauthorized access")),
            eq(headers)
        );
    }

    public void testInterceptorThrowsStatusRuntimeExceptionUnauthenticated() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createStatusRuntimeExceptionInterceptor(10, Status.UNAUTHENTICATED.withDescription("Invalid token")),
            createTestInterceptor(20, false, null)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.UNAUTHENTICATED && status.getDescription().equals("Invalid token")),
            eq(headers)
        );
    }

    public void testInterceptorThrowsStatusRuntimeExceptionResourceExhausted() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, false, null),
            createTestInterceptor(20, false, null),
            createStatusRuntimeExceptionInterceptor(30, Status.RESOURCE_EXHAUSTED.withDescription("Rate limit exceeded"))
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.RESOURCE_EXHAUSTED && status.getDescription().equals("Rate limit exceeded")),
            eq(headers)
        );
    }

    public void testInterceptorOrdering() {
        List<Integer> executionOrder = new ArrayList<>();

        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createOrderTrackingInterceptor(30, executionOrder),
            createOrderTrackingInterceptor(10, executionOrder),
            createOrderTrackingInterceptor(20, executionOrder)
        );

        // Sort as GrpcPlugin would
        interceptors.sort((a, b) -> Integer.compare(a.order(), b.order()));

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        // Verify execution order
        assertEquals(Arrays.asList(10, 20, 30), executionOrder);
    }

    public void testNullInterceptorList() {
        // Constructor should throw NullPointerException for null interceptors
        expectThrows(NullPointerException.class, () -> { new GrpcInterceptorChain(null); });
    }

    public void testChainIntegrationWithRealScenario() {
        // Simulate a real-world scenario: Auth -> Logging -> Metrics
        List<String> executionLog = new ArrayList<>();

        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createLoggingInterceptor(10, "AUTH", executionLog),
            createLoggingInterceptor(20, "LOGGING", executionLog),
            createLoggingInterceptor(30, "METRICS", executionLog)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        assertEquals(Arrays.asList("AUTH", "LOGGING", "METRICS"), executionLog);
    }

    /**
     * Generic test method that can be extended for different scenarios
     */
    public void testChainWithPattern(List<OrderedGrpcInterceptor> interceptors, boolean expectSuccess, String expectedErrorMessage) {
        GrpcInterceptorChain chain = new GrpcInterceptorChain(threadContext, interceptors);

        if (expectSuccess) {
            ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);
            assertNotNull(result);
            verify(mockCall, never()).close(any(Status.class), any(Metadata.class));
        } else {
            chain.interceptCall(mockCall, headers, mockHandler);
            verify(mockCall).close(
                argThat(status -> status.getCode() == Status.Code.INTERNAL && status.getDescription().contains(expectedErrorMessage)),
                eq(headers)
            );
        }
    }

    /**
     * Test method that can easily be extended with new interceptor combinations
     */
    @SuppressWarnings("unchecked")
    public void testVariousInterceptorCombinations() {
        // Success scenario
        testChainWithPattern(Arrays.asList(createTestInterceptor(10, false, null), createTestInterceptor(20, false, null)), true, null);

        // Reset mocks for next test
        reset(mockCall);

        // Failure scenario
        testChainWithPattern(
            Arrays.asList(createTestInterceptor(10, false, null), createTestInterceptor(20, true, "Test failure")),
            false,
            "Test failure"
        );
    }

    // =============================================
    // HELPER METHODS - EASILY EXTENSIBLE
    // =============================================

    /**
     * Creates a test interceptor with specified behavior - easily configurable
     */
    private OrderedGrpcInterceptor createTestInterceptor(int order, boolean shouldFail, String errorMessage) {
        return new OrderedGrpcInterceptor() {
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
                        if (shouldFail) {
                            throw new RuntimeException(errorMessage);
                        }
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }

    /**
     * Creates an interceptor that tracks execution order - useful for ordering tests
     */
    private OrderedGrpcInterceptor createOrderTrackingInterceptor(int order, List<Integer> executionOrder) {
        return new OrderedGrpcInterceptor() {
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
                        executionOrder.add(order);
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }

    /**
     * Creates a logging interceptor - useful for integration tests
     */
    private OrderedGrpcInterceptor createLoggingInterceptor(int order, String name, List<String> log) {
        return new OrderedGrpcInterceptor() {
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
                        log.add(name);
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }

    /**
     * Creates an interceptor that throws StatusRuntimeException with specific gRPC status
     * This is used to test the StatusRuntimeException handling path (lines 71-79)
     */
    private OrderedGrpcInterceptor createStatusRuntimeExceptionInterceptor(int order, Status status) {
        return new OrderedGrpcInterceptor() {
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
                        throw new StatusRuntimeException(status);
                    }
                };
            }
        };
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.interceptor;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
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

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        when(mockHandler.startCall(any(), any())).thenReturn(mockListener);
        headers = new Metadata();
    }

    // =============================================
    // BASIC FUNCTIONALITY TESTS
    // =============================================

    public void testEmptyChain() {
        GrpcInterceptorChain chain = new GrpcInterceptorChain(Collections.emptyList());
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        assertEquals(mockListener, result);
        verify(mockHandler).startCall(mockCall, headers);
    }

    public void testSingleSuccessfulInterceptor() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(createTestInterceptor(10, false, null));

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
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

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);

        assertNotNull(result);
        verify(mockCall, never()).close(any(Status.class), any(Metadata.class));
    }

    // =============================================
    // FAILURE SCENARIO TESTS
    // =============================================

    public void testFirstInterceptorFails() {
        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createTestInterceptor(10, true, "First failure"),
            createTestInterceptor(20, false, null),
            createTestInterceptor(30, false, null)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
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

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
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

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        verify(mockCall).close(
            argThat(status -> status.getCode() == Status.Code.INTERNAL && status.getDescription().contains("Last failure")),
            eq(headers)
        );
    }

    // =============================================
    // ORDERING TESTS
    // =============================================

    public void testInterceptorOrdering() {
        List<Integer> executionOrder = new ArrayList<>();

        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createOrderTrackingInterceptor(30, executionOrder),
            createOrderTrackingInterceptor(10, executionOrder),
            createOrderTrackingInterceptor(20, executionOrder)
        );

        // Sort as GrpcPlugin would
        interceptors.sort((a, b) -> Integer.compare(a.getOrder(), b.getOrder()));

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        // Verify execution order
        assertEquals(Arrays.asList(10, 20, 30), executionOrder);
    }

    // =============================================
    // EDGE CASE TESTS
    // =============================================

    public void testNullInterceptorList() {
        // Constructor should accept null, but interceptCall should fail
        GrpcInterceptorChain chain = new GrpcInterceptorChain(null);
        assertNotNull(chain);

        // When interceptCall is called with null interceptors, it should throw NullPointerException
        expectThrows(NullPointerException.class, () -> { chain.interceptCall(mockCall, headers, mockHandler); });
    }

    // =============================================
    // PERFORMANCE TESTS
    // =============================================

    public void testLargeChainPerformance() {
        // Test with 50 interceptors
        List<OrderedGrpcInterceptor> interceptors = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            interceptors.add(createTestInterceptor(i * 10, false, null));
        }

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);

        long startTime = System.currentTimeMillis();
        ServerCall.Listener<String> result = chain.interceptCall(mockCall, headers, mockHandler);
        long endTime = System.currentTimeMillis();

        assertNotNull(result);
        assertTrue("Chain execution should be fast", (endTime - startTime) < 100); // Less than 100ms
        verify(mockCall, never()).close(any(Status.class), any(Metadata.class));
    }

    // =============================================
    // INTEGRATION TESTS
    // =============================================

    public void testChainIntegrationWithRealScenario() {
        // Simulate a real-world scenario: Auth -> Logging -> Metrics
        List<String> executionLog = new ArrayList<>();

        List<OrderedGrpcInterceptor> interceptors = Arrays.asList(
            createLoggingInterceptor(10, "AUTH", executionLog),
            createLoggingInterceptor(20, "LOGGING", executionLog),
            createLoggingInterceptor(30, "METRICS", executionLog)
        );

        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);
        chain.interceptCall(mockCall, headers, mockHandler);

        assertEquals(Arrays.asList("AUTH", "LOGGING", "METRICS"), executionLog);
    }

    // =============================================
    // SCALABLE TEST PATTERNS
    // =============================================

    /**
     * Generic test method that can be extended for different scenarios
     */
    public void testChainWithPattern(List<OrderedGrpcInterceptor> interceptors, boolean expectSuccess, String expectedErrorMessage) {
        GrpcInterceptorChain chain = new GrpcInterceptorChain(interceptors);

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
            public int getOrder() {
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
            public int getOrder() {
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
            public int getOrder() {
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
     * Factory method to create interceptors with execution counting - useful for performance tests
     */
    private OrderedGrpcInterceptor createCountingInterceptor(int order, AtomicInteger counter) {
        return new OrderedGrpcInterceptor() {
            @Override
            public int getOrder() {
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
                        counter.incrementAndGet();
                        return next.startCall(call, headers);
                    }
                };
            }
        };
    }
}

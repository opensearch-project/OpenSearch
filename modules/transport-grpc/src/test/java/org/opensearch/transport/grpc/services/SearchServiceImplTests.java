/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import io.grpc.stub.StreamObserver;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchServiceImplTests extends OpenSearchTestCase {

    private SearchServiceImpl service;
    private AbstractQueryBuilderProtoUtils queryUtils;

    @Mock
    private NodeClient client;

    @Mock
    private CircuitBreakerService circuitBreakerService;

    @Mock
    private CircuitBreaker circuitBreaker;

    @Mock
    private StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        when(circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS)).thenReturn(circuitBreaker);
        queryUtils = QueryBuilderProtoTestUtils.createQueryUtils();
        service = new SearchServiceImpl(client, queryUtils, circuitBreakerService);
    }

    public void testConstructorWithNullClient() {
        // Test that constructor throws IllegalArgumentException when client is null
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchServiceImpl(null, queryUtils, circuitBreakerService)
        );

        assertEquals("Client cannot be null", exception.getMessage());
    }

    public void testConstructorWithNullQueryUtils() {
        // Test that constructor throws IllegalArgumentException when queryUtils is null
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchServiceImpl(client, null, circuitBreakerService)
        );

        assertEquals("Query utils cannot be null", exception.getMessage());
    }

    public void testConstructorWithNullCircuitBreakerService() {
        // Test that constructor throws IllegalArgumentException when circuitBreakerService is null
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchServiceImpl(client, queryUtils, null)
        );

        assertEquals("Circuit breaker service cannot be null", exception.getMessage());
    }

    public void testConstructorWithBothNull() {
        // Test that constructor throws IllegalArgumentException when both parameters are null
        // Should fail on the first null check (client)
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchServiceImpl(null, null, circuitBreakerService)
        );

        assertEquals("Client cannot be null", exception.getMessage());
    }

    public void testSearchSuccess() throws IOException {
        // Create a test request
        org.opensearch.protobufs.SearchRequest request = createTestSearchRequest();

        // Call the search method
        service.search(request, responseObserver);

        // Verify that client.search was called with any SearchRequest and any ActionListener
        verify(client).search(any(org.opensearch.action.search.SearchRequest.class), any());
    }

    public void testCircuitBreakerCheckedBeforeProcessing() throws IOException {
        // Create a test request
        org.opensearch.protobufs.SearchRequest request = createTestSearchRequest();

        // Call the search method
        service.search(request, responseObserver);

        // Verify circuit breaker was checked with the request size
        verify(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), eq("<grpc_request>"));

        // Verify client.search was called
        verify(client).search(any(org.opensearch.action.search.SearchRequest.class), any());
    }

    public void testCircuitBreakerTripsAndRejectsRequest() throws IOException {
        // Create a test request
        org.opensearch.protobufs.SearchRequest request = createTestSearchRequest();

        // Make circuit breaker throw exception
        CircuitBreakingException circuitBreakerException = new CircuitBreakingException(
            "Data too large",
            100L,
            50 * 1024 * 1024L,
            CircuitBreaker.Durability.TRANSIENT
        );
        doThrow(circuitBreakerException).when(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        // Call the search method
        service.search(request, responseObserver);

        // Verify circuit breaker was checked
        verify(circuitBreaker).addEstimateBytesAndMaybeBreak(anyLong(), eq("<grpc_request>"));

        // Verify client.search was NOT called (request was rejected before processing)
        verify(client, never()).search(any(org.opensearch.action.search.SearchRequest.class), any());

        // Verify error was sent to client
        verify(responseObserver).onError(any());
    }

    public void testCircuitBreakerStreamObserverWrapsResponseObserver() throws IOException {
        // Create a test request
        org.opensearch.protobufs.SearchRequest request = createTestSearchRequest();

        // Capture the ActionListener to simulate success
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<SearchResponse>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        // Call the search method
        service.search(request, responseObserver);

        // Verify client.search was called and capture the listener
        verify(client).search(any(org.opensearch.action.search.SearchRequest.class), listenerCaptor.capture());

        // Simulate successful response
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new org.opensearch.action.search.ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));
        listenerCaptor.getValue().onResponse(mockResponse);

        // Verify the wrapped observer was called (through CircuitBreakerStreamObserver)
        verify(responseObserver).onNext(any(org.opensearch.protobufs.SearchResponse.class));
        verify(responseObserver).onCompleted();

        // Verify bytes were released (via CircuitBreakerStreamObserver wrapper)
        verify(circuitBreaker).addWithoutBreaking(anyLong());
    }

    public void testCircuitBreakerBytesReleasedOnException() throws IOException {
        // Create a test request
        org.opensearch.protobufs.SearchRequest request = createTestSearchRequest();

        // Capture the bytes added and released
        ArgumentCaptor<Long> addedBytesCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> releasedBytesCaptor = ArgumentCaptor.forClass(Long.class);

        // Mock client to throw an exception
        doThrow(new RuntimeException("Test exception")).when(client).search(any(), any());

        // Call search method
        service.search(request, responseObserver);

        // Verify bytes were added first (positive value)
        verify(circuitBreaker).addEstimateBytesAndMaybeBreak(addedBytesCaptor.capture(), eq("<grpc_request>"));

        // Verify bytes were released after exception (negative value)
        verify(circuitBreaker).addWithoutBreaking(releasedBytesCaptor.capture());

        // Verify the magnitudes match (added is positive, released is negative of same value)
        long addedBytes = addedBytesCaptor.getValue();
        long releasedBytes = releasedBytesCaptor.getValue();
        assertTrue("Added bytes should be positive", addedBytes > 0);
        assertEquals("Released bytes should equal negative of added bytes", -addedBytes, releasedBytes);

        // Verify error was sent to client
        verify(responseObserver).onError(any());
    }

    private org.opensearch.protobufs.SearchRequest createTestSearchRequest() {
        return org.opensearch.protobufs.SearchRequest.newBuilder()
            .addIndex("test-index")
            .setSearchRequestBody(SearchRequestBody.newBuilder().setSize(10).build())
            .build();
    }

    public void testSearchWithAggregations() throws IOException {
        // Integration test: search request with aggregations returns aggregated results
        // Create test request with aggregations
        org.opensearch.protobufs.SearchRequest request =
            org.opensearch.protobufs.SearchRequest.newBuilder()
                .addIndex("test-index")
                .setSearchRequestBody(
                    SearchRequestBody.newBuilder()
                        .setSize(10)
                        .putAggregations("max_price",
                            AggregationContainer.newBuilder()
                                .setMax(MaxAggregation.newBuilder().setField("price").build())
                                .build())
                        .build())
                .build();

        // Mock action response with aggregations
        InternalMax maxAgg = new InternalMax("max_price", 150.00, DocValueFormat.RAW, null);
        InternalAggregations aggregations = InternalAggregations.from(List.of(maxAgg));

        SearchResponse mockSearchResponse = mock(SearchResponse.class);
        when(mockSearchResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockSearchResponse.getAggregations()).thenReturn(aggregations);
        when(mockSearchResponse.getTook()).thenReturn(TimeValue.timeValueMillis(50));
        when(mockSearchResponse.isTimedOut()).thenReturn(false);
        when(mockSearchResponse.getTotalShards()).thenReturn(5);
        when(mockSearchResponse.getSuccessfulShards()).thenReturn(5);
        when(mockSearchResponse.getSkippedShards()).thenReturn(0);
        when(mockSearchResponse.getFailedShards()).thenReturn(0);
        when(mockSearchResponse.getShardFailures()).thenReturn(new org.opensearch.action.search.ShardSearchFailure[0]);
        when(mockSearchResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockSearchResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));
        when(mockSearchResponse.getSuggest()).thenReturn(null);
        when(mockSearchResponse.getProfileResults()).thenReturn(null);

        // Setup action listener to capture and respond
        doAnswer(invocation -> {
            org.opensearch.action.search.SearchRequest searchRequest = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            // Verify request has aggregations
            assertNotNull("Request should have source", searchRequest.source());
            assertNotNull("Request should have aggregations", searchRequest.source().aggregations());
            assertEquals("Request should have 1 aggregation", 1, searchRequest.source().aggregations().count());
            listener.onResponse(mockSearchResponse);
            return null;
        }).when(client).search(any(), any());

        // Call service
        @SuppressWarnings("unchecked")
        StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver =
            mock(StreamObserver.class);
        service.search(request, responseObserver);

        // Verify response
        @SuppressWarnings("unchecked")
        ArgumentCaptor<org.opensearch.protobufs.SearchResponse> responseCaptor =
            ArgumentCaptor.forClass(org.opensearch.protobufs.SearchResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        org.opensearch.protobufs.SearchResponse response = responseCaptor.getValue();
        assertEquals("Response should have 1 aggregation", 1, response.getAggregationsCount());
        assertTrue("Response should contain max_price aggregation", response.containsAggregations("max_price"));

        org.opensearch.protobufs.Aggregate maxAggregate = response.getAggregationsOrThrow("max_price");
        assertTrue("max_price should have Max aggregate", maxAggregate.hasMax());
        assertEquals("max_price value should match", 150.00, maxAggregate.getMax().getValue().getDouble(), 0.001);
    }
}

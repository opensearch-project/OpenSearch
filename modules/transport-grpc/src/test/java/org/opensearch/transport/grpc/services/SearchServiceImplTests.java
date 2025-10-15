/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.services;

import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;
import org.junit.Before;

import java.io.IOException;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

public class SearchServiceImplTests extends OpenSearchTestCase {

    private SearchServiceImpl service;
    private AbstractQueryBuilderProtoUtils queryUtils;

    @Mock
    private NodeClient client;

    @Mock
    private StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        queryUtils = QueryBuilderProtoTestUtils.createQueryUtils();
        service = new SearchServiceImpl(client, queryUtils, true);
    }

    public void testConstructorWithNullClient() {
        // Test that constructor throws IllegalArgumentException when client is null
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchServiceImpl(null, queryUtils, true)
        );

        assertEquals("Client cannot be null", exception.getMessage());
    }

    public void testConstructorWithNullQueryUtils() {
        // Test that constructor throws IllegalArgumentException when queryUtils is null
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new SearchServiceImpl(client, null, true));

        assertEquals("Query utils cannot be null", exception.getMessage());
    }

    public void testConstructorWithBothNull() {
        // Test that constructor throws IllegalArgumentException when both parameters are null
        // Should fail on the first null check (client)
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new SearchServiceImpl(null, null, true));

        assertEquals("Client cannot be null", exception.getMessage());
    }

    public void testSearchSuccess() {
        // Create a test request
        SearchRequest request = createTestSearchRequest();

        // Call the search method
        service.search(request, responseObserver);

        // Verify that client.search was called with any SearchRequest and any ActionListener
        verify(client).search(any(org.opensearch.action.search.SearchRequest.class), any());
    }

    public void testSearchWithException() {
        // Create a test request
        SearchRequest request = createTestSearchRequest();

        // Mock client to throw an exception
        doThrow(new RuntimeException("Test exception")).when(client).search(any(), any());

        // Call search method
        service.search(request, responseObserver);

        // Verify that responseObserver.onError was called
        verify(responseObserver).onError(any());
    }

    public void testErrorTracingConfigValidationFailsWhenServerSettingIsDisabledAndRequestRequiresTracing() {
        // Setup request and the service, server setting is off and request requires tracing
        SearchRequest request = createTestSearchRequest();
        SearchServiceImpl serviceWithDisabledErrorsTracing = new SearchServiceImpl(client, queryUtils, false);

        // Call search method
        serviceWithDisabledErrorsTracing.search(request, responseObserver);

        // Verify that responseObserver.onError reports request parameter must be disabled
        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    public void testErrorTracingConfigValidationPassesWhenServerSettingIsDisabledAndRequestSkipsTracing() {
        // Setup request and the service, server setting is off and request skips tracing
        SearchRequest request = createTestSearchRequest().toBuilder()
            .setGlobalParams(org.opensearch.protobufs.GlobalParams.newBuilder().setErrorTrace(false))
            .build();
        SearchServiceImpl serviceWithDisabledErrorsTracing = new SearchServiceImpl(client, queryUtils, false);

        // Call search method
        serviceWithDisabledErrorsTracing.search(request, responseObserver);

        // Verify that client.search was called
        verify(client).search(any(org.opensearch.action.search.SearchRequest.class), any());
    }

    private SearchRequest createTestSearchRequest() {
        return SearchRequest.newBuilder()
            .addIndex("test-index")
            .setRequestBody(SearchRequestBody.newBuilder().setSize(10).build())
            .setGlobalParams(org.opensearch.protobufs.GlobalParams.newBuilder().setErrorTrace(true).build())
            .build();
    }
}

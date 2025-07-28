/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.services;

import org.opensearch.plugin.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.junit.Before;

import java.io.IOException;

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
        service = new SearchServiceImpl(client, queryUtils);
    }

    public void testSearchSuccess() throws IOException {
        // Create a test request
        SearchRequest request = createTestSearchRequest();

        // Call the search method
        service.search(request, responseObserver);

        // Verify that client.search was called with any SearchRequest and any ActionListener
        verify(client).search(any(org.opensearch.action.search.SearchRequest.class), any());
    }

    public void testSearchError() throws IOException {
        // Create a test request
        SearchRequest request = createTestSearchRequest();

        // Make the client throw an exception when search is called
        doThrow(new RuntimeException("Test exception")).when(client).search(any(org.opensearch.action.search.SearchRequest.class), any());

        // Call the search method
        service.search(request, responseObserver);

        // Verify that the error was sent
        verify(responseObserver).onError(any(RuntimeException.class));
    }

    private SearchRequest createTestSearchRequest() {
        SearchRequestBody requestBody = SearchRequestBody.newBuilder().build();

        return SearchRequest.newBuilder().setRequestBody(requestBody).build();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.protobufs.GlobalParams;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SearchRequestActionListenerTests extends OpenSearchTestCase {

    @Mock
    private StreamObserver<org.opensearch.protobufs.SearchResponse> responseObserver;

    private SearchRequestActionListener listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        listener = new SearchRequestActionListener(responseObserver, GlobalParams.newBuilder().build());
    }

    public void testOnResponse() {

        // Create a SearchResponse
        SearchResponse mockSearchResponse = new SearchResponse(
            new SearchResponseSections(SearchHits.empty(), null, null, false, false, null, 1),
            randomAlphaOfLengthBetween(5, 10),
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // Call the method under test
        listener.onResponse(mockSearchResponse);

        // Verify that onNext and onCompleted were called
        verify(responseObserver, times(1)).onNext(any(org.opensearch.protobufs.SearchResponse.class));
        verify(responseObserver, times(1)).onCompleted();
    }

    public void testOnFailure() {
        // Create an exception
        Exception exception = new Exception("Test exception");

        // Call the method under test
        listener.onFailure(exception);

        // Verify that onError was called with a StatusRuntimeException
        verify(responseObserver, times(1)).onError(any(StatusRuntimeException.class));
    }
}

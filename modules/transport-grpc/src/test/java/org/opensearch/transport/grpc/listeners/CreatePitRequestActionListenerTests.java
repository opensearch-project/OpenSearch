/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.test.OpenSearchTestCase;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CreatePitRequestActionListenerTests extends OpenSearchTestCase {

    @Mock
    private StreamObserver<org.opensearch.protobufs.CreatePITResponse> responseObserver;

    private CreatePitRequestActionListener listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        listener = new CreatePitRequestActionListener(responseObserver);
    }

    public void testOnResponse() {
        CreatePitResponse response = new CreatePitResponse("pit-123", 123L, 3, 3, 0, 0, ShardSearchFailure.EMPTY_ARRAY);

        listener.onResponse(response);

        verify(responseObserver, times(1)).onNext(any(org.opensearch.protobufs.CreatePITResponse.class));
        verify(responseObserver, times(1)).onCompleted();
    }

    public void testOnFailure() {
        listener.onFailure(new Exception("Test exception"));

        verify(responseObserver, times(1)).onError(any(StatusRuntimeException.class));
    }
}

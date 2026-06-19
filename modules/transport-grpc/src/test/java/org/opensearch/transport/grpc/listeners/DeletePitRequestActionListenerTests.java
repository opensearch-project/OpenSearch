/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.listeners;

import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DeletePitRequestActionListenerTests extends OpenSearchTestCase {

    @Mock
    private StreamObserver<org.opensearch.protobufs.DeletePITResponse> responseObserver;

    private DeletePitRequestActionListener listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        listener = new DeletePitRequestActionListener(responseObserver);
    }

    public void testOnResponse() {
        DeletePitResponse response = new DeletePitResponse(List.of(new DeletePitInfo(true, "pit-123")));

        listener.onResponse(response);

        verify(responseObserver, times(1)).onNext(any(org.opensearch.protobufs.DeletePITResponse.class));
        verify(responseObserver, times(1)).onCompleted();
    }

    public void testOnFailure() {
        listener.onFailure(new Exception("Test exception"));

        verify(responseObserver, times(1)).onError(any(StatusRuntimeException.class));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import org.opensearch.test.OpenSearchTestCase;

import io.grpc.stub.ServerCallStreamObserver;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Verifies {@link OSFlightListeners#setOnReadyThreshold} sets the watermark on the gRPC observer of a listener
 * backed by {@link OutboundStreamListenerImpl} (as Arrow's server listener is), and is a no-op otherwise.
 */
public class OSFlightListenersTests extends OpenSearchTestCase {

    /**
     * A {@link OutboundStreamListenerImpl}-backed {@code ServerStreamListener}, matching the shape of Arrow's
     * package-private server listener so {@code responseObserver} is the real Arrow field the bridge reads.
     */
    private static final class TestServerStreamListener extends OutboundStreamListenerImpl implements FlightProducer.ServerStreamListener {
        TestServerStreamListener(ServerCallStreamObserver<ArrowMessage> observer) {
            super(null, observer);
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(Runnable handler) {}

        @Override
        protected void waitUntilStreamReady() {}
    }

    @SuppressWarnings("unchecked")
    public void testSetOnReadyThresholdReachesServerCallObserver() {
        ServerCallStreamObserver<ArrowMessage> observer = mock(ServerCallStreamObserver.class);
        TestServerStreamListener listener = new TestServerStreamListener(observer);

        OSFlightListeners.setOnReadyThreshold(listener, 512 * 1024);

        verify(observer).setOnReadyThreshold(512 * 1024);
    }

    public void testSetOnReadyThresholdIgnoresListenerWithoutObserver() {
        FlightProducer.ServerStreamListener notObserverBacked = mock(FlightProducer.ServerStreamListener.class);
        OSFlightListeners.setOnReadyThreshold(notObserverBacked, 512 * 1024);
    }

    @SuppressWarnings("unchecked")
    public void testSetOnReadyThresholdAppliesExactValue() {
        ServerCallStreamObserver<ArrowMessage> observer = mock(ServerCallStreamObserver.class);
        TestServerStreamListener listener = new TestServerStreamListener(observer);

        OSFlightListeners.setOnReadyThreshold(listener, 1);
        OSFlightListeners.setOnReadyThreshold(listener, 64 * 1024 * 1024);

        verify(observer).setOnReadyThreshold(1);
        verify(observer).setOnReadyThreshold(64 * 1024 * 1024);
        verify(observer, never()).setOnReadyThreshold(0);
    }
}

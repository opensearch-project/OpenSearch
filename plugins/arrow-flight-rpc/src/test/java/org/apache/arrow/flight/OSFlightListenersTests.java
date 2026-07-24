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
 * Proves {@link OSFlightListeners#setOnReadyThreshold} actually reaches gRPC's per-stream watermark on a REAL
 * Arrow server listener, not just the injected seam used by the {@code FlightServerChannel} unit tests.
 *
 * <p>Arrow's concrete server-side listener ({@code FlightService.GetListener}) extends
 * {@link OutboundStreamListenerImpl} and holds the call's {@link ServerCallStreamObserver} in the {@code
 * protected} {@code responseObserver} field. {@code GetListener} is not accessible from tests, so this uses a
 * minimal listener with the identical shape — extends {@code OutboundStreamListenerImpl} (so the {@code
 * responseObserver} the bridge reads is the real Arrow field) and implements {@code
 * FlightProducer.ServerStreamListener}. The test asserts the threshold call lands on the observer, exercising
 * the {@code instanceof OutboundStreamListenerImpl} cast and {@code responseObserver} access the production
 * path depends on. If an Arrow upgrade changed the listener hierarchy or hid the observer, this fails instead
 * of the threshold silently becoming a no-op.
 */
public class OSFlightListenersTests extends OpenSearchTestCase {

    /**
     * Same shape as Arrow's package-private {@code FlightService.GetListener}: a {@code ServerStreamListener}
     * backed by {@link OutboundStreamListenerImpl}, so {@code responseObserver} is the genuine Arrow field the
     * bridge reads. Only the members needed to instantiate are implemented.
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
        // A ServerStreamListener that is not an OutboundStreamListenerImpl (e.g. a test double) must be a
        // safe no-op rather than throwing, so the stream simply keeps the transport-wide default watermark.
        FlightProducer.ServerStreamListener notObserverBacked = mock(FlightProducer.ServerStreamListener.class);

        OSFlightListeners.setOnReadyThreshold(notObserverBacked, 512 * 1024);
        // No exception; nothing to apply.
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

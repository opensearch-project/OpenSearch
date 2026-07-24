/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;

import io.grpc.stub.ServerCallStreamObserver;

/**
 * Reaches the gRPC {@link ServerCallStreamObserver} behind an Arrow {@link ServerStreamListener}, which Arrow's
 * public API does not expose. Lives in {@code org.apache.arrow.flight} to access the {@code protected}
 * {@code responseObserver} field of {@link OutboundStreamListenerImpl} without reflection.
 */
public final class OSFlightListeners {

    private OSFlightListeners() {}

    /**
     * Sets the per-stream outbound buffer watermark (bytes at which {@code isReady()} flips false) on the
     * listener's gRPC observer. May be called after the call has started. No-op if the listener is not backed
     * by a {@link ServerCallStreamObserver}.
     */
    public static void setOnReadyThreshold(ServerStreamListener listener, int bytes) {
        if (listener instanceof OutboundStreamListenerImpl impl
            && impl.responseObserver instanceof ServerCallStreamObserver<?> observer) {
            observer.setOnReadyThreshold(bytes);
        }
    }
}

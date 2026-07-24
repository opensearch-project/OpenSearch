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
 * Package-private bridge into Arrow Flight server listener internals, living in {@code org.apache.arrow.flight}
 * (alongside {@link OSFlightServer}) so it can reach members not exposed by Arrow's public API.
 *
 * <p>The gRPC per-stream backpressure watermark is set via {@link ServerCallStreamObserver#setOnReadyThreshold(int)},
 * but Arrow's {@link ServerStreamListener} does not surface that observer. The concrete server listener extends
 * {@link OutboundStreamListenerImpl}, whose {@code responseObserver} is {@code protected} and therefore accessible
 * from this package. This is ordinary package access, not reflection: a change to that field on an Arrow upgrade is
 * a compile error surfaced here, not a runtime surprise.
 */
public final class OSFlightListeners {

    private OSFlightListeners() {}

    /**
     * Applies a per-stream outbound buffer threshold to the given server listener, if it is backed by a gRPC
     * {@link ServerCallStreamObserver}. gRPC allows the threshold to be re-set at any point during the call, so this
     * may be called after the call has started. A listener not backed by such an observer (e.g. a test double) is a
     * no-op.
     *
     * @param listener the Arrow server stream listener for the call
     * @param bytes    the outbound buffered-bytes watermark at which {@code isReady()} flips false
     */
    public static void setOnReadyThreshold(ServerStreamListener listener, int bytes) {
        if (listener instanceof OutboundStreamListenerImpl impl
            && impl.responseObserver instanceof ServerCallStreamObserver<?> observer) {
            observer.setOnReadyThreshold(bytes);
        }
    }
}

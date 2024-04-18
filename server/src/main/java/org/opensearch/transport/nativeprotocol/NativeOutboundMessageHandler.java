/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nativeprotocol;

import org.opensearch.Version;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.OutboundMessageHandler;
import org.opensearch.transport.ProtocolOutboundMessage;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Set;

/**
 * Outbound message handler for native transport protocol.
 *
 * @opensearch.internal
 */
public class NativeOutboundMessageHandler implements OutboundMessageHandler {

    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;

    public NativeOutboundMessageHandler(String[] features, StatsTracker statsTracker, ThreadPool threadPool, BigArrays bigArrays) {
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
    }

    @Override
    public void sendMessage(TcpChannel channel, ProtocolOutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        NativeOutboundMessage nativeOutboundMessage = (NativeOutboundMessage) networkMessage;
        MessageSerializer serializer = new MessageSerializer(nativeOutboundMessage, bigArrays);
        SendContext sendContext = new SendContext(channel, serializer, listener, serializer, statsTracker);
        internalSend(channel, sendContext, threadPool);
    }

    @Override
    public NativeOutboundMessage.Request convertRequestToOutboundMessage(
        final TransportRequest request,
        final String action,
        final long requestId,
        final boolean isHandshake,
        final boolean compressRequest,
        final Version version
    ) {
        return new NativeOutboundMessage.Request(
            threadPool.getThreadContext(),
            features,
            request,
            version,
            action,
            requestId,
            isHandshake,
            compressRequest
        );
    }

    @Override
    public NativeOutboundMessage.Response convertResponseToOutboundMessage(
        final TransportResponse response,
        final Set<String> features,
        final long requestId,
        final boolean isHandshake,
        final boolean compress,
        final Version version
    ) {
        return new NativeOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            response,
            version,
            requestId,
            isHandshake,
            compress
        );
    }

    @Override
    public NativeOutboundMessage.Response convertErrorResponseToOutboundMessage(
        final RemoteTransportException tx,
        final Set<String> features,
        final long requestId,
        final boolean isHandshake,
        final boolean compress,
        final Version version
    ) {
        return new NativeOutboundMessage.Response(threadPool.getThreadContext(), features, tx, version, requestId, false, false);
    }

    /**
     * Internal message serializer
     *
     * @opensearch.internal
     */
    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private final NativeOutboundMessage message;
        private final BigArrays bigArrays;
        private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(NativeOutboundMessage message, BigArrays bigArrays) {
            this.message = message;
            this.bigArrays = bigArrays;
        }

        @Override
        public BytesReference get() throws IOException {
            bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
            return message.serialize(bytesStreamOutput);
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }

}

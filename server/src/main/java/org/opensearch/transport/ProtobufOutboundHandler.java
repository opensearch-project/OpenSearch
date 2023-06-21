/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.NotifyOnceListener;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.network.CloseableChannel;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;

/**
 * Outbound data handler
*
* @opensearch.internal
*/
final class ProtobufOutboundHandler {

    private static final Logger logger = LogManager.getLogger(ProtobufOutboundHandler.class);

    private final String nodeName;
    private final Version version;
    private final String[] features;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    // private final BigArrays bigArrays;
    private final byte[] bytes;
    private volatile ProtobufTransportMessageListener messageListener = ProtobufTransportMessageListener.NOOP_LISTENER;

    ProtobufOutboundHandler(
        String nodeName,
        Version version,
        String[] features,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        byte[] bytes
    ) {
        this.nodeName = nodeName;
        this.version = version;
        this.features = features;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bytes = bytes;
    }

    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        SendContext sendContext = new SendContext(channel, () -> bytes, listener);
        try {
            internalSend(channel, sendContext);
        } catch (IOException e) {
            // This should not happen as the bytes are already serialized
            throw new AssertionError(e);
        }
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link ProtobufTransportRequest}
    * objects back to the caller.
    */
    void sendRequest(
        final ProtobufDiscoveryNode node,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final ProtobufTransportRequest request,
        final TransportRequestOptions options,
        final Version channelVersion,
        final boolean compressRequest,
        final boolean isHandshake
    ) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        ProtobufOutboundMessage.Request message = new ProtobufOutboundMessage.Request(
            threadPool.getThreadContext(),
            features,
            request,
            version,
            action,
            requestId,
            isHandshake,
            compressRequest
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onRequestSent(node, requestId, action, request, options));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link ProtobufTransportResponse}
    * objects back to the caller.
    *
    * @see #sendErrorResponse(Version, Set, TcpChannel, long, String, Exception) for sending error responses
    */
    void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final ProtobufTransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        ProtobufOutboundMessage.Response message = new ProtobufOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            response,
            version,
            requestId,
            isHandshake,
            compress
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, response));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends back an error response to the caller via the given channel
    */
    void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final Exception error
    ) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        ProtobufTransportAddress address = new ProtobufTransportAddress(channel.getLocalAddress());
        ProtobufRemoteTransportException tx = new ProtobufRemoteTransportException(nodeName, address, action, error);
        ProtobufOutboundMessage.Response message = new ProtobufOutboundMessage.Response(
            threadPool.getThreadContext(),
            features,
            tx,
            version,
            requestId,
            false,
            false
        );
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        sendMessage(channel, message, listener);
    }

    private void sendMessage(TcpChannel channel, ProtobufOutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        MessageSerializer serializer = new MessageSerializer(networkMessage, bytes);
        SendContext sendContext = new SendContext(channel, serializer, listener, serializer);
        internalSend(channel, sendContext);
    }

    private void internalSend(TcpChannel channel, SendContext sendContext) throws IOException {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        BytesReference reference = sendContext.get();
        // stash thread context so that channel event loop is not polluted by thread context
        try (ThreadContext.StoredContext existing = threadPool.getThreadContext().stashContext()) {
            channel.sendMessage(reference, sendContext);
        } catch (RuntimeException ex) {
            sendContext.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }
    }

    void setMessageListener(ProtobufTransportMessageListener listener) {
        if (messageListener == ProtobufTransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    /**
     * Internal message serializer
    *
    * @opensearch.internal
    */
    private static class MessageSerializer implements CheckedSupplier<BytesReference, IOException>, Releasable {

        private final ProtobufOutboundMessage message;
        private byte[] bytes;
        // private final BigArrays bigArrays;
        // private volatile ReleasableBytesStreamOutput bytesStreamOutput;

        private MessageSerializer(ProtobufOutboundMessage message, byte[] bytes) {
            this.message = message;
            this.bytes = bytes;
        }

        @Override
        public BytesReference get() throws IOException {
            CodedOutputStream out = CodedOutputStream.newInstance(bytes);
            return message.serialize(out, bytes);
        }

        @Override
        public void close() {
            // IOUtils.closeWhileHandlingException(bytesStreamOutput);
        }
    }

    private class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {

        private final TcpChannel channel;
        private final CheckedSupplier<BytesReference, IOException> messageSupplier;
        private final ActionListener<Void> listener;
        private final Releasable optionalReleasable;
        private long messageSize = -1;

        private SendContext(
            TcpChannel channel,
            CheckedSupplier<BytesReference, IOException> messageSupplier,
            ActionListener<Void> listener
        ) {
            this(channel, messageSupplier, listener, null);
        }

        private SendContext(
            TcpChannel channel,
            CheckedSupplier<BytesReference, IOException> messageSupplier,
            ActionListener<Void> listener,
            Releasable optionalReleasable
        ) {
            this.channel = channel;
            this.messageSupplier = messageSupplier;
            this.listener = listener;
            this.optionalReleasable = optionalReleasable;
        }

        public BytesReference get() throws IOException {
            BytesReference message;
            try {
                message = messageSupplier.get();
                messageSize = message.length();
                TransportLogger.logOutboundMessage(channel, message);
                return message;
            } catch (Exception e) {
                onFailure(e);
                throw e;
            }
        }

        @Override
        protected void innerOnResponse(Void v) {
            assert messageSize != -1 : "If onResponse is being called, the message should have been serialized";
            statsTracker.markBytesWritten(messageSize);
            closeAndCallback(() -> listener.onResponse(v));
        }

        @Override
        protected void innerOnFailure(Exception e) {
            if (NetworkExceptionHelper.isCloseConnectionException(e)) {
                logger.debug(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            } else {
                logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
            }
            closeAndCallback(() -> listener.onFailure(e));
        }

        private void closeAndCallback(Runnable runnable) {
            Releasables.close(optionalReleasable, runnable::run);
        }
    }
}

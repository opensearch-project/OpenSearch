/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.network.CloseableChannel;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;

/**
 * Interface for outbound message handler. Can be implemented for different transport protocols.
 *
 * @opensearch.internal
 */
public interface OutboundMessageHandler {

    static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    void sendMessage(TcpChannel channel, ProtocolOutboundMessage networkMessage, ActionListener<Void> listener) throws IOException;

    ProtocolOutboundMessage convertRequestToOutboundMessage(
        final TransportRequest request,
        final String action,
        final long requestId,
        final boolean isHandshake,
        final boolean compressRequest,
        final Version channelVersion
    );

    ProtocolOutboundMessage convertResponseToOutboundMessage(
        final TransportResponse response,
        final Set<String> features,
        final long requestId,
        final boolean isHandshake,
        final boolean compressRequest,
        final Version channelVersion
    );

    ProtocolOutboundMessage convertErrorResponseToOutboundMessage(
        final RemoteTransportException tx,
        final Set<String> features,
        final long requestId,
        final boolean isHandshake,
        final boolean compress,
        final Version channelVersion
    );

    public default void internalSend(TcpChannel channel, SendContext sendContext, ThreadPool threadPool) throws IOException {
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

    /**
     * Context for sending a message.
     */
    public class SendContext extends NotifyOnceListener<Void> implements CheckedSupplier<BytesReference, IOException> {

        private final TcpChannel channel;
        private final CheckedSupplier<BytesReference, IOException> messageSupplier;
        private final ActionListener<Void> listener;
        private final Releasable optionalReleasable;
        private final StatsTracker statsTracker;
        private long messageSize = -1;

        public SendContext(
            TcpChannel channel,
            CheckedSupplier<BytesReference, IOException> messageSupplier,
            ActionListener<Void> listener,
            StatsTracker statsTracker
        ) {
            this(channel, messageSupplier, listener, null, statsTracker);
        }

        public SendContext(
            TcpChannel channel,
            CheckedSupplier<BytesReference, IOException> messageSupplier,
            ActionListener<Void> listener,
            Releasable optionalReleasable,
            StatsTracker statsTracker
        ) {
            this.channel = channel;
            this.messageSupplier = messageSupplier;
            this.listener = listener;
            this.optionalReleasable = optionalReleasable;
            this.statsTracker = statsTracker;
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

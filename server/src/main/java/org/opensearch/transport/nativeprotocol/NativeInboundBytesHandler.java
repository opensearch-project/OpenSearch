/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nativeprotocol;

import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.transport.Header;
import org.opensearch.transport.InboundAggregator;
import org.opensearch.transport.InboundBytesHandler;
import org.opensearch.transport.InboundDecoder;
import org.opensearch.transport.InboundMessage;
import org.opensearch.transport.ProtocolInboundMessage;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.TcpChannel;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.function.BiConsumer;

/**
 * Handler for inbound bytes for the native protocol.
 */
public class NativeInboundBytesHandler implements InboundBytesHandler {

    private static final ThreadLocal<ArrayList<Object>> fragmentList = ThreadLocal.withInitial(ArrayList::new);
    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final ArrayDeque<ReleasableBytesReference> pending;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private final StatsTracker statsTracker;
    private boolean isClosed = false;

    public NativeInboundBytesHandler(
        ArrayDeque<ReleasableBytesReference> pending,
        InboundDecoder decoder,
        InboundAggregator aggregator,
        StatsTracker statsTracker
    ) {
        this.pending = pending;
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.statsTracker = statsTracker;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean canHandleBytes(ReleasableBytesReference reference) {
        return true;
    }

    @Override
    public void doHandleBytes(
        TcpChannel channel,
        ReleasableBytesReference reference,
        BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler
    ) throws IOException {
        final ArrayList<Object> fragments = fragmentList.get();
        boolean continueHandling = true;

        while (continueHandling && isClosed == false) {
            boolean continueDecoding = true;
            while (continueDecoding && pending.isEmpty() == false) {
                try (ReleasableBytesReference toDecode = getPendingBytes()) {
                    final int bytesDecoded = decoder.decode(toDecode, fragments::add);
                    if (bytesDecoded != 0) {
                        releasePendingBytes(bytesDecoded);
                        if (fragments.isEmpty() == false && endOfMessage(fragments.get(fragments.size() - 1))) {
                            continueDecoding = false;
                        }
                    } else {
                        continueDecoding = false;
                    }
                }
            }

            if (fragments.isEmpty()) {
                continueHandling = false;
            } else {
                try {
                    forwardFragments(channel, fragments, messageHandler);
                } finally {
                    for (Object fragment : fragments) {
                        if (fragment instanceof ReleasableBytesReference) {
                            ((ReleasableBytesReference) fragment).close();
                        }
                    }
                    fragments.clear();
                }
            }
        }
    }

    private ReleasableBytesReference getPendingBytes() {
        if (pending.size() == 1) {
            return pending.peekFirst().retain();
        } else {
            final ReleasableBytesReference[] bytesReferences = new ReleasableBytesReference[pending.size()];
            int index = 0;
            for (ReleasableBytesReference pendingReference : pending) {
                bytesReferences[index] = pendingReference.retain();
                ++index;
            }
            final Releasable releasable = () -> Releasables.closeWhileHandlingException(bytesReferences);
            return new ReleasableBytesReference(CompositeBytesReference.of(bytesReferences), releasable);
        }
    }

    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            try (ReleasableBytesReference reference = pending.pollFirst()) {
                assert reference != null;
                if (bytesToRelease < reference.length()) {
                    pending.addFirst(reference.retainedSlice(bytesToRelease, reference.length() - bytesToRelease));
                    bytesToRelease -= bytesToRelease;
                } else {
                    bytesToRelease -= reference.length();
                }
            }
        }
    }

    private boolean endOfMessage(Object fragment) {
        return fragment == InboundDecoder.PING || fragment == InboundDecoder.END_CONTENT || fragment instanceof Exception;
    }

    private void forwardFragments(
        TcpChannel channel,
        ArrayList<Object> fragments,
        BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler
    ) throws IOException {
        for (Object fragment : fragments) {
            if (fragment instanceof Header) {
                assert aggregator.isAggregating() == false;
                aggregator.headerReceived((Header) fragment);
            } else if (fragment == InboundDecoder.PING) {
                assert aggregator.isAggregating() == false;
                messageHandler.accept(channel, PING_MESSAGE);
            } else if (fragment == InboundDecoder.END_CONTENT) {
                assert aggregator.isAggregating();
                try (InboundMessage aggregated = aggregator.finishAggregation()) {
                    statsTracker.markMessageReceived();
                    messageHandler.accept(channel, aggregated);
                }
            } else {
                assert aggregator.isAggregating();
                assert fragment instanceof ReleasableBytesReference;
                aggregator.aggregate((ReleasableBytesReference) fragment);
            }
        }
    }

}

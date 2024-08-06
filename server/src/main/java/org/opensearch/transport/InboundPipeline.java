/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.transport.nativeprotocol.NativeInboundBytesHandler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Pipeline for receiving inbound messages
 *
 * @opensearch.internal
 */
public class InboundPipeline implements Releasable {

    private final LongSupplier relativeTimeInMillis;
    private final StatsTracker statsTracker;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private Exception uncaughtException;
    private final ArrayDeque<ReleasableBytesReference> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;
    private final BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler;
    private final List<InboundBytesHandler> protocolBytesHandlers;
    private InboundBytesHandler currentHandler;

    public InboundPipeline(
        Version version,
        StatsTracker statsTracker,
        PageCacheRecycler recycler,
        LongSupplier relativeTimeInMillis,
        Supplier<CircuitBreaker> circuitBreaker,
        Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
        BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler
    ) {
        this(
            statsTracker,
            relativeTimeInMillis,
            new InboundDecoder(version, recycler),
            new InboundAggregator(circuitBreaker, registryFunction),
            messageHandler
        );
    }

    public InboundPipeline(
        StatsTracker statsTracker,
        LongSupplier relativeTimeInMillis,
        InboundDecoder decoder,
        InboundAggregator aggregator,
        BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler
    ) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.statsTracker = statsTracker;
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.protocolBytesHandlers = List.of(new NativeInboundBytesHandler(pending, decoder, aggregator, statsTracker));
        this.messageHandler = messageHandler;
    }

    @Override
    public void close() {
        isClosed = true;
        if (currentHandler != null) {
            currentHandler.close();
            currentHandler = null;
        }
        Releasables.closeWhileHandlingException(decoder, aggregator);
        Releasables.closeWhileHandlingException(pending);
        pending.clear();
    }

    public void handleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, reference);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    public void doHandleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        statsTracker.markBytesRead(reference.length());
        pending.add(reference.retain());

        // If we don't have a current handler, we should try to find one based on the protocol of the incoming bytes.
        if (currentHandler == null) {
            for (InboundBytesHandler handler : protocolBytesHandlers) {
                if (handler.canHandleBytes(reference)) {
                    currentHandler = handler;
                    break;
                }
            }
        }

        // If we have a current handler determined based on protocol, we should continue to use it for the fragmented bytes.
        if (currentHandler != null) {
            currentHandler.doHandleBytes(channel, reference, messageHandler);
        } else {
            throw new IllegalStateException("No bytes handler found for the incoming transport protocol");
        }
    }
}

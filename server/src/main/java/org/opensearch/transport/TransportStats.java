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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stats for transport activity
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TransportStats implements Writeable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOutboundConnections;
    private final long rxCount;
    private final long rxSize;
    private final long txCount;
    private final long txSize;
    // Socket closes that brought a connection down, keyed by channel type. Distinguishes impairment
    // affecting only some channel types from a whole-node failure. A socket carrying several types counts
    // against each of them, since its loss affects all of them, so the total across types is not the number
    // of sockets closed. Connections closed deliberately are not counted.
    private final Map<String, Long> channelCloseByType;
    // outgoingTimeouts: requests that got no response within their timeout.
    // requestsFailedOnDisconnect: in-flight requests cancelled when a connection closed.
    private final long outgoingTimeouts;
    private final long requestsFailedOnDisconnect;
    // Connect-time counters, covering the connection-open path that the request-level counters above
    // cannot observe:
    // connectFailures: outbound connection opens that failed or timed out.
    // connectTimeMillis: cumulative time spent on connection opens that succeeded, measured over the whole
    // open path including the transport handshake. Average open latency is connectTimeMillis /
    // totalOutboundConnections.
    // connectTimeMillisMax: slowest single connection open since node start. This is a high watermark that is
    // never reset, so on a long-lived node it reflects the worst event ever seen rather than recent behaviour.
    private final long connectFailures;
    private final long connectTimeMillis;
    private final long connectTimeMillisMax;

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new TransportStats object.
     * @param builder The builder instance containing all the values.
     */
    private TransportStats(Builder builder) {
        this.serverOpen = builder.serverOpen;
        this.totalOutboundConnections = builder.totalOutboundConnections;
        this.rxCount = builder.rxCount;
        this.rxSize = builder.rxSize;
        this.txCount = builder.txCount;
        this.txSize = builder.txSize;
        // Sorted so the rendered field order is stable, whether the counters were collected locally or read
        // off the wire from another node.
        this.channelCloseByType = Collections.unmodifiableMap(new TreeMap<>(builder.channelCloseByType));
        this.outgoingTimeouts = builder.outgoingTimeouts;
        this.requestsFailedOnDisconnect = builder.requestsFailedOnDisconnect;
        this.connectFailures = builder.connectFailures;
        this.connectTimeMillis = builder.connectTimeMillis;
        this.connectTimeMillisMax = builder.connectTimeMillisMax;
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public TransportStats(long serverOpen, long totalOutboundConnections, long rxCount, long rxSize, long txCount, long txSize) {
        this.serverOpen = serverOpen;
        this.totalOutboundConnections = totalOutboundConnections;
        this.rxCount = rxCount;
        this.rxSize = rxSize;
        this.txCount = txCount;
        this.txSize = txSize;
        this.channelCloseByType = Collections.emptyMap();
        this.outgoingTimeouts = 0L;
        this.requestsFailedOnDisconnect = 0L;
        this.connectFailures = 0L;
        this.connectTimeMillis = 0L;
        this.connectTimeMillisMax = 0L;
    }

    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOutboundConnections = in.readVLong();
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_3_8_0)) {
            channelCloseByType = in.readMap(StreamInput::readString, StreamInput::readVLong);
            outgoingTimeouts = in.readVLong();
            requestsFailedOnDisconnect = in.readVLong();
            connectFailures = in.readVLong();
            connectTimeMillis = in.readVLong();
            connectTimeMillisMax = in.readVLong();
        } else {
            channelCloseByType = Collections.emptyMap();
            outgoingTimeouts = 0L;
            requestsFailedOnDisconnect = 0L;
            connectFailures = 0L;
            connectTimeMillis = 0L;
            connectTimeMillisMax = 0L;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOutboundConnections);
        out.writeVLong(rxCount);
        out.writeVLong(rxSize);
        out.writeVLong(txCount);
        out.writeVLong(txSize);
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeMap(channelCloseByType, StreamOutput::writeString, StreamOutput::writeVLong);
            out.writeVLong(outgoingTimeouts);
            out.writeVLong(requestsFailedOnDisconnect);
            out.writeVLong(connectFailures);
            out.writeVLong(connectTimeMillis);
            out.writeVLong(connectTimeMillisMax);
        }
    }

    public long serverOpen() {
        return this.serverOpen;
    }

    public long getServerOpen() {
        return serverOpen();
    }

    public long getTotalOutboundConnections() {
        return totalOutboundConnections;
    }

    public long rxCount() {
        return rxCount;
    }

    public long getRxCount() {
        return rxCount();
    }

    public ByteSizeValue rxSize() {
        return new ByteSizeValue(rxSize);
    }

    public ByteSizeValue getRxSize() {
        return rxSize();
    }

    public long txCount() {
        return txCount;
    }

    public long getTxCount() {
        return txCount();
    }

    public ByteSizeValue txSize() {
        return new ByteSizeValue(txSize);
    }

    public ByteSizeValue getTxSize() {
        return txSize();
    }

    public Map<String, Long> getChannelCloseByType() {
        return channelCloseByType;
    }

    public long getOutgoingTimeouts() {
        return outgoingTimeouts;
    }

    public long getRequestsFailedOnDisconnect() {
        return requestsFailedOnDisconnect;
    }

    public long getConnectFailures() {
        return connectFailures;
    }

    public long getConnectTimeMillis() {
        return connectTimeMillis;
    }

    public long getConnectTimeMillisMax() {
        return connectTimeMillisMax;
    }

    /**
     * Builder for the {@link TransportStats} class.
     * Provides a fluent API for constructing a TransportStats object.
     */
    public static class Builder {
        private long serverOpen = 0;
        private long totalOutboundConnections = 0;
        private long rxCount = 0;
        private long rxSize = 0;
        private long txCount = 0;
        private long txSize = 0;
        private Map<String, Long> channelCloseByType = Collections.emptyMap();
        private long outgoingTimeouts = 0L;
        private long requestsFailedOnDisconnect = 0L;
        private long connectFailures = 0L;
        private long connectTimeMillis = 0L;
        private long connectTimeMillisMax = 0L;

        public Builder() {}

        /**
         * Seeds the builder from an existing {@link TransportStats}, so a caller that only contributes a
         * subset of the counters does not have to restate the rest.
         */
        public Builder(TransportStats base) {
            this.serverOpen = base.serverOpen;
            this.totalOutboundConnections = base.totalOutboundConnections;
            this.rxCount = base.rxCount;
            this.rxSize = base.rxSize;
            this.txCount = base.txCount;
            this.txSize = base.txSize;
            this.channelCloseByType = base.channelCloseByType;
            this.outgoingTimeouts = base.outgoingTimeouts;
            this.requestsFailedOnDisconnect = base.requestsFailedOnDisconnect;
            this.connectFailures = base.connectFailures;
            this.connectTimeMillis = base.connectTimeMillis;
            this.connectTimeMillisMax = base.connectTimeMillisMax;
        }

        public Builder serverOpen(long serverOpen) {
            this.serverOpen = serverOpen;
            return this;
        }

        public Builder totalOutboundConnections(long connections) {
            this.totalOutboundConnections = connections;
            return this;
        }

        public Builder rxCount(long count) {
            this.rxCount = count;
            return this;
        }

        public Builder rxSize(long size) {
            this.rxSize = size;
            return this;
        }

        public Builder txCount(long count) {
            this.txCount = count;
            return this;
        }

        public Builder txSize(long size) {
            this.txSize = size;
            return this;
        }

        public Builder channelCloseByType(Map<String, Long> channelCloseByType) {
            this.channelCloseByType = channelCloseByType;
            return this;
        }

        public Builder outgoingTimeouts(long outgoingTimeouts) {
            this.outgoingTimeouts = outgoingTimeouts;
            return this;
        }

        public Builder requestsFailedOnDisconnect(long requestsFailedOnDisconnect) {
            this.requestsFailedOnDisconnect = requestsFailedOnDisconnect;
            return this;
        }

        public Builder connectFailures(long connectFailures) {
            this.connectFailures = connectFailures;
            return this;
        }

        public Builder connectTimeMillis(long connectTimeMillis) {
            this.connectTimeMillis = connectTimeMillis;
            return this;
        }

        public Builder connectTimeMillisMax(long connectTimeMillisMax) {
            this.connectTimeMillisMax = connectTimeMillisMax;
            return this;
        }

        /**
         * Creates a {@link TransportStats} object from the builder's current state.
         * @return A new TransportStats instance.
         */
        public TransportStats build() {
            return new TransportStats(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.TRANSPORT);
        builder.field(Fields.SERVER_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OUTBOUND_CONNECTIONS, totalOutboundConnections);
        builder.field(Fields.RX_COUNT, rxCount);
        builder.humanReadableField(Fields.RX_SIZE_IN_BYTES, Fields.RX_SIZE, new ByteSizeValue(rxSize));
        builder.field(Fields.TX_COUNT, txCount);
        builder.humanReadableField(Fields.TX_SIZE_IN_BYTES, Fields.TX_SIZE, new ByteSizeValue(txSize));
        if (channelCloseByType.isEmpty() == false) {
            builder.field(Fields.CHANNEL_CLOSE_BY_TYPE, channelCloseByType);
        }
        builder.field(Fields.OUTGOING_TIMEOUTS, outgoingTimeouts);
        builder.field(Fields.REQUESTS_FAILED_ON_DISCONNECT, requestsFailedOnDisconnect);
        builder.field(Fields.CONNECT_FAILURES, connectFailures);
        builder.field(Fields.CONNECT_TIME_MILLIS, connectTimeMillis);
        builder.field(Fields.CONNECT_TIME_MILLIS_MAX, connectTimeMillisMax);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRANSPORT = "transport";
        static final String SERVER_OPEN = "server_open";
        static final String TOTAL_OUTBOUND_CONNECTIONS = "total_outbound_connections";
        static final String RX_COUNT = "rx_count";
        static final String RX_SIZE = "rx_size";
        static final String RX_SIZE_IN_BYTES = "rx_size_in_bytes";
        static final String TX_COUNT = "tx_count";
        static final String TX_SIZE = "tx_size";
        static final String TX_SIZE_IN_BYTES = "tx_size_in_bytes";
        static final String CHANNEL_CLOSE_BY_TYPE = "channel_close_by_type";
        static final String OUTGOING_TIMEOUTS = "outgoing_timeouts";
        static final String REQUESTS_FAILED_ON_DISCONNECT = "requests_failed_on_disconnect";
        static final String CONNECT_FAILURES = "connect_failures";
        static final String CONNECT_TIME_MILLIS = "connect_time_millis";
        static final String CONNECT_TIME_MILLIS_MAX = "connect_time_millis_max";
    }
}

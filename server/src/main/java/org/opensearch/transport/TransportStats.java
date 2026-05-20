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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

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
    }

    public TransportStats(StreamInput in) throws IOException {
        serverOpen = in.readVLong();
        totalOutboundConnections = in.readVLong();
        rxCount = in.readVLong();
        rxSize = in.readVLong();
        txCount = in.readVLong();
        txSize = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(serverOpen);
        out.writeVLong(totalOutboundConnections);
        out.writeVLong(rxCount);
        out.writeVLong(rxSize);
        out.writeVLong(txCount);
        out.writeVLong(txSize);
    }

    public long serverOpen() {
        return this.serverOpen;
    }

    public long getServerOpen() {
        return serverOpen();
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

        public Builder() {}

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
    }
}

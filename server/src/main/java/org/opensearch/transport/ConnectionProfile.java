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

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A connection profile describes how many connection are established to specific node for each of the available request types.
 * ({@link org.opensearch.transport.TransportRequestOptions.Type}). This allows to tailor a connection towards a specific usage.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ConnectionProfile {

    /**
     * takes a {@link ConnectionProfile} resolves it to a fully specified (i.e., no nulls) profile
     */
    public static ConnectionProfile resolveConnectionProfile(@Nullable ConnectionProfile profile, ConnectionProfile fallbackProfile) {
        Objects.requireNonNull(fallbackProfile);
        if (profile == null) {
            return fallbackProfile;
        } else if (profile.getConnectTimeout() != null
            && profile.getHandshakeTimeout() != null
            && profile.getPingInterval() != null
            && profile.getCompressionEnabled() != null) {
                return profile;
            } else {
                ConnectionProfile.Builder builder = new ConnectionProfile.Builder(profile);
                if (profile.getConnectTimeout() == null) {
                    builder.setConnectTimeout(fallbackProfile.getConnectTimeout());
                }
                if (profile.getHandshakeTimeout() == null) {
                    builder.setHandshakeTimeout(fallbackProfile.getHandshakeTimeout());
                }
                if (profile.getPingInterval() == null) {
                    builder.setPingInterval(fallbackProfile.getPingInterval());
                }
                if (profile.getCompressionEnabled() == null) {
                    builder.setCompressionEnabled(fallbackProfile.getCompressionEnabled());
                }
                return builder.build();
            }
    }

    /**
     * Builds a default connection profile based on the provided settings.
     *
     * @param settings to build the connection profile from
     * @return the connection profile
     */
    public static ConnectionProfile buildDefaultConnectionProfile(Settings settings) {
        int connectionsPerNodeRecovery = TransportSettings.CONNECTIONS_PER_NODE_RECOVERY.get(settings);
        int connectionsPerNodeBulk = TransportSettings.CONNECTIONS_PER_NODE_BULK.get(settings);
        int connectionsPerNodeReg = TransportSettings.CONNECTIONS_PER_NODE_REG.get(settings);
        int connectionsPerNodeState = TransportSettings.CONNECTIONS_PER_NODE_STATE.get(settings);
        int connectionsPerNodePing = TransportSettings.CONNECTIONS_PER_NODE_PING.get(settings);
        Builder builder = new Builder();
        builder.setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings));
        builder.setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings));
        builder.setPingInterval(TransportSettings.PING_SCHEDULE.get(settings));
        builder.setCompressionEnabled(TransportSettings.TRANSPORT_COMPRESS.get(settings));
        builder.addConnections(connectionsPerNodeBulk, TransportRequestOptions.Type.BULK);
        builder.addConnections(connectionsPerNodePing, TransportRequestOptions.Type.PING);
        // if we are not cluster-manager eligible we don't need a dedicated channel to publish the state
        builder.addConnections(
            DiscoveryNode.isClusterManagerNode(settings) ? connectionsPerNodeState : 0,
            TransportRequestOptions.Type.STATE
        );
        // if we are not a data-node we don't need any dedicated channels for recovery
        builder.addConnections(DiscoveryNode.isDataNode(settings) ? connectionsPerNodeRecovery : 0, TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(connectionsPerNodeReg, TransportRequestOptions.Type.REG);
        // we build a single channel profile with only supported type as STREAM for stream transport defined in StreamTransportService
        builder.addConnections(0, TransportRequestOptions.Type.STREAM);
        return builder.build();
    }

    /**
     * Builds a connection profile that is dedicated to a single channel type. Use this
     * when opening single use connections
     */
    public static ConnectionProfile buildSingleChannelProfile(TransportRequestOptions.Type channelType) {
        return buildSingleChannelProfile(channelType, null, null, null, null);
    }

    /**
     * Builds a connection profile that is dedicated to a single channel type. Allows passing connection and
     * handshake timeouts and compression settings.
     */
    public static ConnectionProfile buildSingleChannelProfile(
        TransportRequestOptions.Type channelType,
        @Nullable TimeValue connectTimeout,
        @Nullable TimeValue handshakeTimeout,
        @Nullable TimeValue pingInterval,
        @Nullable Boolean compressionEnabled
    ) {
        Builder builder = new Builder();
        builder.addConnections(1, channelType);
        final EnumSet<TransportRequestOptions.Type> otherTypes = EnumSet.allOf(TransportRequestOptions.Type.class);
        otherTypes.remove(channelType);
        builder.addConnections(0, otherTypes.toArray(new TransportRequestOptions.Type[0]));
        if (connectTimeout != null) {
            builder.setConnectTimeout(connectTimeout);
        }
        if (handshakeTimeout != null) {
            builder.setHandshakeTimeout(handshakeTimeout);
        }
        if (pingInterval != null) {
            builder.setPingInterval(pingInterval);
        }
        if (compressionEnabled != null) {
            builder.setCompressionEnabled(compressionEnabled);
        }
        return builder.build();
    }

    private final List<ConnectionTypeHandle> handles;
    private final int numConnections;
    private final TimeValue connectTimeout;
    private final TimeValue handshakeTimeout;
    private final TimeValue pingInterval;
    private final Boolean compressionEnabled;

    private ConnectionProfile(
        List<ConnectionTypeHandle> handles,
        int numConnections,
        TimeValue connectTimeout,
        TimeValue handshakeTimeout,
        TimeValue pingInterval,
        Boolean compressionEnabled
    ) {
        this.handles = handles;
        this.numConnections = numConnections;
        this.connectTimeout = connectTimeout;
        this.handshakeTimeout = handshakeTimeout;
        this.pingInterval = pingInterval;
        this.compressionEnabled = compressionEnabled;
    }

    /**
     * A builder to build a new {@link ConnectionProfile}
     *
     * @opensearch.internal
     */
    public static class Builder {
        private final List<ConnectionTypeHandle> handles = new ArrayList<>();
        private final Set<TransportRequestOptions.Type> addedTypes = EnumSet.noneOf(TransportRequestOptions.Type.class);
        private int numConnections = 0;
        private TimeValue connectTimeout;
        private TimeValue handshakeTimeout;
        private Boolean compressionEnabled;
        private TimeValue pingInterval;

        /** create an empty builder */
        public Builder() {}

        /** copy constructor, using another profile as a base */
        public Builder(ConnectionProfile source) {
            handles.addAll(source.getHandles());
            numConnections = source.getNumConnections();
            handles.forEach(th -> addedTypes.addAll(th.types));
            connectTimeout = source.getConnectTimeout();
            handshakeTimeout = source.getHandshakeTimeout();
            compressionEnabled = source.getCompressionEnabled();
            pingInterval = source.getPingInterval();
        }

        /**
         * Sets a connect timeout for this connection profile
         */
        public Builder setConnectTimeout(TimeValue connectTimeout) {
            if (connectTimeout.millis() < 0) {
                throw new IllegalArgumentException("connectTimeout must be non-negative but was: " + connectTimeout);
            }
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets a handshake timeout for this connection profile
         */
        public Builder setHandshakeTimeout(TimeValue handshakeTimeout) {
            if (handshakeTimeout.millis() < 0) {
                throw new IllegalArgumentException("handshakeTimeout must be non-negative but was: " + handshakeTimeout);
            }
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        /**
         * Sets a ping interval for this connection profile
         */
        public Builder setPingInterval(TimeValue pingInterval) {
            this.pingInterval = pingInterval;
            return this;
        }

        /**
         * Sets compression enabled for this connection profile
         */
        public Builder setCompressionEnabled(boolean compressionEnabled) {
            this.compressionEnabled = compressionEnabled;
            return this;
        }

        /**
         * Adds a number of connections for one or more types. Each type can only be added once.
         * @param numConnections the number of connections to use in the pool for the given connection types
         * @param types a set of types that should share the given number of connections
         */
        public Builder addConnections(int numConnections, TransportRequestOptions.Type... types) {
            if (types == null || types.length == 0) {
                throw new IllegalArgumentException("types must not be null");
            }
            for (TransportRequestOptions.Type type : types) {
                if (addedTypes.contains(type)) {
                    throw new IllegalArgumentException("type [" + type + "] is already registered");
                }
            }
            addedTypes.addAll(Arrays.asList(types));
            handles.add(new ConnectionTypeHandle(this.numConnections, numConnections, EnumSet.copyOf(Arrays.asList(types))));
            this.numConnections += numConnections;
            return this;
        }

        /**
         * Creates a new {@link ConnectionProfile} based on the added connections.
         * @throws IllegalStateException if any of the {@link org.opensearch.transport.TransportRequestOptions.Type} enum is missing
         */
        public ConnectionProfile build() {
            EnumSet<TransportRequestOptions.Type> types = EnumSet.allOf(TransportRequestOptions.Type.class);
            types.removeAll(addedTypes);
            if (types.isEmpty() == false) {
                throw new IllegalStateException("not all types are added for this connection profile - missing types: " + types);
            }
            return new ConnectionProfile(
                Collections.unmodifiableList(handles),
                numConnections,
                connectTimeout,
                handshakeTimeout,
                pingInterval,
                compressionEnabled
            );
        }

    }

    /**
     * Returns the connect timeout or <code>null</code> if no explicit timeout is set on this profile.
     */
    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns the handshake timeout or <code>null</code> if no explicit timeout is set on this profile.
     */
    public TimeValue getHandshakeTimeout() {
        return handshakeTimeout;
    }

    /**
     * Returns the ping interval or <code>null</code> if no explicit ping interval is set on this profile.
     */
    public TimeValue getPingInterval() {
        return pingInterval;
    }

    /**
     * Returns boolean indicating if compression is enabled or <code>null</code> if no explicit compression
     * is set on this profile.
     */
    public Boolean getCompressionEnabled() {
        return compressionEnabled;
    }

    /**
     * Returns the total number of connections for this profile
     */
    public int getNumConnections() {
        return numConnections;
    }

    /**
     * Returns the number of connections per type for this profile. This might return a count that is shared with other types such
     * that the sum of all connections per type might be higher than {@link #getNumConnections()}. For instance if
     * {@link org.opensearch.transport.TransportRequestOptions.Type#BULK} shares connections with
     * {@link org.opensearch.transport.TransportRequestOptions.Type#REG} they will return both the same number of connections from
     * this method but the connections are not distinct.
     */
    public int getNumConnectionsPerType(TransportRequestOptions.Type type) {
        for (ConnectionTypeHandle handle : handles) {
            if (handle.getTypes().contains(type)) {
                return handle.length;
            }
        }
        throw new AssertionError("no handle found for type: " + type);
    }

    /**
     * Returns the type handles for this connection profile
     */
    List<ConnectionTypeHandle> getHandles() {
        return Collections.unmodifiableList(handles);
    }

    /**
     * Connection type handle encapsulates the logic which connection
     */
    static final class ConnectionTypeHandle {
        public final int length;
        public final int offset;
        private final Set<TransportRequestOptions.Type> types;
        private final AtomicInteger counter = new AtomicInteger();

        private ConnectionTypeHandle(int offset, int length, Set<TransportRequestOptions.Type> types) {
            this.length = length;
            this.offset = offset;
            this.types = types;
        }

        /**
         * Returns one of the channels out configured for this handle. The channel is selected in a round-robin
         * fashion.
         */
        <T> T getChannel(List<T> channels) {
            if (length == 0) {
                throw new IllegalStateException("can't select channel size is 0 for types: " + types);
            }
            assert channels.size() >= offset + length : "illegal size: " + channels.size() + " expected >= " + (offset + length);
            return channels.get(offset + Math.floorMod(counter.incrementAndGet(), length));
        }

        /**
         * Returns all types for this handle
         */
        Set<TransportRequestOptions.Type> getTypes() {
            return types;
        }
    }
}

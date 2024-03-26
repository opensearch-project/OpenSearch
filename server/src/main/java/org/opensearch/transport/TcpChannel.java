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
import org.opensearch.common.network.CloseableChannel;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;

import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * This is a tcp channel representing a single channel connection to another node. It is the base channel
 * abstraction used by the {@link TcpTransport} and {@link TransportService}. All tcp transport
 * implementations must return channels that adhere to the required method contracts.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface TcpChannel extends CloseableChannel {

    /**
     * Indicates if the channel is an inbound server channel.
     */
    boolean isServerChannel();

    /**
     * This returns the profile for this channel.
     */
    String getProfile();

    /**
     * Returns the local address for this channel.
     *
     * @return the local address of this channel.
     */
    InetSocketAddress getLocalAddress();

    /**
     * Returns the remote address for this channel. Can be null if channel does not have a remote address.
     *
     * @return the remote address of this channel.
     */
    InetSocketAddress getRemoteAddress();

    /**
     * Sends a tcp message to the channel. The listener will be executed once the send process has been
     * completed.
     *
     * @param reference to send to channel
     * @param listener to execute upon send completion
     */
    void sendMessage(BytesReference reference, ActionListener<Void> listener);

    /**
     * Adds a listener that will be executed when the channel is connected. If the channel is still
     * unconnected when this listener is added, the listener will be executed by the thread that eventually
     * finishes the channel connection. If the channel is already connected when the listener is added the
     * listener will immediately be executed by the thread that is attempting to add the listener.
     *
     * @param listener to be executed
     */
    void addConnectListener(ActionListener<Void> listener);

    /**
     * Returns stats about this channel
     */
    ChannelStats getChannelStats();

    /**
     * Returns the contextual property associated with this specific TCP channel (the
     * implementation of how such properties are managed depends on the the particular
     * transport engine).
     *
     * @param name the name of the property
     * @param clazz the expected type of the property
     *
     * @return the value of the property
     */
    default <T> Optional<T> get(String name, Class<T> clazz) {
        return Optional.empty();
    }

    /**
     * Channel statistics
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    class ChannelStats {

        private volatile long lastAccessedTime;

        public ChannelStats() {
            lastAccessedTime = TimeValue.nsecToMSec(System.nanoTime());
        }

        void markAccessed(long relativeMillisTime) {
            lastAccessedTime = relativeMillisTime;
        }

        long lastAccessedTime() {
            return lastAccessedTime;
        }
    }
}

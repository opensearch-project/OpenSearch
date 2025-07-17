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

package org.opensearch.http;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.network.CloseableChannel;
import org.opensearch.core.action.ActionListener;

import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * Represents an HTTP comms channel
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface HttpChannel extends CloseableChannel {
    /**
     * Notify HTTP channel that exception happens and the response may not be sent (for example, timeout)
     * @param ex the exception being raised
     */
    default void handleException(Exception ex) {}

    /**
     * Sends an http response to the channel. The listener will be executed once the send process has been
     * completed.
     *
     * @param response to send to channel
     * @param listener to execute upon send completion
     */
    void sendResponse(HttpResponse response, ActionListener<Void> listener);

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
     * Returns the contextual property associated with this specific HTTP channel (the
     * implementation of how such properties are managed depends on the particular
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
}

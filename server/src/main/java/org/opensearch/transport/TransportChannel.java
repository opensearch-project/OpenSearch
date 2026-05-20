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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.util.Optional;

/**
 * A transport channel allows to send a response to a request on the channel.
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public interface TransportChannel {

    Logger logger = LogManager.getLogger(TransportChannel.class);

    String getProfileName();

    String getChannelType();

    /**
     * Sends a batch of responses to the request that this channel is associated with.
     * Call {@link #completeStream()} on a successful completion.
     * For errors, use {@link #sendResponse(Exception)} and do not call {@link #completeStream()}
     * Do not use {@link #sendResponse} in conjunction with this method if you are sending a batch of responses.
     *
     * @param response the batch of responses to send
     * @throws StreamException with {@link StreamErrorCode#CANCELLED} if the stream has been canceled.
     * Do not call this method again or completeStream() once canceled.
     */
    @ExperimentalApi
    default void sendResponseBatch(TransportResponse response) {
        throw new UnsupportedOperationException();
    }

    /**
     * Call this method on a successful completion the streaming response.
     * Note: not calling this method on success will result in a memory leak
     */
    @ExperimentalApi
    default void completeStream() {
        throw new UnsupportedOperationException();
    }

    void sendResponse(TransportResponse response) throws IOException;

    void sendResponse(Exception exception) throws IOException;

    /**
     * Returns the version of the other party that this channel will send a response to.
     */
    default Version getVersion() {
        return Version.CURRENT;
    }

    /**
     * A helper method to send an exception and handle and log a subsequent exception
     */
    static void sendErrorResponse(TransportChannel channel, String actionName, TransportRequest request, Exception e) {
        try {
            channel.sendResponse(e);
        } catch (Exception sendException) {
            sendException.addSuppressed(e);
            logger.warn(
                () -> new ParameterizedMessage("Failed to send error response for action [{}] and request [{}]", actionName, request),
                sendException
            );
        }
    }

    /**
     * Returns the contextual property associated with this specific transport channel (the
     * implementation of how such properties are managed depends on the particular
     * transport engine).
     *
     * @param name the name of the property
     * @param clazz the expected type of the property
     *
     * @return the value of the property.
     */
    default <T> Optional<T> get(String name, Class<T> clazz) {
        return Optional.empty();
    }
}

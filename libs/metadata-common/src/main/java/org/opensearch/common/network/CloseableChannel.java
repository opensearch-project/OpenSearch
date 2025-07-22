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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.network;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Channel that can be closed
 *
 * @opensearch.internal
 */
public interface CloseableChannel extends Closeable {

    /**
     * Closes the channel. For most implementations, this will be be an asynchronous process. For this
     * reason, this method does not throw {@link java.io.IOException} There is no guarantee that the channel
     * will be closed when this method returns. Use the {@link #addCloseListener(ActionListener)} method
     * to implement logic that depends on knowing when the channel is closed.
     */
    @Override
    void close();

    /**
     * Adds a listener that will be executed when the channel is closed. If the channel is still open when
     * this listener is added, the listener will be executed by the thread that eventually closes the
     * channel. If the channel is already closed when the listener is added the listener will immediately be
     * executed by the thread that is attempting to add the listener.
     *
     * @param listener to be executed
     */
    void addCloseListener(ActionListener<Void> listener);

    /**
     * Indicates whether a channel is currently open
     *
     * @return boolean indicating if channel is open
     */
    boolean isOpen();

    /**
     * Closes the channel without blocking.
     *
     * @param channel to close
     */
    static <C extends CloseableChannel> void closeChannel(C channel) {
        closeChannel(channel, false);
    }

    /**
     * Closes the channel.
     *
     * @param channel  to close
     * @param blocking indicates if we should block on channel close
     */
    static <C extends CloseableChannel> void closeChannel(C channel, boolean blocking) {
        closeChannels(Collections.singletonList(channel), blocking);
    }

    /**
     * Closes the channels.
     *
     * @param channels to close
     * @param blocking indicates if we should block on channel close
     */
    static <C extends CloseableChannel> void closeChannels(List<C> channels, boolean blocking) {
        try {
            IOUtils.close(channels);
        } catch (IOException e) {
            // The CloseableChannel#close method does not throw IOException, so this should not occur.
            throw new AssertionError(e);
        }
        if (blocking) {
            ArrayList<ActionFuture<Void>> futures = new ArrayList<>(channels.size());
            for (final C channel : channels) {
                PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
                channel.addCloseListener(closeFuture);
                futures.add(closeFuture);
            }
            blockOnFutures(futures);
        }
    }

    static void blockOnFutures(List<ActionFuture<Void>> futures) {
        for (ActionFuture<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // Ignore as we are only interested in waiting for the close process to complete. Logging
                // close exceptions happens elsewhere.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

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
import org.opensearch.Version;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transport channel for tasks
 *
 * @opensearch.internal
 */
public class TaskTransportChannel implements TransportChannel {

    private static final Logger logger = LogManager.getLogger(TaskTransportChannel.class);

    private final TransportChannel channel;
    private final Releasable onTaskFinished;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean streamingUsed = new AtomicBoolean(false);

    TaskTransportChannel(TransportChannel channel, Releasable onTaskFinished) {
        this.channel = channel;
        this.onTaskFinished = onTaskFinished;
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public String getChannelType() {
        return channel.getChannelType();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            channel.sendResponse(response);
        } finally {
            if (!streamingUsed.get() && finished.compareAndSet(false, true)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Closing task from sendResponse (non-streaming mode)");
                }
                onTaskFinished.close();
            }
        }
    }

    @Override
    public void sendResponseBatch(TransportResponse response) {
        streamingUsed.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("sendResponseBatch called - marking as streaming mode, not closing task");
        }
        channel.sendResponseBatch(response);
    }

    @Override
    public void completeStream() {
        streamingUsed.set(true);
        try {
            channel.completeStream();
        } finally {
            if (streamingUsed.get() && finished.compareAndSet(false, true)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Closing task from completeStream (streaming mode)");
                }
                onTaskFinished.close();
            } else if (logger.isTraceEnabled()) {
                logger.trace("completeStream called but task already finished={}, streamingUsed={}", finished.get(), streamingUsed.get());
            }
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            channel.sendResponse(exception);
        } finally {
            if (finished.compareAndSet(false, true)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Closing task from sendResponse(Exception), streamingUsed={}", streamingUsed.get());
                }
                onTaskFinished.close();
            } else if (logger.isTraceEnabled()) {
                logger.trace("sendResponse(Exception) called but task already finished");
            }
        }
    }

    @Override
    public Version getVersion() {
        return channel.getVersion();
    }

    public TransportChannel getChannel() {
        return channel;
    }

    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        return getChannel().get(name, clazz);
    }
}

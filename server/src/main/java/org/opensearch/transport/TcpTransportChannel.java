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
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Channel for a TCP connection
 *
 * @opensearch.internal
 */
public final class TcpTransportChannel extends BaseTcpTransportChannel {

    private final AtomicBoolean released = new AtomicBoolean();
    private final ProtocolOutboundHandler outboundHandler;
    private final String action;
    private final long requestId;
    private final Version version;
    private final Set<String> features;
    private final boolean compressResponse;
    private final boolean isHandshake;
    private final Releasable breakerRelease;

    TcpTransportChannel(
        ProtocolOutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        Version version,
        Set<String> features,
        boolean compressResponse,
        boolean isHandshake,
        Releasable breakerRelease
    ) {
        super(channel);
        this.version = version;
        this.features = features;
        this.outboundHandler = outboundHandler;
        this.action = action;
        this.requestId = requestId;
        this.compressResponse = compressResponse;
        this.isHandshake = isHandshake;
        this.breakerRelease = breakerRelease;
    }

    @Override
    public String getProfileName() {
        return getChannel().getProfile();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            if (response instanceof QuerySearchResult && ((QuerySearchResult) response).getShardSearchRequest() != null) {
                // update outbound network time with current time before sending response over network
                ((QuerySearchResult) response).getShardSearchRequest().setOutboundNetworkTime(System.currentTimeMillis());
            }
            outboundHandler.sendResponse(version, features, getChannel(), requestId, action, response, compressResponse, isHandshake);
        } finally {
            release(false);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            outboundHandler.sendErrorResponse(version, features, getChannel(), requestId, action, exception);
        } finally {
            release(true);
        }
    }

    private Exception releaseBy;

    private void release(boolean isExceptionResponse) {
        if (released.compareAndSet(false, true)) {
            assert (releaseBy = new Exception()) != null; // easier to debug if it's already closed
            breakerRelease.close();
        } else if (isExceptionResponse == false) {
            // only fail if we are not sending an error - we might send the error triggered by the previous
            // sendResponse call
            throw new IllegalStateException("reserved bytes are already released", releaseBy);
        }
    }

    @Override
    public String getChannelType() {
        return "transport";
    }

    @Override
    public Version getVersion() {
        return version;
    }

    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        return getChannel().get(name, clazz);
    }
}

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

package org.opensearch.grpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.common.network.NetworkService.resolvePublishPort;

import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_BIND_HOST;
import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_MAX_CONTENT_LENGTH;
import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_PORT;
import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_PUBLISH_HOST;
import static org.opensearch.grpc.GrpcTransportSettings.SETTING_GRPC_PUBLISH_PORT;

/**
 * Base GrpcServer class
 *
 * @opensearch.internal
 */
public abstract class AbstractGrpcServerTransport extends AbstractLifecycleComponent implements GrpcServerTransport {
    private static final Logger logger = LogManager.getLogger(AbstractGrpcServerTransport.class);

    private volatile BoundTransportAddress boundAddress;

    private final String[] bindHosts;
    private final String[] publishHosts;
    private final Settings settings;
    private final NetworkService networkService;

    protected final PortsRange port;
    protected final ByteSizeValue maxContentLength;

    protected AbstractGrpcServerTransport(
        Settings settings,
        NetworkService networkService
    ) {
        this.settings = settings;
        this.networkService = networkService;

        List<String> httpBindHost = SETTING_GRPC_BIND_HOST.get(settings);
        this.bindHosts = (httpBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : httpBindHost).toArray(
            Strings.EMPTY_ARRAY
        );

        List<String> httpPublishHost = SETTING_GRPC_PUBLISH_HOST.get(settings);
        this.publishHosts = (httpPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings) : httpPublishHost)
            .toArray(Strings.EMPTY_ARRAY);

        this.port = SETTING_GRPC_PORT.get(settings);
        this.maxContentLength = SETTING_GRPC_MAX_CONTENT_LENGTH.get(settings);
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public GrpcInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new GrpcInfo(boundTransportAddress, maxContentLength.getBytes());
    }

    @Override
    public GrpcStats stats() {
        return new GrpcStats();
    }

    // gRPC service definitions provided at bind
    abstract protected TransportAddress bindAddress(InetAddress hostAddress, PortsRange portRange);

    // TODO: IDENTICAL TO - HTTP SERVER TRANSPORT
    protected void bindServer() {
        InetAddress hostAddresses[];
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<TransportAddress> boundAddresses = new ArrayList<>(hostAddresses.length);
        for (InetAddress address : hostAddresses) {
            boundAddresses.add(bindAddress(address, port));
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolvePublishPort(SETTING_GRPC_PUBLISH_PORT.get(settings), boundAddresses, publishInetAddress);
        if (publishPort < 0) {
            throw new BindTransportException(
                "Failed to auto-resolve http publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). "
                    + "Please specify a unique port by setting "
                    + SETTING_GRPC_PORT.getKey()
                    + " or "
                    + SETTING_GRPC_PUBLISH_PORT.getKey()
            );
        }



        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), publishAddress);
        logger.info("{}", boundAddress);
    }
}

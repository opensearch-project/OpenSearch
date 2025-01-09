/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.PortsRange;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.common.settings.Setting.intSetting;
import static org.opensearch.common.settings.Setting.listSetting;
import static org.opensearch.transport.Transport.resolveTransportPublishPort;

public class FlightServerTransport extends NetworkPlugin.AuxTransport {

    public static final String FLIGHT_TRANSPORT_SETTING_KEY = "transport-flight";

    public static final Setting<PortsRange> SETTING_FLIGHT_PORTS = AUX_TRANSPORT_PORTS.getConcreteSettingForNamespace(
        FLIGHT_TRANSPORT_SETTING_KEY
    );

    public static final Setting<List<String>> SETTING_FLIGHT_HOST = listSetting(
        "flight.host",
        emptyList(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_FLIGHT_BIND_HOST = listSetting(
        "flight.bind_host",
        SETTING_FLIGHT_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> SETTING_FLIGHT_PUBLISH_HOST = listSetting(
        "flight.publish_host",
        SETTING_FLIGHT_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> SETTING_FLIGHT_PUBLISH_PORT = intSetting(
        "flight.publish_port",
        -1,
        -1,
        Setting.Property.NodeScope
    );

    private final Settings settings;
    private final NetworkService networkService;
    private final PortsRange port;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private volatile BoundTransportAddress boundAddress;
    private final Function<TransportAddress, Boolean> startServerCallback;

    public FlightServerTransport(
        Settings settings,
        NetworkService networkService,
        Function<TransportAddress, Boolean> startServerCallback
    ) {
        this.settings = settings;
        this.networkService = networkService;
        this.port = SETTING_FLIGHT_PORTS.get(settings);

        List<String> bindHosts = SETTING_FLIGHT_BIND_HOST.get(settings);
        this.bindHosts = bindHosts.toArray(new String[0]);

        List<String> publishHosts = SETTING_FLIGHT_PUBLISH_HOST.get(settings);
        this.publishHosts = publishHosts.toArray(new String[0]);
        this.startServerCallback = startServerCallback;
    }

    @Override
    protected void doStart() {
        InetAddress[] hostAddresses;
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

        final int publishPort = resolveTransportPublishPort(SETTING_FLIGHT_PUBLISH_PORT.get(settings), boundAddresses, publishInetAddress);

        if (publishPort < 0) {
            throw new BindTransportException(
                "Failed to auto-resolve flight publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). Please specify a unique port by setting "
                    + SETTING_FLIGHT_PUBLISH_PORT.getKey()
            );
        }

        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), publishAddress);
    }

    public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

    private TransportAddress bindAddress(final InetAddress hostAddress, final PortsRange portsRange) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        final TransportAddress[] address = new TransportAddress[1];
        boolean success = portsRange.iterate(portNumber -> {
            boundSocket.set(new InetSocketAddress(hostAddress, portNumber));
            address[0] = new TransportAddress(boundSocket.get());
            try {
                return startServerCallback.apply(address[0]);
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
        });

        if (!success) {
            throw new BindTransportException("Failed to bind to [" + hostAddress + "]", lastException.get());
        }

        return address[0];
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

}

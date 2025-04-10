/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc;

import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.util.List;

import io.grpc.BindableService;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class Netty4GrpcServerTransportTests extends OpenSearchTestCase {

    private NetworkService networkService;
    private List<BindableService> services;

    @Before
    public void setup() {
        networkService = new NetworkService(List.of());
        services = List.of();
    }

    public void testBasicStartAndStop() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.boundAddress().publishAddress().address());

            transport.stop();
        }
    }

    public void testWithCustomPort() {
        // Create settings with a specific port
        Settings settings = Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), "9000-9010").build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.boundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertTrue("Port should be in the specified range", publishAddress.getPort() >= 9000 && publishAddress.getPort() <= 9010);

            transport.stop();
        }
    }

    public void testWithCustomPublishPort() {
        // Create settings with a specific publish port
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT.getKey(), 9000)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.boundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertEquals("Publish port should match the specified value", 9000, publishAddress.getPort());

            transport.stop();
        }
    }

    public void testWithCustomHost() {
        // Create settings with a specific host
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_HOST.getKey(), "127.0.0.1")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.boundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertEquals(
                "Host should match the specified value",
                "127.0.0.1",
                InetAddresses.toAddrString(publishAddress.address().getAddress())
            );

            transport.stop();
        }
    }

    public void testWithCustomBindHost() {
        // Create settings with a specific bind host
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST.getKey(), "127.0.0.1")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress boundAddress = transport.boundAddress().boundAddresses()[0];
            assertNotNull(boundAddress.address());
            assertEquals(
                "Bind host should match the specified value",
                "127.0.0.1",
                InetAddresses.toAddrString(boundAddress.address().getAddress())
            );

            transport.stop();
        }
    }

    public void testWithCustomPublishHost() {
        // Create settings with a specific publish host
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST.getKey(), "127.0.0.1")
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            TransportAddress publishAddress = transport.boundAddress().publishAddress();
            assertNotNull(publishAddress.address());
            assertEquals(
                "Publish host should match the specified value",
                "127.0.0.1",
                InetAddresses.toAddrString(publishAddress.address().getAddress())
            );

            transport.stop();
        }
    }

    public void testWithCustomWorkerCount() {
        // Create settings with a specific worker count
        Settings settings = Settings.builder()
            .put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange())
            .put(Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT.getKey(), 4)
            .build();

        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(settings, services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.boundAddress().publishAddress().address());

            transport.stop();
        }
    }

    private static Settings createSettings() {
        return Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORT.getKey(), OpenSearchTestCase.getPortRange()).build();
    }
}

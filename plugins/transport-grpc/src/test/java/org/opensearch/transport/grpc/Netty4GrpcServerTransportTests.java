/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
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

    public void test() {
        try (Netty4GrpcServerTransport transport = new Netty4GrpcServerTransport(createSettings(), services, networkService)) {
            transport.start();

            MatcherAssert.assertThat(transport.boundAddress().boundAddresses(), not(emptyArray()));
            assertNotNull(transport.boundAddress().publishAddress().address());

            transport.stop();
        }
    }

    private static Settings createSettings() {
        return Settings.builder().put(Netty4GrpcServerTransport.SETTING_GRPC_PORTS.getKey(), getPortRange()).build();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.transport.grpc.ssl.NettyGrpcClient;

import io.grpc.health.v1.HealthCheckResponse;

/**
 * Integration tests for the gRPC transport itself.
 */
public class Netty4GrpcServerTransportIT extends GrpcTransportBaseIT {

    /**
     * Tests that the gRPC transport is properly started.
     */
    public void testGrpcTransportStarted() {
        verifyGrpcTransportStarted();
    }

    /**
     * Tests the health of the gRPC transport service.
     */
    public void testGrpcTransportHealth() throws Exception {
        checkGrpcTransportHealth();
    }

    /**
     * Tests both REST API cluster health and gRPC transport service health.
     */
    public void testStartGrpcTransportClusterHealth() throws Exception {
        // REST api cluster health
        ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().get();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());

        // gRPC transport service health
        try (NettyGrpcClient client = createGrpcClient()) {
            assertEquals(client.checkHealth(), HealthCheckResponse.ServingStatus.SERVING);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.OSFlightClient;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 5)
public class ArrowFlightServerIT extends OpenSearchIntegTestCase {

    private FlightClientManager flightClientManager;

    @BeforeClass
    public static void setupFeatureFlags() {
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ensureGreen();
        Thread.sleep(1000);
        FlightService flightService = internalCluster().getInstance(FlightService.class);
        flightClientManager = flightService.getFlightClientManager();
    }

    public void testArrowFlightEndpoint() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            try (OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId())) {
                assertNotNull(flightClient);
                flightClient.handshake(CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
            }
        }
    }
}

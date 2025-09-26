/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.arrow.flight.api.flightinfo.FlightServerInfoAction;
import org.opensearch.arrow.flight.api.flightinfo.NodesFlightInfoAction;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.arrow.flight.stats.FlightStatsAction;
import org.opensearch.arrow.flight.stats.FlightStatsRestHandler;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.AuxTransport;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.opensearch.arrow.flight.bootstrap.FlightService.ARROW_FLIGHT_TRANSPORT_SETTING_KEY;
import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS;
import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightStreamPluginTests extends OpenSearchTestCase {
    private Settings settings;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("flight.ssl.enable", true).build();
        clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(nodes);
        when(nodes.getLocalNodeId()).thenReturn("test-node");
    }

    @LockFeatureFlag(ARROW_STREAMS)
    public void testPluginEnabledWithStreamManagerApproach() throws IOException {
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        plugin.createComponents(null, clusterService, mock(ThreadPool.class), null, null, null, null, null, null, null, null);
        Map<String, Supplier<AuxTransport>> aux_map = plugin.getAuxTransports(
            settings,
            mock(ThreadPool.class),
            null,
            new NetworkService(List.of()),
            null,
            null
        );

        AuxTransport transport = aux_map.get(ARROW_FLIGHT_TRANSPORT_SETTING_KEY).get();
        assertNotNull(transport);
        assertTrue(transport instanceof FlightService);

        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());
        assertEquals(3, executorBuilders.size());

        Optional<StreamManager> streamManager = plugin.getStreamManager();
        assertTrue(streamManager.isPresent());

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        assertTrue(
            plugin.getAuxTransports(null, null, null, new NetworkService(List.of()), null, null)
                .get(ARROW_FLIGHT_TRANSPORT_SETTING_KEY)
                .get() instanceof FlightService
        );
        assertEquals(1, plugin.getRestHandlers(null, null, null, null, null, null, null).size());
        assertTrue(plugin.getRestHandlers(null, null, null, null, null, null, null).get(0) instanceof FlightServerInfoAction);

        assertEquals(1, plugin.getActions().size());
        assertEquals(NodesFlightInfoAction.INSTANCE.name(), plugin.getActions().get(0).getAction().name());

        plugin.close();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testPluginEnabledStreamTransportApproach() throws IOException {
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        plugin.createComponents(null, clusterService, mock(ThreadPool.class), null, null, null, null, null, null, null, null);
        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());
        assertEquals(3, executorBuilders.size());

        Optional<StreamManager> streamManager = plugin.getStreamManager();
        assertTrue(streamManager.isEmpty());

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        assertFalse(
            plugin.getSecureTransports(null, null, null, null, null, null, mock(SecureTransportSettingsProvider.class), null).isEmpty()
        );

        assertEquals(1, plugin.getRestHandlers(null, null, null, null, null, null, null).size());
        assertTrue(plugin.getRestHandlers(null, null, null, null, null, null, null).get(0) instanceof FlightStatsRestHandler);

        assertEquals(1, plugin.getActions().size());
        assertEquals(FlightStatsAction.INSTANCE.name(), plugin.getActions().get(0).getAction().name());

        plugin.close();
    }

    public void testBothDisabled() throws IOException {
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        plugin.createComponents(null, clusterService, mock(ThreadPool.class), null, null, null, null, null, null, null, null);

        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertTrue(executorBuilders.isEmpty());

        Optional<StreamManager> streamManager = plugin.getStreamManager();
        assertTrue(streamManager.isEmpty());

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertTrue(settings.isEmpty());

        assertTrue(
            plugin.getSecureTransports(null, null, null, null, null, null, mock(SecureTransportSettingsProvider.class), null).isEmpty()
        );

        assertEquals(0, plugin.getRestHandlers(null, null, null, null, null, null, null).size());

        assertEquals(0, plugin.getActions().size());
        plugin.close();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.opensearch.arrow.flight.api.FlightServerInfoAction;
import org.opensearch.arrow.flight.api.NodesFlightInfoAction;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS_SETTING;
import static org.opensearch.plugins.NetworkPlugin.AuxTransport.AUX_TRANSPORT_TYPES_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightStreamPluginTests extends OpenSearchTestCase {
    private Settings settings;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put("node.attr.transport.stream.port", "9880").put(ARROW_STREAMS_SETTING.getKey(), true).build();
        clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(nodes);
        when(nodes.getLocalNodeId()).thenReturn("test-node");
    }

    public void testPluginEnableAndDisable() throws IOException {

        Settings disabledSettings = Settings.builder()
            .put("node.attr.transport.stream.port", "9880")
            .put(ARROW_STREAMS_SETTING.getKey(), false)
            .build();
        FeatureFlags.initializeFeatureFlags(disabledSettings);
        FlightStreamPlugin disabledPlugin = new FlightStreamPlugin(disabledSettings);

        Collection<Object> disabledPluginComponents = disabledPlugin.createComponents(
            null,
            clusterService,
            mock(ThreadPool.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertTrue(disabledPluginComponents.isEmpty());
        assertNull(disabledPlugin.getStreamManager().get());
        assertTrue(disabledPlugin.getExecutorBuilders(disabledSettings).isEmpty());
        assertNotNull(disabledPlugin.getSettings());
        assertTrue(disabledPlugin.getSettings().isEmpty());
        assertNotNull(disabledPlugin.getSecureTransports(null, null, null, null, null, null, null, null));
        assertTrue(disabledPlugin.getAuxTransports(null, null, null, null, null, null).isEmpty());
        assertEquals(0, disabledPlugin.getRestHandlers(null, null, null, null, null, null, null).size());
        assertEquals(0, disabledPlugin.getActions().size());

        disabledPlugin.close();

        FeatureFlags.initializeFeatureFlags(settings);
        FeatureFlagSetter.set(ARROW_STREAMS_SETTING.getKey());
        FlightStreamPlugin plugin = new FlightStreamPlugin(settings);
        Collection<Object> components = plugin.createComponents(
            null,
            clusterService,
            mock(ThreadPool.class),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertNotNull(components);
        assertFalse(components.isEmpty());
        assertEquals(1, components.size());
        assertTrue(components.iterator().next() instanceof FlightService);

        List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
        assertNotNull(executorBuilders);
        assertFalse(executorBuilders.isEmpty());
        assertEquals(2, executorBuilders.size());

        Supplier<StreamManager> streamManager = plugin.getStreamManager();
        assertNotNull(streamManager);

        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertFalse(settings.isEmpty());

        assertNotNull(plugin.getSecureTransports(null, null, null, null, null, null, mock(SecureTransportSettingsProvider.class), null));

        assertTrue(
            plugin.getAuxTransports(null, null, null, new NetworkService(List.of()), null, null)
                .get(AUX_TRANSPORT_TYPES_KEY)
                .get() instanceof FlightService
        );
        assertEquals(1, plugin.getRestHandlers(null, null, null, null, null, null, null).size());
        assertTrue(plugin.getRestHandlers(null, null, null, null, null, null, null).get(0) instanceof FlightServerInfoAction);
        assertEquals(1, plugin.getActions().size());
        assertEquals(NodesFlightInfoAction.INSTANCE.name(), plugin.getActions().get(0).getAction().name());

        plugin.close();
    }
}

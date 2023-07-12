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

package org.opensearch.discovery;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiscoveryModuleTests extends OpenSearchTestCase {

    private TransportService transportService;
    private NamedWriteableRegistry namedWriteableRegistry;
    private ClusterManagerService clusterManagerService;
    private ClusterApplier clusterApplier;
    private ThreadPool threadPool;
    private ClusterSettings clusterSettings;
    private GatewayMetaState gatewayMetaState;

    public interface DummyHostsProviderPlugin extends DiscoveryPlugin {
        Map<String, Supplier<SeedHostsProvider>> impl();

        @Override
        default Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(
            TransportService transportService,
            NetworkService networkService
        ) {
            return impl();
        }
    }

    @Before
    public void setupDummyServices() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, null);
        clusterManagerService = mock(ClusterManagerService.class);
        namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        clusterApplier = mock(ClusterApplier.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        gatewayMetaState = mock(GatewayMetaState.class);
    }

    @After
    public void clearDummyServices() throws IOException {
        IOUtils.close(transportService);
    }

    private DiscoveryModule newModule(Settings settings, List<DiscoveryPlugin> plugins) {
        return new DiscoveryModule(
            settings,
            threadPool,
            transportService,
            namedWriteableRegistry,
            null,
            clusterManagerService,
            clusterApplier,
            clusterSettings,
            plugins,
            null,
            createTempDir().toAbsolutePath(),
            gatewayMetaState,
            mock(RerouteService.class),
            null
        );
    }

    public void testDefaults() {
        DiscoveryModule module = newModule(Settings.EMPTY, Collections.emptyList());
        assertTrue(module.getDiscovery() instanceof Coordinator);
    }

    public void testUnknownDiscovery() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newModule(settings, Collections.emptyList()));
        assertEquals("Unknown discovery type [dne]", e.getMessage());
    }

    public void testSeedProviders() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "custom").build();
        AtomicBoolean created = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("custom", () -> {
            created.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        newModule(settings, Collections.singletonList(plugin));
        assertTrue(created.get());
    }

    public void testLegacyHostsProvider() {
        Settings settings = Settings.builder().put(DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "custom").build();
        AtomicBoolean created = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("custom", () -> {
            created.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        newModule(settings, Collections.singletonList(plugin));
        assertTrue(created.get());
        assertWarnings(
            "[discovery.zen.hosts_provider] setting was deprecated in OpenSearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    public void testLegacyAndNonLegacyProvidersRejected() {
        Settings settings = Settings.builder()
            .putList(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey())
            .putList(DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.getKey())
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newModule(settings, Collections.emptyList()));
        assertEquals("it is forbidden to set both [discovery.seed_providers] and [discovery.zen.hosts_provider]", e.getMessage());
    }

    public void testUnknownSeedsProvider() {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "dne").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newModule(settings, Collections.emptyList()));
        assertEquals("Unknown seed providers [dne]", e.getMessage());
    }

    public void testDuplicateSeedsProvider() {
        DummyHostsProviderPlugin plugin1 = () -> Collections.singletonMap("dup", () -> null);
        DummyHostsProviderPlugin plugin2 = () -> Collections.singletonMap("dup", () -> null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> newModule(Settings.EMPTY, Arrays.asList(plugin1, plugin2))
        );
        assertEquals("Cannot register seed provider [dup] twice", e.getMessage());
    }

    public void testSettingsSeedsProvider() {
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("settings", () -> null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newModule(Settings.EMPTY, Arrays.asList(plugin)));
        assertEquals("Cannot register seed provider [settings] twice", e.getMessage());
    }

    public void testMultipleSeedsProviders() {
        AtomicBoolean created1 = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin1 = () -> Collections.singletonMap("provider1", () -> {
            created1.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        AtomicBoolean created2 = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin2 = () -> Collections.singletonMap("provider2", () -> {
            created2.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        AtomicBoolean created3 = new AtomicBoolean(false);
        DummyHostsProviderPlugin plugin3 = () -> Collections.singletonMap("provider3", () -> {
            created3.set(true);
            return hostsResolver -> Collections.emptyList();
        });
        Settings settings = Settings.builder()
            .putList(DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "provider1", "provider3")
            .build();
        newModule(settings, Arrays.asList(plugin1, plugin2, plugin3));
        assertTrue(created1.get());
        assertFalse(created2.get());
        assertTrue(created3.get());
    }

    public void testLazyConstructionSeedsProvider() {
        DummyHostsProviderPlugin plugin = () -> Collections.singletonMap("custom", () -> {
            throw new AssertionError("created hosts provider which was not selected");
        });
        newModule(Settings.EMPTY, Collections.singletonList(plugin));
    }

    public void testJoinValidator() {
        BiConsumer<DiscoveryNode, ClusterState> consumer = (a, b) -> {};
        DiscoveryModule module = newModule(
            Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.ZEN2_DISCOVERY_TYPE).build(),
            Collections.singletonList(new DiscoveryPlugin() {
                @Override
                public BiConsumer<DiscoveryNode, ClusterState> getJoinValidator() {
                    return consumer;
                }
            })
        );
        Coordinator discovery = (Coordinator) module.getDiscovery();
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators = discovery.getOnJoinValidators();
        assertEquals(2, onJoinValidators.size());
        assertTrue(onJoinValidators.contains(consumer));
    }
}

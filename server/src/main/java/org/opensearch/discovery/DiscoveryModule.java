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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.coordination.ElectionStrategy;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.common.Randomness;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.monitor.NodeHealthService;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.node.Node.NODE_NAME_SETTING;

/**
 * A module for loading classes for node discovery.
 *
 * @opensearch.internal
 */
public class DiscoveryModule {
    private static final Logger logger = LogManager.getLogger(DiscoveryModule.class);

    public static final String ZEN2_DISCOVERY_TYPE = "zen";
    public static final String SINGLE_NODE_DISCOVERY_TYPE = "single-node";

    public static final Setting<String> DISCOVERY_TYPE_SETTING = new Setting<>(
        "discovery.type",
        ZEN2_DISCOVERY_TYPE,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<List<String>> LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING = Setting.listSetting(
        "discovery.zen.hosts_provider",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<List<String>> DISCOVERY_SEED_PROVIDERS_SETTING = Setting.listSetting(
        "discovery.seed_providers",
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public static final String DEFAULT_ELECTION_STRATEGY = "default";

    public static final Setting<String> ELECTION_STRATEGY_SETTING = new Setting<>(
        "cluster.election.strategy",
        DEFAULT_ELECTION_STRATEGY,
        Function.identity(),
        Property.NodeScope
    );

    private final Discovery discovery;

    public DiscoveryModule(
        Settings settings,
        ThreadPool threadPool,
        TransportService transportService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        ClusterManagerService clusterManagerService,
        ClusterApplier clusterApplier,
        ClusterSettings clusterSettings,
        List<DiscoveryPlugin> plugins,
        AllocationService allocationService,
        Path configFile,
        GatewayMetaState gatewayMetaState,
        RerouteService rerouteService,
        NodeHealthService nodeHealthService,
        PersistedStateRegistry persistedStateRegistry,
        RemoteStoreNodeService remoteStoreNodeService,
        ClusterManagerMetrics clusterManagerMetrics,
        RemoteClusterStateService remoteClusterStateService
    ) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators = new ArrayList<>();
        final Map<String, Supplier<SeedHostsProvider>> hostProviders = new HashMap<>();
        hostProviders.put("settings", () -> new SettingsBasedSeedHostsProvider(settings, transportService));
        hostProviders.put("file", () -> new FileBasedSeedHostsProvider(configFile));
        final Map<String, ElectionStrategy> electionStrategies = new HashMap<>();
        electionStrategies.put(DEFAULT_ELECTION_STRATEGY, ElectionStrategy.DEFAULT_INSTANCE);
        for (DiscoveryPlugin plugin : plugins) {
            plugin.getSeedHostProviders(transportService, networkService).forEach((key, value) -> {
                if (hostProviders.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register seed provider [" + key + "] twice");
                }
            });
            BiConsumer<DiscoveryNode, ClusterState> joinValidator = plugin.getJoinValidator();
            if (joinValidator != null) {
                joinValidators.add(joinValidator);
            }
            plugin.getElectionStrategies().forEach((key, value) -> {
                if (electionStrategies.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register election strategy [" + key + "] twice");
                }
            });
        }

        List<String> seedProviderNames = getSeedProviderNames(settings);
        // for bwc purposes, add settings provider even if not explicitly specified
        if (seedProviderNames.contains("settings") == false) {
            List<String> extendedSeedProviderNames = new ArrayList<>();
            extendedSeedProviderNames.add("settings");
            extendedSeedProviderNames.addAll(seedProviderNames);
            seedProviderNames = extendedSeedProviderNames;
        }

        final Set<String> missingProviderNames = new HashSet<>(seedProviderNames);
        missingProviderNames.removeAll(hostProviders.keySet());
        if (missingProviderNames.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown seed providers " + missingProviderNames);
        }

        List<SeedHostsProvider> filteredSeedProviders = seedProviderNames.stream()
            .map(hostProviders::get)
            .map(Supplier::get)
            .collect(Collectors.toList());

        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);

        final SeedHostsProvider seedHostsProvider = hostsResolver -> {
            final List<TransportAddress> addresses = new ArrayList<>();
            for (SeedHostsProvider provider : filteredSeedProviders) {
                addresses.addAll(provider.getSeedAddresses(hostsResolver));
            }
            return Collections.unmodifiableList(addresses);
        };

        final ElectionStrategy electionStrategy = electionStrategies.get(ELECTION_STRATEGY_SETTING.get(settings));
        if (electionStrategy == null) {
            throw new IllegalArgumentException("Unknown election strategy " + ELECTION_STRATEGY_SETTING.get(settings));
        }

        if (ZEN2_DISCOVERY_TYPE.equals(discoveryType) || SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType)) {
            discovery = new Coordinator(
                NODE_NAME_SETTING.get(settings),
                settings,
                clusterSettings,
                transportService,
                namedWriteableRegistry,
                allocationService,
                clusterManagerService,
                gatewayMetaState::getPersistedState,
                seedHostsProvider,
                clusterApplier,
                joinValidators,
                new Random(Randomness.get().nextLong()),
                rerouteService,
                electionStrategy,
                nodeHealthService,
                persistedStateRegistry,
                remoteStoreNodeService,
                clusterManagerMetrics,
                remoteClusterStateService
            );
        } else {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }

        logger.info("using discovery type [{}] and seed hosts providers {}", discoveryType, seedProviderNames);
    }

    private List<String> getSeedProviderNames(Settings settings) {
        if (LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.exists(settings)) {
            if (DISCOVERY_SEED_PROVIDERS_SETTING.exists(settings)) {
                throw new IllegalArgumentException(
                    "it is forbidden to set both ["
                        + DISCOVERY_SEED_PROVIDERS_SETTING.getKey()
                        + "] and ["
                        + LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.getKey()
                        + "]"
                );
            }
            return LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.get(settings);
        }
        return DISCOVERY_SEED_PROVIDERS_SETTING.get(settings);
    }

    public static boolean isSingleNodeDiscovery(Settings settings) {
        return SINGLE_NODE_DISCOVERY_TYPE.equals(DISCOVERY_TYPE_SETTING.get(settings));
    }

    public Discovery getDiscovery() {
        return discovery;
    }
}

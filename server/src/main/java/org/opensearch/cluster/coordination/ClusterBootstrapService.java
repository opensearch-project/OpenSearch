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

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.discovery.DiscoveryModule;
import org.opensearch.node.Node;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;
import static org.opensearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.opensearch.discovery.DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

/**
 * Service for bootstrapping the OpenSearch cluster
 *
 * @opensearch.internal
 */
public class ClusterBootstrapService {

    public static final Setting<List<String>> INITIAL_MASTER_NODES_SETTING = Setting.listSetting(
        "cluster.initial_master_nodes",
        emptyList(),
        Function.identity(),
        Property.NodeScope,
        Property.Deprecated
    );
    // The setting below is going to replace the above.
    // To keep backwards compatibility, the old usage is remained, and it's also used as the fallback for the new usage.
    public static final Setting<List<String>> INITIAL_CLUSTER_MANAGER_NODES_SETTING = Setting.listSetting(
        "cluster.initial_cluster_manager_nodes",
        INITIAL_MASTER_NODES_SETTING,
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<TimeValue> UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.unconfigured_bootstrap_timeout",
        TimeValue.timeValueSeconds(3),
        TimeValue.timeValueMillis(1),
        Property.NodeScope
    );

    static final String BOOTSTRAP_PLACEHOLDER_PREFIX = "{bootstrap-placeholder}-";

    private static final Logger logger = LogManager.getLogger(ClusterBootstrapService.class);
    private final Set<String> bootstrapRequirements;
    @Nullable // null if discoveryIsConfigured()
    private final TimeValue unconfiguredBootstrapTimeout;
    private final TransportService transportService;
    private final Supplier<Iterable<DiscoveryNode>> discoveredNodesSupplier;
    private final BooleanSupplier isBootstrappedSupplier;
    private final Consumer<VotingConfiguration> votingConfigurationConsumer;
    private final AtomicBoolean bootstrappingPermitted = new AtomicBoolean(true);

    public ClusterBootstrapService(
        Settings settings,
        TransportService transportService,
        Supplier<Iterable<DiscoveryNode>> discoveredNodesSupplier,
        BooleanSupplier isBootstrappedSupplier,
        Consumer<VotingConfiguration> votingConfigurationConsumer
    ) {
        // TODO: Remove variable 'initialClusterManagerSettingName' after removing MASTER_ROLE.
        String initialClusterManagerSettingName = INITIAL_CLUSTER_MANAGER_NODES_SETTING.exists(settings)
            ? INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey()
            : INITIAL_MASTER_NODES_SETTING.getKey();
        if (DiscoveryModule.isSingleNodeDiscovery(settings)) {
            if (INITIAL_CLUSTER_MANAGER_NODES_SETTING.existsOrFallbackExists(settings)) {
                throw new IllegalArgumentException(
                    "setting ["
                        + initialClusterManagerSettingName
                        + "] is not allowed when ["
                        + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()
                        + "] is set to ["
                        + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE
                        + "]"
                );
            }
            if (DiscoveryNode.isClusterManagerNode(settings) == false) {
                throw new IllegalArgumentException(
                    "node with ["
                        + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()
                        + "] set to ["
                        + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE
                        + "] must be cluster-manager-eligible"
                );
            }
            bootstrapRequirements = Collections.singleton(Node.NODE_NAME_SETTING.get(settings));
            unconfiguredBootstrapTimeout = null;
        } else {
            final List<String> initialClusterManagerNodes = INITIAL_CLUSTER_MANAGER_NODES_SETTING.get(settings);
            bootstrapRequirements = unmodifiableSet(new LinkedHashSet<>(initialClusterManagerNodes));
            if (bootstrapRequirements.size() != initialClusterManagerNodes.size()) {
                throw new IllegalArgumentException(
                    "setting [" + initialClusterManagerSettingName + "] contains duplicates: " + initialClusterManagerNodes
                );
            }
            unconfiguredBootstrapTimeout = discoveryIsConfigured(settings) ? null : UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.get(settings);
        }

        this.transportService = transportService;
        this.discoveredNodesSupplier = discoveredNodesSupplier;
        this.isBootstrappedSupplier = isBootstrappedSupplier;
        this.votingConfigurationConsumer = votingConfigurationConsumer;
    }

    public static boolean discoveryIsConfigured(Settings settings) {
        return Stream.of(
            DISCOVERY_SEED_PROVIDERS_SETTING,
            LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING,
            DISCOVERY_SEED_HOSTS_SETTING,
            LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING,
            INITIAL_CLUSTER_MANAGER_NODES_SETTING,
            INITIAL_MASTER_NODES_SETTING
        ).anyMatch(s -> s.exists(settings));
    }

    void onFoundPeersUpdated() {
        final Set<DiscoveryNode> nodes = getDiscoveredNodes();
        if (bootstrappingPermitted.get()
            && transportService.getLocalNode().isClusterManagerNode()
            && bootstrapRequirements.isEmpty() == false
            && isBootstrappedSupplier.getAsBoolean() == false
            && nodes.stream().noneMatch(Coordinator::isZen1Node)) {

            final Tuple<Set<DiscoveryNode>, List<String>> requirementMatchingResult;
            try {
                requirementMatchingResult = checkRequirements(nodes);
            } catch (IllegalStateException e) {
                logger.warn("bootstrapping cancelled", e);
                bootstrappingPermitted.set(false);
                return;
            }

            final Set<DiscoveryNode> nodesMatchingRequirements = requirementMatchingResult.v1();
            final List<String> unsatisfiedRequirements = requirementMatchingResult.v2();
            logger.trace(
                "nodesMatchingRequirements={}, unsatisfiedRequirements={}, bootstrapRequirements={}",
                nodesMatchingRequirements,
                unsatisfiedRequirements,
                bootstrapRequirements
            );

            if (nodesMatchingRequirements.contains(transportService.getLocalNode()) == false) {
                logger.info(
                    "skipping cluster bootstrapping as local node does not match bootstrap requirements: {}",
                    bootstrapRequirements
                );
                bootstrappingPermitted.set(false);
                return;
            }

            if (nodesMatchingRequirements.size() * 2 > bootstrapRequirements.size()) {
                startBootstrap(nodesMatchingRequirements, unsatisfiedRequirements);
            }
        }
    }

    void scheduleUnconfiguredBootstrap() {
        if (unconfiguredBootstrapTimeout == null) {
            return;
        }

        if (transportService.getLocalNode().isClusterManagerNode() == false) {
            return;
        }

        logger.info(
            "no discovery configuration found, will perform best-effort cluster bootstrapping after [{}] "
                + "unless existing cluster-manager is discovered",
            unconfiguredBootstrapTimeout
        );

        transportService.getThreadPool().scheduleUnlessShuttingDown(unconfiguredBootstrapTimeout, Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                final Set<DiscoveryNode> discoveredNodes = getDiscoveredNodes();
                final List<DiscoveryNode> zen1Nodes = discoveredNodes.stream().filter(Coordinator::isZen1Node).collect(Collectors.toList());
                if (zen1Nodes.isEmpty()) {
                    logger.debug("performing best-effort cluster bootstrapping with {}", discoveredNodes);
                    startBootstrap(discoveredNodes, emptyList());
                } else {
                    logger.info("avoiding best-effort cluster bootstrapping due to discovery of pre-7.0 nodes {}", zen1Nodes);
                }
            }

            @Override
            public String toString() {
                return "unconfigured-discovery delayed bootstrap";
            }
        });
    }

    private Set<DiscoveryNode> getDiscoveredNodes() {
        return Stream.concat(
            Stream.of(transportService.getLocalNode()),
            StreamSupport.stream(discoveredNodesSupplier.get().spliterator(), false)
        ).collect(Collectors.toSet());
    }

    private void startBootstrap(Set<DiscoveryNode> discoveryNodes, List<String> unsatisfiedRequirements) {
        assert discoveryNodes.stream().allMatch(DiscoveryNode::isClusterManagerNode) : discoveryNodes;
        assert discoveryNodes.stream().noneMatch(Coordinator::isZen1Node) : discoveryNodes;
        assert unsatisfiedRequirements.size() < discoveryNodes.size() : discoveryNodes + " smaller than " + unsatisfiedRequirements;
        if (bootstrappingPermitted.compareAndSet(true, false)) {
            doBootstrap(
                new VotingConfiguration(
                    Stream.concat(
                        discoveryNodes.stream().map(DiscoveryNode::getId),
                        unsatisfiedRequirements.stream().map(s -> BOOTSTRAP_PLACEHOLDER_PREFIX + s)
                    ).collect(Collectors.toSet())
                )
            );
        }
    }

    public static boolean isBootstrapPlaceholder(String nodeId) {
        return nodeId.startsWith(BOOTSTRAP_PLACEHOLDER_PREFIX);
    }

    private void doBootstrap(VotingConfiguration votingConfiguration) {
        assert transportService.getLocalNode().isClusterManagerNode();

        try {
            votingConfigurationConsumer.accept(votingConfiguration);
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("exception when bootstrapping with {}, rescheduling", votingConfiguration), e);
            transportService.getThreadPool().scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(10), Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    doBootstrap(votingConfiguration);
                }

                @Override
                public String toString() {
                    return "retry of failed bootstrapping with " + votingConfiguration;
                }
            });
        }
    }

    private static boolean matchesRequirement(DiscoveryNode discoveryNode, String requirement) {
        return discoveryNode.getName().equals(requirement)
            || discoveryNode.getAddress().toString().equals(requirement)
            || discoveryNode.getAddress().getAddress().equals(requirement);
    }

    private Tuple<Set<DiscoveryNode>, List<String>> checkRequirements(Set<DiscoveryNode> nodes) {
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        final List<String> unmatchedRequirements = new ArrayList<>();
        for (final String bootstrapRequirement : bootstrapRequirements) {
            final Set<DiscoveryNode> matchingNodes = nodes.stream()
                .filter(n -> matchesRequirement(n, bootstrapRequirement))
                .collect(Collectors.toSet());

            if (matchingNodes.size() == 0) {
                unmatchedRequirements.add(bootstrapRequirement);
            }

            if (matchingNodes.size() > 1) {
                throw new IllegalStateException("requirement [" + bootstrapRequirement + "] matches multiple nodes: " + matchingNodes);
            }

            for (final DiscoveryNode matchingNode : matchingNodes) {
                if (selectedNodes.add(matchingNode) == false) {
                    throw new IllegalStateException(
                        "node ["
                            + matchingNode
                            + "] matches multiple requirements: "
                            + bootstrapRequirements.stream().filter(r -> matchesRequirement(matchingNode, r)).collect(Collectors.toList())
                    );
                }
            }
        }

        return Tuple.tuple(selectedNodes, unmatchedRequirements);
    }
}

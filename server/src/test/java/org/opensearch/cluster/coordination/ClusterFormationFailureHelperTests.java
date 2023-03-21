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

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.opensearch.cluster.coordination.ClusterBootstrapService.BOOTSTRAP_PLACEHOLDER_PREFIX;
import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING;
import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.opensearch.monitor.StatusInfo.Status.HEALTHY;
import static org.opensearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ClusterFormationFailureHelperTests extends OpenSearchTestCase {

    private static final ElectionStrategy electionStrategy = ElectionStrategy.DEFAULT_INSTANCE;

    public void testScheduling() {
        final long expectedDelayMillis;
        final Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            expectedDelayMillis = ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(Settings.EMPTY)
                .millis();
        } else {
            expectedDelayMillis = randomLongBetween(100, 100000);
            settingsBuilder.put(
                ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.getKey(),
                expectedDelayMillis + "ms"
            );
        }

        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(),
            random()
        );

        final AtomicLong warningCount = new AtomicLong();
        final AtomicLong logLastFailedJoinAttemptWarningCount = new AtomicLong();

        final ClusterFormationFailureHelper clusterFormationFailureHelper = new ClusterFormationFailureHelper(
            settingsBuilder.build(),
            () -> {
                warningCount.incrementAndGet();
                return new ClusterFormationState(
                    Settings.EMPTY,
                    clusterState,
                    emptyList(),
                    emptyList(),
                    0L,
                    electionStrategy,
                    new StatusInfo(HEALTHY, "healthy-info")
                );
            },
            deterministicTaskQueue.getThreadPool(),
            logLastFailedJoinAttemptWarningCount::incrementAndGet
        );

        deterministicTaskQueue.runAllTasks();
        assertThat("should not schedule anything yet", warningCount.get(), is(0L));

        final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
        clusterFormationFailureHelper.start();

        while (warningCount.get() == 0) {
            assertTrue(clusterFormationFailureHelper.isRunning());
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(warningCount.get(), is(1L));
        assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, is(expectedDelayMillis));

        while (warningCount.get() < 5) {
            assertTrue(clusterFormationFailureHelper.isRunning());
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, equalTo(5 * expectedDelayMillis));

        clusterFormationFailureHelper.stop();
        assertFalse(clusterFormationFailureHelper.isRunning());
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertThat(warningCount.get(), is(5L));
        assertThat(logLastFailedJoinAttemptWarningCount.get(), is(5L));

        warningCount.set(0);
        logLastFailedJoinAttemptWarningCount.set(0);
        clusterFormationFailureHelper.start();
        clusterFormationFailureHelper.stop();
        clusterFormationFailureHelper.start();
        final long secondStartTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();

        while (warningCount.get() < 5) {
            assertTrue(clusterFormationFailureHelper.isRunning());
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(deterministicTaskQueue.getCurrentTimeMillis() - secondStartTimeMillis, equalTo(5 * expectedDelayMillis));

        clusterFormationFailureHelper.stop();
        assertFalse(clusterFormationFailureHelper.isRunning());
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertThat(warningCount.get(), is(5L));
        assertThat(logLastFailedJoinAttemptWarningCount.get(), is(5L));
    }

    public void testDescriptionOnClusterManagerIneligibleNodes() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(4L).build()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet: have discovered []; discovery will continue using [] from hosts providers "
                    + "and [] from last-known cluster state; node term 15, last-accepted version 12 in term 4"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                16L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet: have discovered []; discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and [] from last-known cluster state; node term 16, last-accepted version 12 in term 4"
            )
        );

        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                17L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet: have discovered ["
                    + otherNode
                    + "]; discovery will continue using [] from hosts providers "
                    + "and [] from last-known cluster state; node term 17, last-accepted version 12 in term 4"
            )
        );
    }

    public void testDescriptionForBWCState() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .version(42L) // check that we use metadata version in case of BWC term 0
                    .coordinationMetadata(CoordinationMetadata.builder().term(Coordinator.ZEN1_BWC_TERM).build())
                    .build()
            )
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet: have discovered []; discovery will continue using [] from hosts providers "
                    + "and [] from last-known cluster state; node term 15, last-accepted version 42 in term 0"
            )
        );
    }

    public void testDescriptionOnUnhealthyNodes() {
        final DiscoveryNode dataNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L)
            .nodes(DiscoveryNodes.builder().add(dataNode).localNodeId(dataNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(UNHEALTHY, "unhealthy-info")
            ).getDescription(),
            is("this node is unhealthy: unhealthy-info")
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(
            "local",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L)
            .nodes(DiscoveryNodes.builder().add(clusterManagerNode).localNodeId(clusterManagerNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(UNHEALTHY, "unhealthy-info")
            ).getDescription(),
            is("this node is unhealthy: unhealthy-info")
        );
    }

    public void testDescriptionBeforeBootstrapping() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(7L)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(4L).build()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                1L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "[cluster.initial_cluster_manager_nodes] is empty on this node: have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 1, last-accepted version 7 in term 4"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                2L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "[cluster.initial_cluster_manager_nodes] is empty on this node: have discovered []; "
                    + "discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 2, last-accepted version 7 in term 4"
            )
        );

        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                3L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "[cluster.initial_cluster_manager_nodes] is empty on this node: have discovered ["
                    + otherNode
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 3, last-accepted version 7 in term 4"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.builder().putList(INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey(), "other").build(),
                clusterState,
                emptyList(),
                emptyList(),
                4L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "this node must discover cluster-manager-eligible nodes [other] to bootstrap a cluster: have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 4, last-accepted version 7 in term 4"
            )
        );
    }

    // Validate the value of the deprecated 'master' setting can be obtained during cluster formation failure.
    public void testDescriptionBeforeBootstrappingWithDeprecatedMasterNodesSetting() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(7L)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(4L).build()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();
        assertThat(
            new ClusterFormationState(
                Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "other").build(),
                clusterState,
                emptyList(),
                emptyList(),
                4L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "this node must discover cluster-manager-eligible nodes [other] to bootstrap a cluster: have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 4, last-accepted version 7 in term 4"
            )
        );
        assertWarnings(
            "[cluster.initial_master_nodes] setting was deprecated in OpenSearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    private static VotingConfiguration config(String[] nodeIds) {
        return new VotingConfiguration(Arrays.stream(nodeIds).collect(Collectors.toSet()));
    }

    private static ClusterState state(DiscoveryNode localNode, String... configNodeIds) {
        return state(localNode, configNodeIds, configNodeIds);
    }

    private static ClusterState state(DiscoveryNode localNode, String[] acceptedConfig, String[] committedConfig) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastAcceptedConfiguration(config(acceptedConfig))
                            .lastCommittedConfiguration(config(committedConfig))
                            .build()
                    )
            )
            .build();
    }

    public void testDescriptionAfterDetachCluster() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClusterState clusterState = state(
            localNode,
            VotingConfiguration.MUST_JOIN_ELECTED_CLUSTER_MANAGER.getNodeIds().toArray(new String[0])
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered []; "
                    + "discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered ["
                    + otherNode
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode yetAnotherNode = new DiscoveryNode("yetAnotherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(yetAnotherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered ["
                    + yetAnotherNode
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

    }

    public void testDescriptionAfterBootstrapping() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClusterState clusterState = state(localNode, "otherNode");

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have discovered ["
                    + otherNode
                    + "] which is a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode yetAnotherNode = new DiscoveryNode("yetAnotherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(yetAnotherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have discovered ["
                    + yetAnotherNode
                    + "] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires two nodes with ids [n1, n2], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires at least 2 nodes with ids from [n1, n2, n3], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", BOOTSTRAP_PLACEHOLDER_PREFIX + "n3"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires 2 nodes with ids [n1, n2], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", "n4"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", "n4", "n5"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4, n5], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", "n4", BOOTSTRAP_PLACEHOLDER_PREFIX + "n5"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", BOOTSTRAP_PLACEHOLDER_PREFIX + "n4", BOOTSTRAP_PLACEHOLDER_PREFIX + "n5"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires 3 nodes with ids [n1, n2, n3], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n1" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [n1], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n2" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [n1] and a node with id [n2], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n2", "n3" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [n1] and two nodes with ids [n2, n3], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n2", "n3", "n4" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires a node with id [n1] and "
                    + "at least 2 nodes with ids from [n2, n3, n4], "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode otherClusterManagerNode = new DiscoveryNode(
            "other-cluster-manager",
            buildNewFakeTransportAddress(),
            Version.CURRENT
        );
        final DiscoveryNode otherNonClusterManagerNode = new DiscoveryNode(
            "other-non-cluster-manager",
            buildNewFakeTransportAddress(),
            emptyMap(),
            new HashSet<>(
                randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES).stream()
                    .filter(r -> r != DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)
                    .collect(Collectors.toList())
            ),
            Version.CURRENT
        );

        String[] configNodeIds = new String[] { "n1", "n2" };
        final ClusterState stateWithOtherNodes = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(localNode)
                    .localNodeId(localNode.getId())
                    .add(otherClusterManagerNode)
                    .add(otherNonClusterManagerNode)
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastAcceptedConfiguration(config(configNodeIds))
                            .lastCommittedConfiguration(config(configNodeIds))
                            .build()
                    )
            )
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                stateWithOtherNodes,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),

            // nodes from last-known cluster state could be in either order
            is(
                oneOf(
                    "cluster-manager not discovered or elected yet, an election requires two nodes with ids [n1, n2], "
                        + "have discovered [] which is not a quorum; "
                        + "discovery will continue using [] from hosts providers and ["
                        + localNode
                        + ", "
                        + otherClusterManagerNode
                        + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0",

                    "cluster-manager not discovered or elected yet, an election requires two nodes with ids [n1, n2], "
                        + "have discovered [] which is not a quorum; "
                        + "discovery will continue using [] from hosts providers and ["
                        + otherClusterManagerNode
                        + ", "
                        + localNode
                        + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
                )
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, GatewayMetaState.STALE_STATE_CONFIG_NODE_ID),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")
            ).getDescription(),
            is(
                "cluster-manager not discovered or elected yet, an election requires one or more nodes that have already participated as "
                    + "cluster-manager-eligible nodes in the cluster but this node was not cluster-manager-eligible the last time it joined the cluster, "
                    + "have discovered [] which is not a quorum; "
                    + "discovery will continue using [] from hosts providers and ["
                    + localNode
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

    }
}

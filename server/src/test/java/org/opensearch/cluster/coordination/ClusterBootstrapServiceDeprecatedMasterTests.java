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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.DiscoveryModule;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.opensearch.cluster.coordination.ClusterBootstrapService.UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING;
import static org.opensearch.common.settings.Settings.builder;
import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.opensearch.test.NodeRoles.nonClusterManagerNode;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/*
 * As of 2.0, MASTER_ROLE and setting 'cluster.initial_master_nodes' is deprecated to promote inclusive language.
 * This class is a partial copy of ClusterBootstrapServiceTests
 * to validate ClusterBootstrapService works correctly with the deprecated node role and cluster setting.
 * Remove the class after the deprecated node role and cluster setting is removed.
 */
public class ClusterBootstrapServiceDeprecatedMasterTests extends OpenSearchTestCase {

    private DiscoveryNode localNode, otherNode1, otherNode2;
    private DeterministicTaskQueue deterministicTaskQueue;
    private TransportService transportService;
    private static final String CLUSTER_SETTING_DEPRECATED_MESSAGE =
        "[cluster.initial_master_nodes] setting was deprecated in OpenSearch and will be removed in a future release! "
            + "See the breaking changes documentation for the next major version.";

    @Before
    public void createServices() {
        localNode = newDiscoveryNode("local");
        otherNode1 = newDiscoveryNode("other1");
        otherNode2 = newDiscoveryNode("other2");

        deterministicTaskQueue = new DeterministicTaskQueue(builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                throw new AssertionError("unexpected " + action);
            }
        };

        transportService = transport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT
        );
    }

    public void testBootstrapsAutomaticallyWithDefaultConfiguration() {
        final Settings.Builder settings = Settings.builder();
        final long timeout;
        if (randomBoolean()) {
            timeout = UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeout = randomLongBetween(1, 10000);
            settings.put(UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.getKey(), timeout + "ms");
        }

        final AtomicReference<Supplier<Iterable<DiscoveryNode>>> discoveredNodesSupplier = new AtomicReference<>(() -> {
            throw new AssertionError("should not be called yet");
        });

        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            settings.build(),
            transportService,
            () -> discoveredNodesSupplier.get().get(),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(
                    vc.getNodeIds(),
                    equalTo(Stream.of(localNode, otherNode1, otherNode2).map(DiscoveryNode::getId).collect(Collectors.toSet()))
                );
                assertThat(deterministicTaskQueue.getCurrentTimeMillis(), greaterThanOrEqualTo(timeout));
            }
        );

        deterministicTaskQueue.scheduleAt(
            timeout - 1,
            () -> discoveredNodesSupplier.set(() -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet()))
        );

        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(bootstrapped.get());
    }

    // Validate the deprecated setting is still valid during the cluster bootstrap.
    public void testDoesNothingByDefaultIfMasterNodesConfigured() {
        testDoesNothingWithSettings(builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()));
        assertWarnings(CLUSTER_SETTING_DEPRECATED_MESSAGE);
    }

    private void testDoesNothingWithSettings(Settings.Builder builder) {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(builder.build(), transportService, () -> {
            throw new AssertionError("should not be called");
        }, () -> false, vc -> { throw new AssertionError("should not be called"); });
        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        deterministicTaskQueue.runAllTasks();
    }

    public void testThrowsExceptionOnDuplicates() {
        final IllegalArgumentException illegalArgumentException = expectThrows(IllegalArgumentException.class, () -> {
            new ClusterBootstrapService(
                builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "duplicate-requirement", "duplicate-requirement").build(),
                transportService,
                Collections::emptyList,
                () -> false,
                vc -> {
                    throw new AssertionError("should not be called");
                }
            );
        });

        assertThat(illegalArgumentException.getMessage(), containsString(INITIAL_MASTER_NODES_SETTING.getKey()));
        assertThat(illegalArgumentException.getMessage(), containsString("duplicate-requirement"));
        assertWarnings(CLUSTER_SETTING_DEPRECATED_MESSAGE);
    }

    public void testBootstrapsOnDiscoveryOfAllRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> Stream.of(otherNode1, otherNode2).collect(Collectors.toList()),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(vc.getNodeIds(), containsInAnyOrder(localNode.getId(), otherNode1.getId(), otherNode2.getId()));
                assertThat(vc.getNodeIds(), not(hasItem(containsString("placeholder"))));
            }
        );
        assertWarnings(CLUSTER_SETTING_DEPRECATED_MESSAGE);

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertFalse(bootstrapped.get()); // should only bootstrap once
    }

    public void testDoesNotBootstrapsOnNonMasterNode() {
        localNode = new DiscoveryNode(
            "local",
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toList()),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );
        assertWarnings(CLUSTER_SETTING_DEPRECATED_MESSAGE);
        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsIfLocalNodeNotInInitialMasterNodes() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toList()),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );
        assertWarnings(CLUSTER_SETTING_DEPRECATED_MESSAGE);
        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsIfNotConfigured() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()).build(),
            transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toList()),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );
        assertWarnings(CLUSTER_SETTING_DEPRECATED_MESSAGE);
        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    /**
     * Validate the correct deprecated setting name of cluster.initial_master_nodes is shown in the exception,
     * when discovery type is single-node.
     */
    public void testFailBootstrapWithBothSingleNodeDiscoveryAndInitialMasterNodes() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(INITIAL_MASTER_NODES_SETTING.getKey(), "test");

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new ClusterBootstrapService(settings.build(), transportService, () -> emptyList(), () -> false, vc -> fail())
            ).getMessage(),
            containsString(
                "setting [" + INITIAL_MASTER_NODES_SETTING.getKey() + "] is not allowed when [discovery.type] is set " + "to [single-node]"
            )
        );
    }

    public void testFailBootstrapNonMasterEligibleNodeWithSingleNodeDiscovery() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(nonClusterManagerNode());

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new ClusterBootstrapService(settings.build(), transportService, () -> emptyList(), () -> false, vc -> fail())
            ).getMessage(),
            containsString("node with [discovery.type] set to [single-node] must be cluster-manager-eligible")
        );
    }
}

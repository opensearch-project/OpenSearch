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

package org.opensearch.test;

import org.apache.logging.log4j.core.util.Throwables;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.MasterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.fail;

public class ClusterServiceUtils {

    public static ClusterManagerService createClusterManagerService(ThreadPool threadPool, ClusterState initialClusterState) {
        ClusterManagerService clusterManagerService = new ClusterManagerService(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_cluster_manager_node").build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            NoopMetricsRegistry.INSTANCE
        );
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
        clusterManagerService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            clusterStateRef.set(event.state());
            publishListener.onResponse(null);
        });
        clusterManagerService.setClusterStateSupplier(clusterStateRef::get);
        clusterManagerService.start();
        return clusterManagerService;
    }

    public static ClusterManagerService createClusterManagerService(ThreadPool threadPool, DiscoveryNode localNode) {
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).clusterManagerNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        return createClusterManagerService(threadPool, initialClusterState);
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #createClusterManagerService(ThreadPool, ClusterState)} */
    @Deprecated
    public static MasterService createMasterService(ThreadPool threadPool, ClusterState initialClusterState) {
        return createClusterManagerService(threadPool, initialClusterState);
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #createClusterManagerService(ThreadPool, DiscoveryNode)} */
    @Deprecated
    public static MasterService createMasterService(ThreadPool threadPool, DiscoveryNode localNode) {
        return createClusterManagerService(threadPool, localNode);
    }

    public static void setState(ClusterApplierService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        executor.onNewClusterState(
            "test setting state",
            () -> ClusterState.builder(clusterState).version(clusterState.version() + 1).build(),
            new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    exception.set(e);
                    latch.countDown();
                }
            }
        );
        try {
            latch.await();
            if (exception.get() != null) {
                Throwables.rethrow(exception.get());
            }
        } catch (InterruptedException e) {
            throw new OpenSearchException("unexpected exception", e);
        }
    }

    public static void setState(ClusterManagerService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        executor.submitStateUpdateTask("test setting state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                // make sure we increment versions as listener may depend on it for change
                return ClusterState.builder(clusterState).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                fail("unexpected exception" + e);
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new OpenSearchException("unexpected interruption", e);
        }
    }

    public static ClusterService createClusterService(ThreadPool threadPool) {
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        return createClusterService(threadPool, discoveryNode);
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode) {
        return createClusterService(threadPool, localNode, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode, ClusterSettings clusterSettings) {
        return createClusterService(threadPool, localNode, clusterSettings, NoopMetricsRegistry.INSTANCE);
    }

    public static ClusterService createClusterService(
        ThreadPool threadPool,
        DiscoveryNode localNode,
        ClusterSettings clusterSettings,
        MetricsRegistry metricsRegistry
    ) {
        Settings settings = Settings.builder().put("node.name", "test").put("cluster.name", "ClusterServiceTests").build();
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool, metricsRegistry);
        clusterService.setNodeConnectionsService(createNoOpNodeConnectionsService());
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).clusterManagerNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getClusterManagerService()
            .setClusterStatePublisher(createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getClusterManagerService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public static ClusterService createClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        return new ClusterService(settings, clusterSettings, threadPool, NoopMetricsRegistry.INSTANCE);
    }

    public static NodeConnectionsService createNoOpNodeConnectionsService() {
        return new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
                // don't do anything
                onCompletion.run();
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // don't do anything
            }
        };
    }

    public static ClusterStatePublisher createClusterStatePublisher(ClusterApplier clusterApplier) {
        return (event, publishListener, ackListener) -> clusterApplier.onNewClusterState(
            "mock_publish_to_self[" + event.source() + "]",
            () -> event.state(),
            new ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    publishListener.onResponse(null);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    publishListener.onFailure(e);
                }
            }
        );
    }

    public static ClusterService createClusterService(ClusterState initialState, ThreadPool threadPool) {
        ClusterService clusterService = createClusterService(threadPool);
        setState(clusterService, initialState);
        return clusterService;
    }

    public static void setState(ClusterService clusterService, ClusterState.Builder clusterStateBuilder) {
        setState(clusterService, clusterStateBuilder.build());
    }

    /**
     * Sets the state on the cluster applier service
     */
    public static void setState(ClusterService clusterService, ClusterState clusterState) {
        setState(clusterService.getClusterApplierService(), clusterState);
    }
}

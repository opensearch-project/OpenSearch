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

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.FakeThreadPoolClusterManagerService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.node.Node;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class InternalClusterInfoServiceSchedulingTests extends OpenSearchTestCase {

    public void testScheduling() {
        final DiscoveryNode discoveryNode = new DiscoveryNode("test", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNodes noClusterManager = DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).build();
        final DiscoveryNodes localClusterManager = DiscoveryNodes.builder(noClusterManager)
            .clusterManagerNodeId(discoveryNode.getId())
            .build();

        final Settings.Builder settingsBuilder = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), discoveryNode.getName());
        if (randomBoolean()) {
            settingsBuilder.put(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), randomIntBetween(10000, 60000) + "ms");
        }
        final Settings settings = settingsBuilder.build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        final ClusterApplierService clusterApplierService = new ClusterApplierService(
            "test",
            settings,
            clusterSettings,
            threadPool,
            NoopMetricsRegistry.INSTANCE
        ) {
            @Override
            protected PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
                return new MockSinglePrioritizingExecutor("mock-executor", deterministicTaskQueue, threadPool);
            }
        };

        final ClusterManagerService clusterManagerService = new FakeThreadPoolClusterManagerService(
            "test",
            "clusterManagerService",
            threadPool,
            r -> {
                fail("cluster-manager service should not run any tasks");
            }
        );

        final ClusterService clusterService = new ClusterService(settings, clusterSettings, clusterManagerService, clusterApplierService);

        final FakeClusterInfoServiceClient client = new FakeClusterInfoServiceClient(threadPool);
        final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        clusterService.addListener(clusterInfoService);
        clusterInfoService.addListener(ignored -> {});

        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterApplierService.setInitialState(ClusterState.builder(new ClusterName("cluster")).nodes(noClusterManager).build());
        clusterManagerService.setClusterStatePublisher((clusterChangedEvent, publishListener, ackListener) -> fail("should not publish"));
        clusterManagerService.setClusterStateSupplier(clusterApplierService::state);
        clusterService.start();

        final AtomicBoolean becameClusterManager1 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "become cluster-manager 1",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(localClusterManager).build(),
            setFlagOnSuccess(becameClusterManager1)
        );
        runUntilFlag(deterministicTaskQueue, becameClusterManager1);

        final AtomicBoolean failClusterManager1 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "fail cluster-manager 1",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(noClusterManager).build(),
            setFlagOnSuccess(failClusterManager1)
        );
        runUntilFlag(deterministicTaskQueue, failClusterManager1);

        final AtomicBoolean becameClusterManager2 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "become cluster-manager 2",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(localClusterManager).build(),
            setFlagOnSuccess(becameClusterManager2)
        );
        runUntilFlag(deterministicTaskQueue, becameClusterManager2);

        for (int i = 0; i < 3; i++) {
            final int initialRequestCount = client.requestCount;
            final long duration = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis();
            runFor(deterministicTaskQueue, duration);
            deterministicTaskQueue.runAllRunnableTasks();
            assertThat(client.requestCount, equalTo(initialRequestCount + 2)); // should have run two client requests per interval
        }

        final AtomicBoolean failClusterManager2 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "fail cluster-manager 2",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(noClusterManager).build(),
            setFlagOnSuccess(failClusterManager2)
        );
        runUntilFlag(deterministicTaskQueue, failClusterManager2);

        runFor(deterministicTaskQueue, INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis());
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }

    private static void runFor(DeterministicTaskQueue deterministicTaskQueue, long duration) {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + duration;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime
            && (deterministicTaskQueue.hasRunnableTasks() || deterministicTaskQueue.hasDeferredTasks())) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    private static void runUntilFlag(DeterministicTaskQueue deterministicTaskQueue, AtomicBoolean flag) {
        while (flag.get() == false) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    private static ClusterApplier.ClusterApplyListener setFlagOnSuccess(AtomicBoolean flag) {
        return new ClusterApplier.ClusterApplyListener() {

            @Override
            public void onSuccess(String source) {
                assertTrue(flag.compareAndSet(false, true));
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError("unexpected", e);
            }
        };
    }

    private static class FakeClusterInfoServiceClient extends NoOpClient {

        int requestCount;

        FakeClusterInfoServiceClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof NodesStatsRequest || request instanceof IndicesStatsRequest) {
                requestCount++;
                // ClusterInfoService handles ClusterBlockExceptions quietly, so we invent such an exception to avoid excess logging
                listener.onFailure(new ClusterBlockException(Set.of(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ALL)));
            } else {
                fail("unexpected action: " + action.name());
            }
        }
    }

}

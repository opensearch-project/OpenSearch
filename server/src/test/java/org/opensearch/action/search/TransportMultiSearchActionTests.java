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

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.tasks.TaskManager;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.action.support.PlainActionFuture.newFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchActionTests extends OpenSearchTestCase {

    public void testParentTaskId() throws Exception {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet(),
                NoopTracer.INSTANCE
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

            String localNodeId = randomAlphaOfLengthBetween(3, 10);
            int numSearchRequests = randomIntBetween(1, 100);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }
            AtomicInteger counter = new AtomicInteger(0);
            Task task = multiSearchRequest.createTask(randomLong(), "type", "action", null, Collections.emptyMap());
            NodeClient client = new NodeClient(settings, threadPool) {
                @Override
                public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                    assertEquals(task.getId(), request.getParentTask().getId());
                    assertEquals(localNodeId, request.getParentTask().getNodeId());
                    counter.incrementAndGet();
                    listener.onResponse(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
                }

                @Override
                public String getLocalNodeId() {
                    return localNodeId;
                }
            };
            TransportMultiSearchAction action = new TransportMultiSearchAction(
                threadPool,
                actionFilters,
                transportService,
                clusterService,
                10,
                System::nanoTime,
                client
            );

            PlainActionFuture<MultiSearchResponse> future = newFuture();
            action.execute(task, multiSearchRequest, future);
            future.get();
            assertEquals(numSearchRequests, counter.get());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testBatchExecute() {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        ) {
            @Override
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

        // Keep track of the number of concurrent searches started by multi search api,
        // and if there are more searches than is allowed create an error and remember that.
        int maxAllowedConcurrentSearches = scaledRandomIntBetween(1, 16);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<AssertionError> errorHolder = new AtomicReference<>();
        // randomize whether or not requests are executed asynchronously
        final List<String> threadPoolNames = Arrays.asList(ThreadPool.Names.GENERIC, ThreadPool.Names.SAME);
        Randomness.shuffle(threadPoolNames);
        final ExecutorService commonExecutor = threadPool.executor(threadPoolNames.get(0));
        final ExecutorService rarelyExecutor = threadPool.executor(threadPoolNames.get(1));
        final Set<SearchRequest> requests = Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
        NodeClient client = new NodeClient(settings, threadPool) {
            @Override
            public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                requests.add(request);
                int currentConcurrentSearches = counter.incrementAndGet();
                if (currentConcurrentSearches > maxAllowedConcurrentSearches) {
                    errorHolder.set(
                        new AssertionError(
                            "Current concurrent search ["
                                + currentConcurrentSearches
                                + "] is higher than is allowed ["
                                + maxAllowedConcurrentSearches
                                + "]"
                        )
                    );
                }
                final ExecutorService executorService = rarely() ? rarelyExecutor : commonExecutor;
                executorService.execute(() -> {
                    counter.decrementAndGet();
                    listener.onResponse(
                        new SearchResponse(
                            InternalSearchResponse.empty(),
                            null,
                            0,
                            0,
                            0,
                            0L,
                            ShardSearchFailure.EMPTY_ARRAY,
                            SearchResponse.Clusters.EMPTY
                        )
                    );
                });
            }

            @Override
            public String getLocalNodeId() {
                return "local_node_id";
            }
        };

        TransportMultiSearchAction action = new TransportMultiSearchAction(
            threadPool,
            actionFilters,
            transportService,
            clusterService,
            10,
            System::nanoTime,
            client
        );

        // Execute the multi search api and fail if we find an error after executing:
        try {
            /*
             * Allow for a large number of search requests in a single batch as previous implementations could stack overflow if the number
             * of requests in a single batch was large
             */
            int numSearchRequests = scaledRandomIntBetween(1, 8192);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            multiSearchRequest.maxConcurrentSearchRequests(maxAllowedConcurrentSearches);
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }

            MultiSearchResponse response = ActionTestUtils.executeBlocking(action, multiSearchRequest);
            assertThat(response.getResponses().length, equalTo(numSearchRequests));
            assertThat(requests.size(), equalTo(numSearchRequests));
            assertThat(errorHolder.get(), nullValue());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDefaultMaxConcurrentSearches() {
        int numDataNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < numDataNodes; i++) {
            builder.add(
                new DiscoveryNode(
                    "_id" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT
                )
            );
        }
        builder.add(
            new DiscoveryNode(
                "cluster_manager",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
                Version.CURRENT
            )
        );
        builder.add(
            new DiscoveryNode(
                "ingest",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Collections.singleton(DiscoveryNodeRole.INGEST_ROLE),
                Version.CURRENT
            )
        );

        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(builder).build();
        int result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(10 * numDataNodes));

        state = ClusterState.builder(new ClusterName("_name")).build();
        result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(1));
    }

    public void testCancellation() {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        ) {
            @Override
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

        // Keep track of the number of concurrent searches started by multi search api,
        // and if there are more searches than is allowed create an error and remember that.
        int maxAllowedConcurrentSearches = 1; // Allow 1 search at a time.
        AtomicInteger counter = new AtomicInteger();
        ExecutorService executorService = threadPool.executor(ThreadPool.Names.GENERIC);
        CountDownLatch canceledLatch = new CountDownLatch(1);
        CancellableTask[] parentTask = new CancellableTask[1];
        NodeClient client = new NodeClient(settings, threadPool) {
            @Override
            public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                if (parentTask[0] != null && parentTask[0].isCancelled()) {
                    fail("Should not execute search after parent task is cancelled");
                }

                executorService.execute(() -> {
                    try {
                        if (!canceledLatch.await(1, TimeUnit.SECONDS)) {
                            fail("Latch should have counted down");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    counter.decrementAndGet();
                    listener.onResponse(
                        new SearchResponse(
                            InternalSearchResponse.empty(),
                            null,
                            0,
                            0,
                            0,
                            0L,
                            ShardSearchFailure.EMPTY_ARRAY,
                            SearchResponse.Clusters.EMPTY
                        )
                    );
                });
            }

            @Override
            public String getLocalNodeId() {
                return "local_node_id";
            }
        };

        TransportMultiSearchAction action = new TransportMultiSearchAction(
            threadPool,
            actionFilters,
            transportService,
            clusterService,
            10,
            System::nanoTime,
            client
        );

        // Execute the multi search api and fail if we find an error after executing:
        try {
            /*
             * Allow for a large number of search requests in a single batch as previous implementations could stack overflow if the number
             * of requests in a single batch was large
             */
            int numSearchRequests = scaledRandomIntBetween(1024, 8192);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            multiSearchRequest.maxConcurrentSearchRequests(maxAllowedConcurrentSearches);
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }
            MultiSearchResponse[] responses = new MultiSearchResponse[1];
            Exception[] exceptions = new Exception[1];
            parentTask[0] = (CancellableTask) action.execute(multiSearchRequest, new TaskListener<>() {
                @Override
                public void onResponse(Task task, MultiSearchResponse items) {
                    responses[0] = items;
                }

                @Override
                public void onFailure(Task task, Exception e) {
                    exceptions[0] = e;
                }
            });
            parentTask[0].cancel("Giving up");
            canceledLatch.countDown();

            assertNull(responses[0]);
            assertNull(exceptions[0]);
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }
}

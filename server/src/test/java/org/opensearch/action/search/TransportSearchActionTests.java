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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.OriginalIndicesTests;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.admin.indices.stats.SearchResponseStatusStats;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.GroupShardsIteratorTests;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.RemoteClusterConnectionTests;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.RemoteClusterServiceTests;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportConnectionListener;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupSearchCancelAfterTimeIntervalResolver;
import org.opensearch.wlm.WorkloadGroupSearchSettings;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.opensearch.test.InternalAggregationTestCase.emptyReduceContextBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.awaitLatch;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSearchActionTests extends OpenSearchTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private static SearchShardIterator createSearchShardIterator(
        int id,
        Index index,
        OriginalIndices originalIndices,
        String clusterAlias
    ) {
        ShardId shardId = new ShardId(index, id);
        List<ShardRouting> shardRoutings = GroupShardsIteratorTests.randomShardRoutings(shardId);
        return new SearchShardIterator(clusterAlias, shardId, shardRoutings, originalIndices);
    }

    public void testMergeShardsIterators() {
        Index[] indices = new Index[randomIntBetween(1, 10)];
        for (int i = 0; i < indices.length; i++) {
            if (randomBoolean() && i > 0) {
                Index existingIndex = indices[randomIntBetween(0, i - 1)];
                indices[i] = new Index(existingIndex.getName(), randomAlphaOfLength(10));
            } else {
                indices[i] = new Index(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10));
            }
        }
        Arrays.sort(indices, (o1, o2) -> {
            int nameCompareTo = o1.getName().compareTo(o2.getName());
            if (nameCompareTo == 0) {
                return o1.getUUID().compareTo(o2.getUUID());
            }
            return nameCompareTo;
        });
        String[] remoteClusters = new String[randomIntBetween(1, 3)];
        for (int i = 0; i < remoteClusters.length; i++) {
            remoteClusters[i] = randomAlphaOfLengthBetween(5, 10);
        }
        Arrays.sort(remoteClusters);

        List<SearchShardIterator> expected = new ArrayList<>();
        String localClusterAlias = randomAlphaOfLengthBetween(5, 10);
        OriginalIndices localIndices = OriginalIndicesTests.randomOriginalIndices();
        List<SearchShardIterator> localShardIterators = new ArrayList<>();
        List<SearchShardIterator> remoteShardIterators = new ArrayList<>();
        int numShards = randomIntBetween(0, 10);
        for (int i = 0; i < numShards; i++) {
            int numIndices = randomIntBetween(0, indices.length);
            for (int j = 0; j < numIndices; j++) {
                Index index = indices[j];
                boolean localIndex = randomBoolean();
                if (localIndex) {
                    SearchShardIterator localIterator = createSearchShardIterator(i, index, localIndices, localClusterAlias);
                    localShardIterators.add(localIterator);
                    if (rarely()) {
                        String remoteClusterAlias = randomFrom(remoteClusters);
                        // simulate scenario where the local cluster is also registered as a remote one
                        SearchShardIterator remoteIterator = createSearchShardIterator(
                            i,
                            index,
                            OriginalIndicesTests.randomOriginalIndices(),
                            remoteClusterAlias
                        );
                        remoteShardIterators.add(remoteIterator);
                        assert remoteClusterAlias.equals(localClusterAlias) == false;
                        if (remoteClusterAlias.compareTo(localClusterAlias) < 0) {
                            expected.add(remoteIterator);
                            expected.add(localIterator);
                        } else {
                            expected.add(localIterator);
                            expected.add(remoteIterator);
                        }
                    } else {
                        expected.add(localIterator);
                    }
                } else if (rarely()) {
                    int numClusters = randomIntBetween(1, remoteClusters.length);
                    for (int k = 0; k < numClusters; k++) {
                        // simulate scenario where the same cluster is registered multiple times with different aliases
                        String clusterAlias = remoteClusters[k];
                        SearchShardIterator iterator = createSearchShardIterator(
                            i,
                            index,
                            OriginalIndicesTests.randomOriginalIndices(),
                            clusterAlias
                        );
                        expected.add(iterator);
                        remoteShardIterators.add(iterator);
                    }
                } else {
                    SearchShardIterator iterator = createSearchShardIterator(
                        i,
                        index,
                        OriginalIndicesTests.randomOriginalIndices(),
                        randomFrom(remoteClusters)
                    );
                    expected.add(iterator);
                    remoteShardIterators.add(iterator);
                }
            }
        }

        Collections.shuffle(localShardIterators, random());
        Collections.shuffle(remoteShardIterators, random());

        GroupShardsIterator<SearchShardIterator> groupShardsIterator = TransportSearchAction.mergeShardsIterators(
            localShardIterators,
            remoteShardIterators
        );
        List<SearchShardIterator> result = new ArrayList<>();
        for (SearchShardIterator searchShardIterator : groupShardsIterator) {
            result.add(searchShardIterator);
        }
        assertEquals(expected, result);
    }

    public void testProcessRemoteShards() {
        try (
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool,
                NoopTracer.INSTANCE
            )
        ) {
            RemoteClusterService service = transportService.getRemoteClusterService();
            assertFalse(service.isCrossClusterSearchEnabled());
            Map<String, ClusterSearchShardsResponse> searchShardsResponseMap = new HashMap<>();
            DiscoveryNode[] nodes = new DiscoveryNode[] {
                new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT),
                new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT) };
            Map<String, AliasFilter> indicesAndAliases = new HashMap<>();
            indicesAndAliases.put(
                "foo",
                new AliasFilter(new TermsQueryBuilder("foo", "bar"), "some_alias_for_foo", "some_other_foo_alias")
            );
            indicesAndAliases.put("bar", new AliasFilter(new MatchAllQueryBuilder(), Strings.EMPTY_ARRAY));
            ClusterSearchShardsGroup[] groups = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(
                    new ShardId("foo", "foo_id", 0),
                    new ShardRouting[] {
                        TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 0, "node2", false, ShardRoutingState.STARTED) }
                ),
                new ClusterSearchShardsGroup(
                    new ShardId("foo", "foo_id", 1),
                    new ShardRouting[] {
                        TestShardRouting.newShardRouting("foo", 0, "node1", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("foo", 1, "node2", false, ShardRoutingState.STARTED) }
                ),
                new ClusterSearchShardsGroup(
                    new ShardId("bar", "bar_id", 0),
                    new ShardRouting[] {
                        TestShardRouting.newShardRouting("bar", 0, "node2", true, ShardRoutingState.STARTED),
                        TestShardRouting.newShardRouting("bar", 0, "node1", false, ShardRoutingState.STARTED) }
                ) };
            searchShardsResponseMap.put("test_cluster_1", new ClusterSearchShardsResponse(groups, nodes, indicesAndAliases));
            DiscoveryNode[] nodes2 = new DiscoveryNode[] { new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT) };
            ClusterSearchShardsGroup[] groups2 = new ClusterSearchShardsGroup[] {
                new ClusterSearchShardsGroup(
                    new ShardId("xyz", "xyz_id", 0),
                    new ShardRouting[] { TestShardRouting.newShardRouting("xyz", 0, "node3", true, ShardRoutingState.STARTED) }
                ) };
            Map<String, AliasFilter> filter = new HashMap<>();
            filter.put("xyz", new AliasFilter(null, "some_alias_for_xyz"));
            searchShardsResponseMap.put("test_cluster_2", new ClusterSearchShardsResponse(groups2, nodes2, filter));

            Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
            remoteIndicesByCluster.put(
                "test_cluster_1",
                new OriginalIndices(new String[] { "fo*", "ba*" }, SearchRequest.DEFAULT_INDICES_OPTIONS)
            );
            remoteIndicesByCluster.put("test_cluster_2", new OriginalIndices(new String[] { "x*" }, SearchRequest.DEFAULT_INDICES_OPTIONS));
            Map<String, AliasFilter> remoteAliases = TransportSearchAction.getRemoteAliasFilters(searchShardsResponseMap);
            // a remote cluster's routings are deserialized from its _search_shards response and would otherwise become
            // collectable immediately, so they are retained only when the search opted into shard_info
            final boolean captureRoutings = randomBoolean();
            List<SearchShardIterator> iteratorList = TransportSearchAction.getRemoteShardsIterator(
                searchShardsResponseMap,
                remoteIndicesByCluster,
                remoteAliases,
                captureRoutings
            );
            assertEquals(4, iteratorList.size());
            for (SearchShardIterator iterator : iteratorList) {
                assertEquals(captureRoutings, iterator.getShardRoutings() != null);
            }
            for (SearchShardIterator iterator : iteratorList) {
                if (iterator.shardId().getIndexName().endsWith("foo")) {
                    assertArrayEquals(
                        new String[] { "some_alias_for_foo", "some_other_foo_alias" },
                        iterator.getOriginalIndices().indices()
                    );
                    assertTrue(iterator.shardId().getId() == 0 || iterator.shardId().getId() == 1);
                    assertEquals("test_cluster_1", iterator.getClusterAlias());
                    assertEquals("foo", iterator.shardId().getIndexName());
                    SearchShardTarget shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "foo");
                    shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "foo");
                    assertNull(iterator.nextOrNull());
                } else if (iterator.shardId().getIndexName().endsWith("bar")) {
                    assertArrayEquals(new String[] { "bar" }, iterator.getOriginalIndices().indices());
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("test_cluster_1", iterator.getClusterAlias());
                    assertEquals("bar", iterator.shardId().getIndexName());
                    SearchShardTarget shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "bar");
                    shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "bar");
                    assertNull(iterator.nextOrNull());
                } else if (iterator.shardId().getIndexName().endsWith("xyz")) {
                    assertArrayEquals(new String[] { "some_alias_for_xyz" }, iterator.getOriginalIndices().indices());
                    assertEquals(0, iterator.shardId().getId());
                    assertEquals("xyz", iterator.shardId().getIndexName());
                    assertEquals("test_cluster_2", iterator.getClusterAlias());
                    SearchShardTarget shard = iterator.nextOrNull();
                    assertNotNull(shard);
                    assertEquals(shard.getShardId().getIndexName(), "xyz");
                    assertNull(iterator.nextOrNull());
                }
            }
            assertEquals(3, remoteAliases.size());
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("foo_id"));
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("bar_id"));
            assertTrue(remoteAliases.toString(), remoteAliases.containsKey("xyz_id"));
            assertEquals(new TermsQueryBuilder("foo", "bar"), remoteAliases.get("foo_id").getQueryBuilder());
            assertEquals(new MatchAllQueryBuilder(), remoteAliases.get("bar_id").getQueryBuilder());
            assertNull(remoteAliases.get("xyz_id").getQueryBuilder());
        }
    }

    public void testBuildConnectionLookup() {
        Function<String, DiscoveryNode> localNodes = (nodeId) -> new DiscoveryNode(
            "local-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 1024),
            Version.CURRENT
        );
        BiFunction<String, String, DiscoveryNode> remoteNodes = (clusterAlias, nodeId) -> new DiscoveryNode(
            "remote-" + nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, 2048),
            Version.CURRENT
        );
        BiFunction<String, DiscoveryNode, Transport.Connection> nodeToConnection = (clusterAlias, node) -> new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws TransportException {}

            @Override
            public void addCloseListener(ActionListener<Void> listener) {}

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {}
        };

        {
            BiFunction<String, String, Transport.Connection> connectionLookup = TransportSearchAction.buildConnectionLookup(
                null,
                localNodes,
                remoteNodes,
                nodeToConnection
            );

            Transport.Connection localConnection = connectionLookup.apply(null, randomAlphaOfLengthBetween(5, 10));
            assertThat(localConnection.getNode().getId(), startsWith("local-"));
            Transport.Connection remoteConnection = connectionLookup.apply(
                randomAlphaOfLengthBetween(5, 10),
                randomAlphaOfLengthBetween(5, 10)
            );
            assertThat(remoteConnection.getNode().getId(), startsWith("remote-"));
        }
        {
            String requestClusterAlias = randomAlphaOfLengthBetween(5, 10);
            BiFunction<String, String, Transport.Connection> connectionLookup = TransportSearchAction.buildConnectionLookup(
                requestClusterAlias,
                localNodes,
                remoteNodes,
                nodeToConnection
            );

            Transport.Connection localConnection = connectionLookup.apply(requestClusterAlias, randomAlphaOfLengthBetween(5, 10));
            assertThat(localConnection.getNode().getId(), startsWith("local-"));
        }
    }

    private MockTransportService[] startTransport(
        int numClusters,
        DiscoveryNode[] nodes,
        Map<String, OriginalIndices> remoteIndices,
        Settings.Builder settingsBuilder
    ) {
        return startTransport(numClusters, nodes, remoteIndices, settingsBuilder, Version.CURRENT);
    }

    private MockTransportService[] startTransport(
        int numClusters,
        DiscoveryNode[] nodes,
        Map<String, OriginalIndices> remoteIndices,
        Settings.Builder settingsBuilder,
        Version remoteVersion
    ) {
        MockTransportService[] mockTransportServices = new MockTransportService[numClusters];
        for (int i = 0; i < numClusters; i++) {
            List<DiscoveryNode> knownNodes = new CopyOnWriteArrayList<>();
            MockTransportService remoteSeedTransport = RemoteClusterConnectionTests.startTransport(
                "node_remote" + i,
                knownNodes,
                remoteVersion,
                threadPool
            );
            mockTransportServices[i] = remoteSeedTransport;
            DiscoveryNode remoteSeedNode = remoteSeedTransport.getLocalDiscoNode();
            knownNodes.add(remoteSeedNode);
            nodes[i] = remoteSeedNode;
            settingsBuilder.put("cluster.remote.remote" + i + ".seeds", remoteSeedNode.getAddress().toString());
            remoteIndices.put("remote" + i, new OriginalIndices(new String[] { "index" }, IndicesOptions.lenientExpandOpen()));
        }
        return mockTransportServices;
    }

    private static SearchResponse emptySearchResponse() {
        InternalSearchResponse response = new InternalSearchResponse(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            1
        );
        return new SearchResponse(response, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY, null);
    }

    public void testCCSRemoteReduceMergeFails() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        Function<Boolean, InternalAggregation.ReduceContext> reduceContext = finalReduce -> null;
        try (
            MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.preference("null_target");
            final CountDownLatch latch = new CountDownLatch(1);
            SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                ActionListener.wrap(r -> fail("no response expected"), failure::set),
                latch
            );
            TransportSearchAction.ccsRemoteReduce(
                searchRequest,
                localIndices,
                remoteIndicesByCluster,
                timeProvider,
                emptyReduceContextBuilder(),
                remoteClusterService,
                threadPool,
                listener,
                (r, l) -> setOnce.set(Tuple.tuple(r, l)),
                new SearchRequestContext(
                    new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                    searchRequest,
                    () -> null
                )
            );
            if (localIndices == null) {
                assertNull(setOnce.get());
            } else {
                Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                assertEquals("", tuple.v1().getLocalClusterAlias());
                assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                tuple.v2().onResponse(emptySearchResponse());
            }
            awaitLatch(latch, 5, TimeUnit.SECONDS);
            assertNotNull(failure.get());
            // the intention here is not to test that we throw NPE, rather to trigger a situation that makes
            // SearchResponseMerger#getMergedResponse fail unexpectedly and verify that the listener is properly notified with the NPE
            assertThat(failure.get(), instanceOf(NullPointerException.class));
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testGetRemoteShardsIteratorSuppressesClustersThatPredateShardInfo() {
        Map<String, ClusterSearchShardsResponse> searchShardsResponseMap = new HashMap<>();
        searchShardsResponseMap.put(
            "current_cluster",
            new ClusterSearchShardsResponse(
                new ClusterSearchShardsGroup[] {
                    new ClusterSearchShardsGroup(
                        new ShardId("idx", "current_idx_uuid", 0),
                        new ShardRouting[] { TestShardRouting.newShardRouting("idx", 0, "node_current", true, ShardRoutingState.STARTED) }
                    ) },
                new DiscoveryNode[] { new DiscoveryNode("node_current", buildNewFakeTransportAddress(), Version.CURRENT) },
                null
            )
        );
        searchShardsResponseMap.put(
            "old_cluster",
            new ClusterSearchShardsResponse(
                new ClusterSearchShardsGroup[] {
                    new ClusterSearchShardsGroup(
                        new ShardId("idx", "old_idx_uuid", 0),
                        new ShardRouting[] { TestShardRouting.newShardRouting("idx", 0, "node_old", true, ShardRoutingState.STARTED) }
                    ) },
                new DiscoveryNode[] { new DiscoveryNode("node_old", buildNewFakeTransportAddress(), Version.V_3_7_0) },
                null
            )
        );

        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        remoteIndicesByCluster.put("current_cluster", new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS));
        remoteIndicesByCluster.put("old_cluster", new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS));

        List<SearchShardIterator> iterators = TransportSearchAction.getRemoteShardsIterator(
            searchShardsResponseMap,
            remoteIndicesByCluster,
            TransportSearchAction.getRemoteAliasFilters(searchShardsResponseMap),
            true
        );

        // both clusters are searched; only the one that understands the feature is described in shard_info
        assertEquals(2, iterators.size());
        for (SearchShardIterator iterator : iterators) {
            assertEquals(1, iterator.size());
            if ("current_cluster".equals(iterator.getClusterAlias())) {
                assertTrue("a remote on a supported version is described", iterator.includeInShardInfo());
                assertNotNull(iterator.getShardRoutings());
            } else {
                assertFalse("a remote older than the feature is searched but never described", iterator.includeInShardInfo());
                assertNull(iterator.getShardRoutings());
            }
        }

        // and nothing is described at all when the caller did not ask
        for (SearchShardIterator iterator : TransportSearchAction.getRemoteShardsIterator(
            searchShardsResponseMap,
            remoteIndicesByCluster,
            TransportSearchAction.getRemoteAliasFilters(searchShardsResponseMap),
            false
        )) {
            assertFalse(iterator.includeInShardInfo());
        }
    }

    public void testCCSRemoteReduceSingleClusterPreservesShardInfo() throws Exception {
        DiscoveryNode[] nodes = new DiscoveryNode[1];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(1, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        try (
            MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.shardInfo(true);
            final CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
            AtomicReference<Exception> failureRef = new AtomicReference<>();
            LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                ActionListener.wrap(responseRef::set, failureRef::set),
                latch
            );
            TransportSearchAction.ccsRemoteReduce(
                searchRequest,
                null,
                remoteIndicesByCluster,
                timeProvider,
                emptyReduceContextBuilder(),
                remoteClusterService,
                threadPool,
                listener,
                (r, l) -> fail("no local search expected for a single-remote-cluster search"),
                new SearchRequestContext(
                    new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                    searchRequest,
                    () -> null
                )
            );
            awaitLatch(latch, 5, TimeUnit.SECONDS);
            assertNull(failureRef.get());
            SearchResponse response = responseRef.get();
            assertNotNull(response);
            SearchShardInfo shardInfo = response.getShardInfo();
            assertNotNull("the single-remote rewrap must preserve shard_info from the remote response", shardInfo);
            assertEquals(1, shardInfo.getSuccessful().size());
            SearchShardInfo.Entry entry = shardInfo.getSuccessful().get(0);
            assertEquals("index", entry.getIndex());
            assertEquals("remote_node", entry.getNodeId());
            assertEquals(Boolean.TRUE, entry.getPrimary());
            // the remote stamped its own attribution and the rewrap carried it through untouched, rather than
            // overwriting it with the alias this coordinator knows the cluster by
            assertEquals(RemoteClusterConnectionTests.REMOTE_STAMPED_CLUSTER_ALIAS, entry.getCluster());
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCCSRemoteReduceSingleClusterOmitsShardInfoWhenRemoteReportsNone() throws Exception {
        DiscoveryNode[] nodes = new DiscoveryNode[1];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        // a remote that predates the feature: the request flag is version gated on the wire, so it never arrives there
        MockTransportService[] mockTransportServices = startTransport(1, nodes, remoteIndicesByCluster, builder, Version.V_3_7_0);
        Settings settings = builder.build();
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        try (
            MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.shardInfo(true);
            final CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
            AtomicReference<Exception> failureRef = new AtomicReference<>();
            LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                ActionListener.wrap(responseRef::set, failureRef::set),
                latch
            );
            TransportSearchAction.ccsRemoteReduce(
                searchRequest,
                null,
                remoteIndicesByCluster,
                timeProvider,
                emptyReduceContextBuilder(),
                remoteClusterService,
                threadPool,
                listener,
                (r, l) -> fail("no local search expected for a single-remote-cluster search"),
                new SearchRequestContext(
                    new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                    searchRequest,
                    () -> null
                )
            );
            awaitLatch(latch, 5, TimeUnit.SECONDS);
            assertNull(failureRef.get());
            SearchResponse response = responseRef.get();
            assertNotNull(response);
            // the caller opted in, but a remote that predates the feature reports nothing, so the key is omitted
            assertNull(response.getShardInfo());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCCSRemoteReduce() throws Exception {
        int numClusters = randomIntBetween(1, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        boolean local = randomBoolean();
        OriginalIndices localIndices = local ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS) : null;
        int totalClusters = numClusters + (local ? 1 : 0);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        try (
            MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l)),
                    new SearchRequestContext(
                        new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                        searchRequest,
                        () -> null
                    )
                );
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                assertEquals(0, searchResponse.getClusters().getSkipped());
                assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                assertEquals(totalClusters, searchResponse.getClusters().getSuccessful());
                assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
            }
            {
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.preference("index_not_found");
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<Exception> failure = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(r -> fail("no response expected"), failure::set),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l)),
                    new SearchRequestContext(
                        new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                        searchRequest,
                        () -> null
                    )
                );
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertNotNull(failure.get());
                assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                RemoteTransportException remoteTransportException = (RemoteTransportException) failure.get();
                assertEquals(RestStatus.NOT_FOUND, remoteTransportException.status());
            }

            int numDisconnectedClusters = randomIntBetween(1, numClusters);
            Set<DiscoveryNode> disconnectedNodes = new HashSet<>(numDisconnectedClusters);
            Set<Integer> disconnectedNodesIndices = new HashSet<>(numDisconnectedClusters);
            while (disconnectedNodes.size() < numDisconnectedClusters) {
                int i = randomIntBetween(0, numClusters - 1);
                if (disconnectedNodes.add(nodes[i])) {
                    assertTrue(disconnectedNodesIndices.add(i));
                }
            }

            CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
            RemoteClusterServiceTests.addConnectionListener(remoteClusterService, new TransportConnectionListener() {
                @Override
                public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                    if (disconnectedNodes.remove(node)) {
                        disconnectedLatch.countDown();
                    }
                }
            });
            for (DiscoveryNode disconnectedNode : disconnectedNodes) {
                service.addFailToSendNoConnectRule(disconnectedNode.getAddress());
            }

            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<Exception> failure = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(r -> fail("no response expected"), failure::set),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l)),
                    new SearchRequestContext(
                        new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                        searchRequest,
                        () -> null
                    )
                );
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertNotNull(failure.get());
                assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                assertThat(failure.get().getMessage(), containsString("error while communicating with remote cluster ["));
                assertThat(failure.get().getCause(), instanceOf(NodeDisconnectedException.class));
            }

            // setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
            for (int i : disconnectedNodesIndices) {
                RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
            }

            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l)),
                    new SearchRequestContext(
                        new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                        searchRequest,
                        () -> null
                    )
                );
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                assertEquals(disconnectedNodesIndices.size(), searchResponse.getClusters().getSkipped());
                assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                int successful = totalClusters - disconnectedNodesIndices.size();
                assertEquals(successful, searchResponse.getClusters().getSuccessful());
                assertEquals(successful == 0 ? 0 : successful + 1, searchResponse.getNumReducePhases());
            }

            // give transport service enough time to realize that the node is down, and to notify the connection listeners
            // so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
            assertTrue(disconnectedLatch.await(5, TimeUnit.SECONDS));

            service.clearAllRules();
            if (randomBoolean()) {
                for (int i : disconnectedNodesIndices) {
                    if (randomBoolean()) {
                        RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
                    }

                }
            }
            {
                SearchRequest searchRequest = new SearchRequest();
                final CountDownLatch latch = new CountDownLatch(1);
                SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> setOnce = new SetOnce<>();
                AtomicReference<SearchResponse> response = new AtomicReference<>();
                LatchedActionListener<SearchResponse> listener = new LatchedActionListener<>(
                    ActionListener.wrap(response::set, e -> fail("no failures expected")),
                    latch
                );
                TransportSearchAction.ccsRemoteReduce(
                    searchRequest,
                    localIndices,
                    remoteIndicesByCluster,
                    timeProvider,
                    emptyReduceContextBuilder(),
                    remoteClusterService,
                    threadPool,
                    listener,
                    (r, l) -> setOnce.set(Tuple.tuple(r, l)),
                    new SearchRequestContext(
                        new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                        searchRequest,
                        () -> null
                    )
                );
                if (localIndices == null) {
                    assertNull(setOnce.get());
                } else {
                    Tuple<SearchRequest, ActionListener<SearchResponse>> tuple = setOnce.get();
                    assertEquals("", tuple.v1().getLocalClusterAlias());
                    assertThat(tuple.v2(), instanceOf(TransportSearchAction.CCSActionListener.class));
                    tuple.v2().onResponse(emptySearchResponse());
                }
                awaitLatch(latch, 5, TimeUnit.SECONDS);

                SearchResponse searchResponse = response.get();
                assertEquals(0, searchResponse.getClusters().getSkipped());
                assertEquals(totalClusters, searchResponse.getClusters().getTotal());
                assertEquals(totalClusters, searchResponse.getClusters().getSuccessful());
                assertEquals(totalClusters == 1 ? 1 : totalClusters + 1, searchResponse.getNumReducePhases());
            }
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCollectSearchShards() throws Exception {
        int numClusters = randomIntBetween(2, 10);
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        try (
            MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                AtomicInteger skippedClusters = new AtomicInteger();
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    null,
                    new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(0, skippedClusters.get());
                assertNotNull(response.get());
                Map<String, ClusterSearchShardsResponse> map = response.get();
                assertEquals(numClusters, map.size());
                for (int i = 0; i < numClusters; i++) {
                    String clusterAlias = "remote" + i;
                    assertTrue(map.containsKey(clusterAlias));
                    ClusterSearchShardsResponse shardsResponse = map.get(clusterAlias);
                    assertEquals(1, shardsResponse.getNodes().length);
                }
            }
            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicReference<Exception> failure = new AtomicReference<>();
                AtomicInteger skippedClusters = new AtomicInteger(0);
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    "index_not_found",
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    null,
                    new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(0, skippedClusters.get());
                assertNotNull(failure.get());
                assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                RemoteTransportException remoteTransportException = (RemoteTransportException) failure.get();
                assertEquals(RestStatus.NOT_FOUND, remoteTransportException.status());
            }

            int numDisconnectedClusters = randomIntBetween(1, numClusters);
            Set<DiscoveryNode> disconnectedNodes = new HashSet<>(numDisconnectedClusters);
            Set<Integer> disconnectedNodesIndices = new HashSet<>(numDisconnectedClusters);
            while (disconnectedNodes.size() < numDisconnectedClusters) {
                int i = randomIntBetween(0, numClusters - 1);
                if (disconnectedNodes.add(nodes[i])) {
                    assertTrue(disconnectedNodesIndices.add(i));
                }
            }

            CountDownLatch disconnectedLatch = new CountDownLatch(numDisconnectedClusters);
            RemoteClusterServiceTests.addConnectionListener(remoteClusterService, new TransportConnectionListener() {
                @Override
                public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                    if (disconnectedNodes.remove(node)) {
                        disconnectedLatch.countDown();
                    }
                }
            });
            for (DiscoveryNode disconnectedNode : disconnectedNodes) {
                service.addFailToSendNoConnectRule(disconnectedNode.getAddress());
            }

            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger skippedClusters = new AtomicInteger(0);
                AtomicReference<Exception> failure = new AtomicReference<>();
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    null,
                    new LatchedActionListener<>(ActionListener.wrap(r -> fail("no response expected"), failure::set), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(0, skippedClusters.get());
                assertNotNull(failure.get());
                assertThat(failure.get(), instanceOf(RemoteTransportException.class));
                assertThat(failure.get().getMessage(), containsString("error while communicating with remote cluster ["));
                assertThat(failure.get().getCause(), instanceOf(NodeDisconnectedException.class));
            }

            // setting skip_unavailable to true for all the disconnected clusters will make the request succeed again
            for (int i : disconnectedNodesIndices) {
                RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
            }

            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger skippedClusters = new AtomicInteger(0);
                AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    null,
                    new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertNotNull(response.get());
                Map<String, ClusterSearchShardsResponse> map = response.get();
                assertEquals(numClusters - disconnectedNodesIndices.size(), map.size());
                assertEquals(skippedClusters.get(), disconnectedNodesIndices.size());
                for (int i = 0; i < numClusters; i++) {
                    String clusterAlias = "remote" + i;
                    if (disconnectedNodesIndices.contains(i)) {
                        assertFalse(map.containsKey(clusterAlias));
                    } else {
                        assertNotNull(map.get(clusterAlias));
                    }
                }
            }

            // give transport service enough time to realize that the node is down, and to notify the connection listeners
            // so that RemoteClusterConnection is left with no connected nodes, hence it will retry connecting next
            assertTrue(disconnectedLatch.await(5, TimeUnit.SECONDS));

            service.clearAllRules();
            if (randomBoolean()) {
                for (int i : disconnectedNodesIndices) {
                    if (randomBoolean()) {
                        RemoteClusterServiceTests.updateSkipUnavailable(remoteClusterService, "remote" + i, true);
                    }

                }
            }
            {
                final CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger skippedClusters = new AtomicInteger(0);
                AtomicReference<Map<String, ClusterSearchShardsResponse>> response = new AtomicReference<>();
                TransportSearchAction.collectSearchShards(
                    IndicesOptions.lenientExpandOpen(),
                    null,
                    null,
                    skippedClusters,
                    remoteIndicesByCluster,
                    remoteClusterService,
                    threadPool,
                    null,
                    new LatchedActionListener<>(ActionListener.wrap(response::set, e -> fail("no failures expected")), latch)
                );
                awaitLatch(latch, 5, TimeUnit.SECONDS);
                assertEquals(0, skippedClusters.get());
                assertNotNull(response.get());
                Map<String, ClusterSearchShardsResponse> map = response.get();
                assertEquals(numClusters, map.size());
                for (int i = 0; i < numClusters; i++) {
                    String clusterAlias = "remote" + i;
                    assertTrue(map.containsKey(clusterAlias));
                    assertNotNull(map.get(clusterAlias));
                }
            }
            assertEquals(0, service.getConnectionManager().size());
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    public void testCreateSearchResponseMerger() {
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            assertEquals(-1, source.size());
            assertEquals(-1, source.from());
            assertNull(source.trackTotalHitsUpTo());
            SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                source,
                timeProvider,
                emptyReduceContextBuilder()
            );
            assertEquals(0, merger.from);
            assertEquals(10, merger.size);
            assertEquals(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, merger.trackTotalHitsUpTo);
            assertEquals(0, source.from());
            assertEquals(10, source.size());
            assertNull(source.trackTotalHitsUpTo());
        }
        {
            SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(null, timeProvider, emptyReduceContextBuilder());
            assertEquals(0, merger.from);
            assertEquals(10, merger.size);
            assertEquals(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO, merger.trackTotalHitsUpTo);
        }
        {
            SearchSourceBuilder source = new SearchSourceBuilder();
            int originalFrom = randomIntBetween(0, 1000);
            source.from(originalFrom);
            int originalSize = randomIntBetween(0, 1000);
            source.size(originalSize);
            int trackTotalHitsUpTo = randomIntBetween(0, Integer.MAX_VALUE);
            source.trackTotalHitsUpTo(trackTotalHitsUpTo);
            SearchResponseMerger merger = TransportSearchAction.createSearchResponseMerger(
                source,
                timeProvider,
                emptyReduceContextBuilder()
            );
            assertEquals(0, source.from());
            assertEquals(originalFrom + originalSize, source.size());
            assertEquals(trackTotalHitsUpTo, (int) source.trackTotalHitsUpTo());
            assertEquals(originalFrom, merger.from);
            assertEquals(originalSize, merger.size);
            assertEquals(trackTotalHitsUpTo, merger.trackTotalHitsUpTo);
        }
    }

    public void testShouldMinimizeRoundtrips() throws Exception {
        {
            SearchRequest searchRequest = new SearchRequest();
            assertTrue(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder());
            assertTrue(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.scroll("5s");
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder source = new SearchSourceBuilder();
            searchRequest.source(source);
            CollapseBuilder collapseBuilder = new CollapseBuilder("field");
            source.collapse(collapseBuilder);
            collapseBuilder.setInnerHits(new InnerHitBuilder("inner"));
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
        {
            SearchRequestTests searchRequestTests = new SearchRequestTests();
            searchRequestTests.setUp();
            SearchRequest searchRequest = searchRequestTests.createSearchRequest();
            searchRequest.scroll((Scroll) null);
            searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
            SearchSourceBuilder source = searchRequest.source();
            if (source != null) {
                source.pointInTimeBuilder(null);
                CollapseBuilder collapse = source.collapse();
                if (collapse != null) {
                    collapse.setInnerHits(Collections.emptyList());
                }
            }
            searchRequest.setCcsMinimizeRoundtrips(true);
            assertTrue(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
            searchRequest.setCcsMinimizeRoundtrips(false);
            assertFalse(TransportSearchAction.shouldMinimizeRoundtrips(searchRequest));
        }
    }

    public void testShouldPreFilterSearchShards() {
        int numIndices = randomIntBetween(2, 10);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 128)));
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(129, 10000))
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 128)));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(129, 10000))
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")));
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 127)));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(127, 10000))
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().sort(SortBuilders.fieldSort("timestamp")))
                .scroll("5m");
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 128)));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(129, 10000))
            );
        }
    }

    public void testShouldPreFilterSearchShardsWithReadOnly() {
        int numIndices = randomIntBetween(2, 10);
        int numReadOnly = randomIntBetween(1, numIndices);
        String[] indices = new String[numIndices];
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder();
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
            ;
            if (--numReadOnly >= 0) {
                if (randomBoolean()) {
                    blocksBuilder.addIndexBlock(indices[i], IndexMetadata.INDEX_WRITE_BLOCK);
                } else {
                    blocksBuilder.addIndexBlock(indices[i], IndexMetadata.INDEX_READ_ONLY_BLOCK);
                }
            }
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).blocks(blocksBuilder).build();
        {
            SearchRequest searchRequest = new SearchRequest();
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 127)));
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(127, 10000))
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 127)));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(127, 10000))
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            searchRequest.scroll("5s");
            assertTrue(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 127)));
            assertTrue(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(127, 10000))
            );
        }
        {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().query(QueryBuilders.rangeQuery("timestamp"))
            );
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            assertFalse(TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(2, 127)));
            assertFalse(
                TransportSearchAction.shouldPreFilterSearchShards(clusterState, searchRequest, indices, randomIntBetween(127, 10000))
            );
        }
    }

    public void testUnexpectedAdmissionExceptionTakesTheListenerPath() {
        IllegalStateException boom = new IllegalStateException("boom");
        AtomicReference<Exception> failure = new AtomicReference<>();
        newSearchAction(new SearchExecutionGate((task, request, listener) -> { throw boom; })).executeRequest(
            newSearchTask(),
            new SearchRequest("index"),
            null,
            ActionListener.wrap(r -> fail("must not succeed"), failure::set)
        );
        assertSame("an exception thrown by admission must be delivered to the listener", boom, failure.get());
    }

    public void testSearchSkipsAdmissionWhileGateIsInactive() {
        IllegalStateException boom = new IllegalStateException("boom");
        AtomicBoolean admitted = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        newSearchAction(new SearchExecutionGate(new SearchAdmission() {
            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
                admitted.set(true);
                throw boom;
            }
        })).executeRequest(
            newSearchTask(),
            new SearchRequest("index"),
            null,
            ActionListener.wrap(r -> fail("must not succeed"), failure::set)
        );
        assertFalse("an inactive admission must not be consulted", admitted.get());
        // The search itself still ran; with these mocks it fails past admission, never with the admission exception.
        assertNotNull(failure.get());
        assertNotSame(boom, failure.get());
    }

    public void testResolverDecidesTheCancelAfterTimeInterval() {
        SearchRequest request = new SearchRequest("index");
        TimeValue requested = randomFrom(TimeValue.timeValueHours(1), SearchService.NO_TIMEOUT, null);
        request.setCancelAfterTimeInterval(requested);
        SearchTask task = request.createTask(1, "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        TimeValue resolved = randomFrom(TimeValue.timeValueSeconds(1), SearchService.NO_TIMEOUT, null);
        TimeValue interval = TransportSearchAction.resolveCancelAfterTimeInterval((resolveTask, resolveRequest, resolveRequested) -> {
            assertSame(task, resolveTask);
            assertSame(request, resolveRequest);
            assertEquals(requested, resolveRequested);
            return resolved;
        }, task, request);
        assertEquals(resolved, interval);
        assertEquals(
            "the timer of a timeout that came from the request takes the resolved interval",
            resolved,
            TransportSearchAction.cancellationTimerInterval(task, resolved)
        );
        assertEquals(
            requested,
            TransportSearchAction.resolveCancelAfterTimeInterval(SearchCancelAfterTimeIntervalResolver.NOOP, task, request)
        );
    }

    public void testOptedOutTaskKeepsItsTimeoutWhenTheRequestSetsMinusOne() throws IOException {
        // Asynchronous search, which has its own cancellation mechanism, wraps the caller's request in one whose createTask builds
        // its SearchTask without a timeout, which opts out with SearchService.NO_TIMEOUT. That task's timeout did not come from the
        // request, even when the request sets the same -1, so no resolved interval replaces it, not even the interval of a group that
        // overrides request values.
        SearchRequest callerRequest = searchRequestFromRest("-1");
        assertEquals(SearchService.NO_TIMEOUT, callerRequest.getCancelAfterTimeInterval());
        SearchRequest request = new SearchRequest(callerRequest) {
            @Override
            public SearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new SearchTask(id, type, action, () -> "asynchronous search", parentTaskId, headers);
            }
        };
        SearchTask task = request.createTask(1, "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        assertFalse(task.isCancellationTimeoutFromRequest());
        TimeValue resolved = TimeValue.timeValueSeconds(1);
        assertSame(SearchService.NO_TIMEOUT, TransportSearchAction.cancellationTimerInterval(task, resolved));
    }

    public void testResolverDecidesAMinusOneThatCameFromTheRequest() throws IOException {
        // The other side of the opt-out: SearchRequest#createTask takes the task's -1 from the request, so a resolved interval, such
        // as that of a group that overrides request values, replaces it on the timer too.
        SearchRequest request = searchRequestFromRest("-1");
        SearchTask task = request.createTask(1, "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        assertTrue(task.isCancellationTimeoutFromRequest());
        TimeValue resolved = TimeValue.timeValueSeconds(1);
        assertEquals(TimeValue.timeValueSeconds(1), TransportSearchAction.cancellationTimerInterval(task, resolved));
    }

    public void testTaskThatChoseItsOwnTimeoutKeepsIt() {
        // A createTask that builds its own SearchTask keeps the timeout it chose, even when it passes the request's own interval.
        TimeValue resolved = TimeValue.timeValueSeconds(1);
        SearchRequest request = new SearchRequest("index");
        request.setCancelAfterTimeInterval(TimeValue.timeValueMinutes(5));
        SearchTask chosenTask = new SearchTask(
            1,
            "transport",
            SearchAction.NAME,
            () -> "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            request.getCancelAfterTimeInterval()
        );
        assertFalse(chosenTask.isCancellationTimeoutFromRequest());
        assertEquals(TimeValue.timeValueMinutes(5), TransportSearchAction.cancellationTimerInterval(chosenTask, resolved));
    }

    public void testTaskThatChoseItsOwnTimeoutStillGetsTheGroupIntervalOnItsRequest() {
        // Pins behavior that predates the resolver: when createTask opted out of timeout-based cancellation, the coordinator arms
        // no timer, yet the group's interval is still written onto the request, which carries it to the remote clusters of a
        // ccs_minimize_roundtrips search.
        Settings overridingGroup = Settings.builder().put(WLM_CANCEL_AFTER, "1s").put(WLM_OVERRIDE, true).build();
        for (TimeValue requested : Arrays.asList(null, TimeValue.timeValueHours(1), TimeValue.MINUS_ONE)) {
            SearchRequest request = new SearchRequest("index");
            request.setCancelAfterTimeInterval(requested);
            SearchTask task = new SearchTask(
                1,
                "transport",
                SearchAction.NAME,
                () -> "test",
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap(),
                SearchService.NO_TIMEOUT
            );
            assertCancelAfterTimeInterval(task, request, GROUP_ID, overridingGroup, "-1", null, TimeValue.timeValueSeconds(1));
        }
    }

    public void testCancellationTimerIsArmedBeforeAdmission() {
        // The coordinator arms its timer with the resolver's interval before the search waits for admission, so the time spent in
        // admission counts toward the interval, and a timer that fires there cancels the search's task. The request carries the same
        // interval from then on.
        TimeValue groupInterval = TimeValue.timeValueSeconds(1);
        List<Scheduled> scheduled = new CopyOnWriteArrayList<>();
        ThreadPool timerThreadPool = timerThreadPool(scheduled);
        CancelTasksRecordingClient client = new CancelTasksRecordingClient(timerThreadPool);
        AtomicReference<List<Scheduled>> scheduledAtAdmission = new AtomicReference<>();
        AtomicReference<TimeValue> intervalAtAdmission = new AtomicReference<>();
        AtomicReference<ActionListener<Releasable>> waitingForAdmission = new AtomicReference<>();
        SearchAdmission admission = new SearchAdmission() {
            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
                scheduledAtAdmission.set(List.copyOf(scheduled));
                intervalAtAdmission.set(((SearchRequest) request).getCancelAfterTimeInterval());
                // Holds the search in admission, as a queue or a slow cluster-wide permit would.
                waitingForAdmission.set(listener);
            }
        };
        TransportSearchAction action = newSearchAction(
            new SearchExecutionGate(admission),
            (task, request, requested) -> groupInterval,
            client,
            timerThreadPool,
            Settings.EMPTY
        );
        // Neither the request nor the cluster sets an interval, so only the resolver's can arm the timer.
        SearchRequest request = new SearchRequest("index");
        SearchTask task = request.createTask(
            randomNonNegativeLong(),
            "transport",
            SearchAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        AtomicInteger completions = new AtomicInteger();
        action.doExecute(task, request, ActionListener.wrap(r -> completions.incrementAndGet(), e -> completions.incrementAndGet()));

        assertNotNull("the search must wait for admission", waitingForAdmission.get());
        assertEquals("the timer must be armed before admission", 1, scheduledAtAdmission.get().size());
        Scheduled timer = scheduledAtAdmission.get().get(0);
        assertEquals(groupInterval, timer.delay());
        assertEquals(ThreadPool.Names.GENERIC, timer.executor());
        assertEquals("the request must carry the timer's interval before admission", groupInterval, intervalAtAdmission.get());

        // The timer fires while the search still waits for admission, and asks to cancel the search's task.
        timer.command().run();
        assertEquals(1, client.cancelTasksRequests.size());
        CancelTasksRequest cancelTasksRequest = client.cancelTasksRequests.get(0);
        assertEquals(new TaskId(LOCAL_NODE_ID, task.getId()), cancelTasksRequest.getTaskId());
        assertEquals("Cancellation timeout of " + groupInterval + " is expired", cancelTasksRequest.getReason());
        assertEquals("the search must not complete before it is admitted", 0, completions.get());

        // Once admitted, the search completes exactly once (with these mocks it fails past admission) and returns its permit, and
        // nothing arms a second timer.
        AtomicInteger releases = new AtomicInteger();
        waitingForAdmission.get().onResponse(releases::incrementAndGet);
        assertEquals(1, completions.get());
        assertEquals("the permit must be released", 1, releases.get());
        assertEquals("only doExecute arms a timer", 1, scheduled.size());
    }

    public void testCancelAfterTimeIntervalPrecedence() {
        // Through doExecute and the WLM resolver that Node wires: by default the request's interval beats its group's, which beats
        // the cluster's; with override_request_values the group's beats the request's. Exactly -1 turns cancellation off at
        // whichever level wins. The coordinator's timer and the request agree in every case.
        Settings group = Settings.builder().put(WLM_CANCEL_AFTER, "30s").build();
        Settings overridingGroup = Settings.builder().put(WLM_CANCEL_AFTER, "30s").put(WLM_OVERRIDE, true).build();
        Settings disablingGroup = Settings.builder().put(WLM_CANCEL_AFTER, "-1").build();
        Settings overridingDisablingGroup = Settings.builder().put(WLM_CANCEL_AFTER, "-1").put(WLM_OVERRIDE, true).build();
        TimeValue tenSeconds = TimeValue.timeValueSeconds(10);
        TimeValue thirtySeconds = TimeValue.timeValueSeconds(30);
        TimeValue off = TimeValue.MINUS_ONE;
        for (String cluster : Arrays.asList("-1", "1h")) {
            TimeValue clusterTimer = "-1".equals(cluster) ? null : TimeValue.timeValueHours(1);
            // Guards existing behavior: a search that names no workload group keeps the request's interval, else the cluster's,
            // even where a group would override it.
            assertUntaggedCancelAfterTimeInterval(null, cluster, clusterTimer);
            assertUntaggedCancelAfterTimeInterval(tenSeconds, cluster, tenSeconds);
            assertUntaggedCancelAfterTimeInterval(off, cluster, null);
            // Guards existing behavior: without the group's setting, the request's interval applies, else the cluster's.
            assertCancelAfterTimeInterval(null, Settings.EMPTY, cluster, clusterTimer, null);
            assertCancelAfterTimeInterval(tenSeconds, Settings.EMPTY, cluster, tenSeconds, tenSeconds);
            assertCancelAfterTimeInterval(off, Settings.EMPTY, cluster, null, off);
            // The group's interval applies to a request that sets none, whatever the cluster's.
            assertCancelAfterTimeInterval(null, group, cluster, thirtySeconds, thirtySeconds);
            assertCancelAfterTimeInterval(null, disablingGroup, cluster, null, off);
            // A request's own interval beats its group's...
            assertCancelAfterTimeInterval(tenSeconds, group, cluster, tenSeconds, tenSeconds);
            assertCancelAfterTimeInterval(off, group, cluster, null, off);
            assertCancelAfterTimeInterval(tenSeconds, disablingGroup, cluster, tenSeconds, tenSeconds);
            // ...unless the group overrides request values.
            assertCancelAfterTimeInterval(tenSeconds, overridingGroup, cluster, thirtySeconds, thirtySeconds);
            assertCancelAfterTimeInterval(off, overridingGroup, cluster, thirtySeconds, thirtySeconds);
            assertCancelAfterTimeInterval(tenSeconds, overridingDisablingGroup, cluster, null, off);
        }
    }

    public void testCreatePitArmsNoCancellationTimer() {
        // Guards existing behavior: creating a point in time runs its search on an unregistered SearchTask that reuses the id of
        // the create request's own task, which can't be cancelled, through executeRequest, which skips doExecute. So neither the
        // resolver nor the cluster setting arms a timer, a group's interval can't cancel the creation, and no cancel-tasks request
        // targets that id.
        List<Scheduled> scheduled = new CopyOnWriteArrayList<>();
        ThreadPool timerThreadPool = timerThreadPool(scheduled);
        AtomicBoolean consulted = new AtomicBoolean();
        TransportSearchAction action = newSearchAction(
            SearchExecutionGate.NOOP,
            oneSecondResolver(consulted),
            new CancelTasksRecordingClient(timerThreadPool),
            timerThreadPool,
            Settings.builder().put(TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, "1s").build()
        );
        CreatePitController createPitController = new CreatePitController(
            mock(SearchTransportService.class),
            mock(ClusterService.class),
            action,
            new NamedWriteableRegistry(Collections.emptyList()),
            mock(PitService.class)
        );
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueMinutes(1), true, "index");
        Task createPitTask = createPitRequest.createTask(
            randomNonNegativeLong(),
            "transport",
            CreatePitAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        assertFalse(createPitTask instanceof CancellableTask);
        AtomicReference<Exception> failure = new AtomicReference<>();
        createPitController.executeCreatePit(
            createPitRequest,
            createPitTask,
            new StepListener<>(),
            ActionListener.wrap(r -> fail("must not succeed"), failure::set)
        );
        // With these mocks the search fails past admission, after the point where doExecute would have armed a timer.
        assertNotNull(failure.get());
        assertFalse("creating a point in time must not consult the resolver", consulted.get());
        assertEquals("creating a point in time must not arm a cancellation timer", List.of(), scheduled);
    }

    public void testResolverThatThrowsFallsBackToTheTaskTimeout() throws Exception {
        // A resolver that throws, as the workload group resolver does on a failed lookup, must not fail the search: doExecute logs a
        // warning naming the task, leaves the request's interval as it was, and arms the timer with the task's own cancellation
        // timeout, exactly as without a resolver, or with the cluster's interval when the task has none.
        IllegalStateException boom = new IllegalStateException("boom");
        TimeValue clusterInterval = TimeValue.timeValueMinutes(30);
        for (TimeValue requested : Arrays.asList(TimeValue.timeValueHours(1), null)) {
            String cell = "requested [" + requested + "]";
            List<Scheduled> scheduled = new CopyOnWriteArrayList<>();
            ThreadPool timerThreadPool = timerThreadPool(scheduled);
            AtomicBoolean admitted = new AtomicBoolean();
            SearchAdmission admission = new SearchAdmission() {
                @Override
                public boolean isActive() {
                    return true;
                }

                @Override
                public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
                    admitted.set(true);
                    listener.onResponse(() -> {});
                }
            };
            TransportSearchAction action = newSearchAction(
                new SearchExecutionGate(admission),
                (task, request, resolveRequested) -> { throw boom; },
                new CancelTasksRecordingClient(timerThreadPool),
                timerThreadPool,
                Settings.builder().put(TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, clusterInterval).build()
            );
            SearchRequest request = new SearchRequest("index");
            request.setCancelAfterTimeInterval(requested);
            SearchTask task = request.createTask(
                randomNonNegativeLong(),
                "transport",
                SearchAction.NAME,
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            List<Exception> failures = new CopyOnWriteArrayList<>();
            try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportSearchAction.class))) {
                appender.addExpectation(
                    new MockLogAppender.ExceptionSeenEventExpectation(
                        "resolver failure",
                        TransportSearchAction.class.getCanonicalName(),
                        Level.WARN,
                        "task [" + task.getId() + "]",
                        IllegalStateException.class,
                        "boom"
                    )
                );
                action.execute(task, request, ActionListener.wrap(r -> fail("must not succeed"), failures::add));
                assertTrue(cell + ": the search must run", admitted.get());
                // With these mocks the search fails past admission, never with the resolver's exception.
                assertEquals(cell, 1, failures.size());
                assertNotSame(cell, boom, failures.get(0));
                assertEquals(cell, 1, scheduled.size());
                assertEquals(cell, requested == null ? clusterInterval : requested, scheduled.get(0).delay());
                assertSame(cell, requested, request.getCancelAfterTimeInterval());
                appender.assertAllExpectationsMatched();
            }
        }
    }

    public void testTaskThatCannotBeCancelledArmsNoTimer() {
        // Guards existing behavior: a SearchRequest whose createTask returns a task that can't be cancelled opts out of
        // timeout-based cancellation altogether, so neither its own interval nor its group's arms a timer. The resolved interval
        // still reaches the request.
        List<Scheduled> scheduled = new CopyOnWriteArrayList<>();
        ThreadPool timerThreadPool = timerThreadPool(scheduled);
        TransportSearchAction action = newSearchAction(
            SearchExecutionGate.NOOP,
            (resolveTask, resolveRequest, requested) -> TimeValue.timeValueMinutes(1),
            new CancelTasksRecordingClient(timerThreadPool),
            timerThreadPool,
            Settings.EMPTY
        );
        SearchRequest request = new SearchRequest("index");
        request.setCancelAfterTimeInterval(TimeValue.timeValueSeconds(1));
        Task task = new Task(1, "transport", SearchAction.NAME, "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        AtomicReference<Exception> failure = new AtomicReference<>();
        action.doExecute(task, request, ActionListener.wrap(r -> fail("must not succeed"), failure::set));
        // With these mocks the search fails past admission.
        assertNotNull(failure.get());
        assertEquals(List.of(), scheduled);
        assertEquals(TimeValue.timeValueMinutes(1), request.getCancelAfterTimeInterval());
    }

    public void testStreamingSearchArmsTheResolvedTimer() {
        // StreamTransportSearchAction inherits doExecute, so a streaming search gets its group's interval too.
        List<Scheduled> scheduled = new CopyOnWriteArrayList<>();
        ThreadPool timerThreadPool = timerThreadPool(scheduled);
        TransportSearchAction action = newSearchAction(
            SearchExecutionGate.NOOP,
            (task, request, requested) -> TimeValue.timeValueSeconds(1),
            new CancelTasksRecordingClient(timerThreadPool),
            timerThreadPool,
            Settings.EMPTY,
            true
        );
        assertTrue(action instanceof StreamTransportSearchAction);
        SearchRequest request = new SearchRequest("index");
        SearchTask task = request.createTask(1, "transport", StreamSearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        action.doExecute(task, request, ActionListener.wrap(r -> {}, e -> {}));
        assertEquals(1, scheduled.size());
        assertEquals(TimeValue.timeValueSeconds(1), scheduled.get(0).delay());
    }

    public void testRemoteLegsCarryTheCancelAfterTimeIntervalOfTheRequest() throws Exception {
        // Guards existing behavior: each leg of a ccs_minimize_roundtrips search copies the request's interval, which doExecute has
        // settled by then, a workload group's included, so each remote cluster arms its own timer with it.
        int numClusters = 2;
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        List<TimeValue> forwarded = new CopyOnWriteArrayList<>();
        for (MockTransportService remote : mockTransportServices) {
            remote.<SearchRequest>addRequestHandlingBehavior(SearchAction.NAME, (handler, request, channel, task) -> {
                forwarded.add(request.getCancelAfterTimeInterval());
                handler.messageReceived(request, channel, task);
            });
        }
        TimeValue interval = TimeValue.timeValueSeconds(1);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        try (
            MockTransportService service = MockTransportService.createNewService(
                builder.build(),
                Version.CURRENT,
                threadPool,
                NoopTracer.INSTANCE
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            // A single remote cluster without local indices gets the request on its own path, and every other combination on the
            // general one; each leg gets its own copy (SearchRequest#subSearchRequest) either way.
            for (boolean singleRemote : new boolean[] { true, false }) {
                for (boolean withLocalIndices : new boolean[] { false, true }) {
                    forwarded.clear();
                    Map<String, OriginalIndices> remoteIndices = singleRemote
                        ? Map.of("remote0", remoteIndicesByCluster.get("remote0"))
                        : remoteIndicesByCluster;
                    OriginalIndices localIndices = withLocalIndices
                        ? new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS)
                        : null;
                    SearchRequest searchRequest = new SearchRequest();
                    searchRequest.setCancelAfterTimeInterval(interval);
                    CountDownLatch latch = new CountDownLatch(1);
                    SetOnce<Tuple<SearchRequest, ActionListener<SearchResponse>>> localLeg = new SetOnce<>();
                    AtomicReference<Exception> failure = new AtomicReference<>();
                    TransportSearchAction.ccsRemoteReduce(
                        searchRequest,
                        localIndices,
                        remoteIndices,
                        timeProvider,
                        emptyReduceContextBuilder(),
                        remoteClusterService,
                        threadPool,
                        new LatchedActionListener<>(ActionListener.wrap(r -> {}, failure::set), latch),
                        (r, l) -> localLeg.set(Tuple.tuple(r, l)),
                        new SearchRequestContext(
                            new SearchRequestOperationsListener.CompositeListener(List.of(), LogManager.getLogger()),
                            searchRequest,
                            () -> null
                        )
                    );
                    if (localIndices != null) {
                        // The local leg runs on the coordinator's own task, whose timer is already armed.
                        assertEquals(interval, localLeg.get().v1().getCancelAfterTimeInterval());
                        localLeg.get().v2().onResponse(emptySearchResponse());
                    }
                    awaitLatch(latch, 5, TimeUnit.SECONDS);
                    assertNull(failure.get());
                    assertEquals(Collections.nCopies(remoteIndices.size(), interval), forwarded);
                }
            }
        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }

    /**
     * Runs a search whose request asks for {@code requested}, in the workload group {@link #GROUP_ID} with {@code groupSettings}, on
     * a cluster whose search.cancel_after_time_interval is {@code clusterInterval}, through doExecute and the WLM resolver, and
     * asserts that the coordinator arms its timer with {@code expectedTimer}, or arms none when it is {@code null}, and that the
     * request then carries {@code expectedOnRequest}.
     */
    private void assertCancelAfterTimeInterval(
        TimeValue requested,
        Settings groupSettings,
        String clusterInterval,
        TimeValue expectedTimer,
        TimeValue expectedOnRequest
    ) {
        SearchRequest request = new SearchRequest("index");
        request.setCancelAfterTimeInterval(requested);
        SearchTask task = request.createTask(1, "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        assertCancelAfterTimeInterval(task, request, GROUP_ID, groupSettings, clusterInterval, expectedTimer, expectedOnRequest);
    }

    /**
     * Runs a search that names no workload group, whose request asks for {@code requested}, on a cluster whose group
     * {@link #GROUP_ID} overrides request values with an interval of its own, and asserts that the coordinator arms its timer with
     * {@code expectedTimer}, or arms none when it is {@code null}, and that the request keeps its own interval.
     */
    private void assertUntaggedCancelAfterTimeInterval(TimeValue requested, String clusterInterval, TimeValue expectedTimer) {
        SearchRequest request = new SearchRequest("index");
        request.setCancelAfterTimeInterval(requested);
        SearchTask task = request.createTask(1, "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        Settings overridingGroup = Settings.builder().put(WLM_CANCEL_AFTER, "30s").put(WLM_OVERRIDE, true).build();
        assertCancelAfterTimeInterval(task, request, null, overridingGroup, clusterInterval, expectedTimer, requested);
        assertSame("the request keeps its own interval", requested, request.getCancelAfterTimeInterval());
    }

    /**
     * Like {@link #assertCancelAfterTimeInterval(TimeValue, Settings, String, TimeValue, TimeValue)}, for {@code task}, tagged to
     * {@code workloadGroupId}, or to no workload group when it is {@code null}.
     */
    private void assertCancelAfterTimeInterval(
        CancellableTask task,
        SearchRequest request,
        @Nullable String workloadGroupId,
        Settings groupSettings,
        String clusterInterval,
        TimeValue expectedTimer,
        TimeValue expectedOnRequest
    ) {
        List<Scheduled> scheduled = new CopyOnWriteArrayList<>();
        ThreadPool timerThreadPool = timerThreadPool(scheduled);
        TransportSearchAction action = newSearchAction(
            SearchExecutionGate.NOOP,
            new WorkloadGroupSearchCancelAfterTimeIntervalResolver(workloadGroupService(timerThreadPool, groupSettings)),
            new CancelTasksRecordingClient(timerThreadPool),
            timerThreadPool,
            Settings.builder().put(TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, clusterInterval).build()
        );
        String cell = "request ["
            + request.getCancelAfterTimeInterval()
            + "], group ["
            + workloadGroupId
            + "] "
            + groupSettings
            + ", cluster ["
            + clusterInterval
            + "]";
        ThreadContext threadContext = timerThreadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            if (workloadGroupId != null) {
                threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
            }
            action.doExecute(task, request, ActionListener.wrap(r -> {}, e -> {}));
        }
        if (expectedTimer == null) {
            assertEquals(cell, List.of(), scheduled);
        } else {
            assertEquals(cell, 1, scheduled.size());
            assertEquals(cell, expectedTimer, scheduled.get(0).delay());
        }
        assertEquals(cell, expectedOnRequest, request.getCancelAfterTimeInterval());
    }

    private static final String LOCAL_NODE_ID = "local_node";
    private static final String GROUP_ID = "group_id";
    private static final String WLM_CANCEL_AFTER = WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey();
    private static final String WLM_OVERRIDE = WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES.getKey();

    /** A resolver that records that it was consulted, and resolves every search to one second. */
    private static SearchCancelAfterTimeIntervalResolver oneSecondResolver(AtomicBoolean consulted) {
        return (task, request, requested) -> {
            consulted.set(true);
            return TimeValue.timeValueSeconds(1);
        };
    }

    /** A search request parsed by RestSearchAction from a REST request whose cancel_after_time_interval is {@code value}. */
    private SearchRequest searchRequestFromRest(String value) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("index", "index");
        params.put("cancel_after_time_interval", value);
        SearchRequest request = new SearchRequest();
        RestSearchAction.parseSearchRequest(
            request,
            new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build(),
            null,
            new NamedWriteableRegistry(Collections.emptyList()),
            size -> {}
        );
        return request;
    }

    /** What the cancellation timer scheduled. */
    private record Scheduled(Runnable command, TimeValue delay, String executor) {
    }

    /** A thread pool with one thread context, which records what is scheduled on it instead of running it. */
    private static ThreadPool timerThreadPool(List<Scheduled> scheduled) {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadPool.schedule(any(), any(), any())).thenAnswer(invocation -> {
            scheduled.add(new Scheduled(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2)));
            return mock(Scheduler.ScheduledCancellable.class);
        });
        return threadPool;
    }

    /** A node client that records the cancel-tasks requests of the cancellation timer instead of sending them. */
    private static final class CancelTasksRecordingClient extends NodeClient {
        private final List<CancelTasksRequest> cancelTasksRequests = new CopyOnWriteArrayList<>();

        CancelTasksRecordingClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool);
        }

        @Override
        public String getLocalNodeId() {
            return LOCAL_NODE_ID;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertSame(CancelTasksAction.INSTANCE, action);
            cancelTasksRequests.add((CancelTasksRequest) request);
        }
    }

    /** A workload group service whose cluster state holds the group {@link #GROUP_ID} with {@code searchSettings}. */
    private static WorkloadGroupService workloadGroupService(ThreadPool threadPool, Settings searchSettings) {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "name",
            GROUP_ID,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.MEMORY, 0.5),
                searchSettings
            ),
            System.currentTimeMillis()
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(workloadGroup)).build()
        );
        return new WorkloadGroupService(
            mock(WorkloadGroupTaskCancellationService.class),
            clusterService,
            threadPool,
            mock(WorkloadManagementSettings.class),
            null,
            new WorkloadGroupsStateAccessor(),
            new HashSet<>(),
            new HashSet<>()
        );
    }

    private TransportSearchAction newSearchAction(SearchExecutionGate searchExecutionGate) {
        return newSearchAction(
            searchExecutionGate,
            SearchCancelAfterTimeIntervalResolver.NOOP,
            mock(NodeClient.class),
            threadPool,
            Settings.EMPTY
        );
    }

    /** A search action whose cancellation timer schedules through {@code client}, on a cluster with {@code clusterSettings}. */
    private TransportSearchAction newSearchAction(
        SearchExecutionGate searchExecutionGate,
        SearchCancelAfterTimeIntervalResolver cancelAfterTimeIntervalResolver,
        NodeClient client,
        ThreadPool threadPool,
        Settings clusterSettings
    ) {
        return newSearchAction(searchExecutionGate, cancelAfterTimeIntervalResolver, client, threadPool, clusterSettings, false);
    }

    /**
     * Like {@link #newSearchAction(SearchExecutionGate, SearchCancelAfterTimeIntervalResolver, NodeClient, ThreadPool, Settings)},
     * but a {@link StreamTransportSearchAction} when {@code streaming}.
     */
    private TransportSearchAction newSearchAction(
        SearchExecutionGate searchExecutionGate,
        SearchCancelAfterTimeIntervalResolver cancelAfterTimeIntervalResolver,
        NodeClient client,
        ThreadPool threadPool,
        Settings clusterSettings,
        boolean streaming
    ) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.getSearchResponseStatusStats()).thenReturn(new SearchResponseStatusStats());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        if (streaming) {
            return new StreamTransportSearchAction(
                client,
                threadPool,
                mock(CircuitBreakerService.class),
                mock(StreamTransportService.class),
                mock(SearchService.class),
                mock(StreamSearchTransportService.class),
                new SearchPhaseController(namedWriteableRegistry, (searchSourceBuilder) -> null),
                clusterService,
                new ActionFilters(Collections.emptySet()),
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
                namedWriteableRegistry,
                mock(SearchPipelineService.class),
                mock(MetricsRegistry.class),
                new SearchRequestOperationsCompositeListenerFactory(),
                NoopTracer.INSTANCE,
                mock(TaskResourceTrackingService.class),
                indicesService,
                searchExecutionGate,
                cancelAfterTimeIntervalResolver
            );
        }
        return new TransportSearchAction(
            client,
            threadPool,
            mock(CircuitBreakerService.class),
            mock(TransportService.class),
            mock(SearchService.class),
            mock(SearchTransportService.class),
            new SearchPhaseController(namedWriteableRegistry, (searchSourceBuilder) -> null),
            clusterService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            namedWriteableRegistry,
            mock(SearchPipelineService.class),
            mock(MetricsRegistry.class),
            new SearchRequestOperationsCompositeListenerFactory(),
            NoopTracer.INSTANCE,
            mock(TaskResourceTrackingService.class),
            indicesService,
            searchExecutionGate,
            cancelAfterTimeIntervalResolver
        );
    }

    private static SearchTask newSearchTask() {
        return new SearchTask(1, "transport", SearchAction.NAME, () -> "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    public void testResolveIndices() {
        IndexMetadata.Builder indexA1 = IndexMetadata.builder("index_a1")
            .putAlias(AliasMetadata.builder("alias_a").build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        IndexMetadata.Builder indexA2 = IndexMetadata.builder("index_a2")
            .putAlias(AliasMetadata.builder("alias_a").build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexA1).put(indexA2))
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
            )
        );
        int numClusters = 2;
        DiscoveryNode[] nodes = new DiscoveryNode[numClusters];
        Map<String, OriginalIndices> remoteIndicesByCluster = new HashMap<>();
        Settings.Builder builder = Settings.builder();
        MockTransportService[] mockTransportServices = startTransport(numClusters, nodes, remoteIndicesByCluster, builder);
        Settings settings = builder.build();
        try (
            MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.start();
            service.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = service.getRemoteClusterService();

            SearchTransportService searchTransportService = mock(SearchTransportService.class);
            when(searchTransportService.getRemoteClusterService()).thenReturn(remoteClusterService);

            TransportSearchAction action = new TransportSearchAction(
                mock(NodeClient.class),
                threadPool,
                mock(CircuitBreakerService.class),
                mock(TransportService.class),
                mock(SearchService.class),
                searchTransportService,
                new SearchPhaseController(namedWriteableRegistry, (searchSourceBuilder) -> null),
                clusterService,
                mock(ActionFilters.class),
                indexNameExpressionResolver,
                namedWriteableRegistry,
                mock(SearchPipelineService.class),
                mock(MetricsRegistry.class),
                new SearchRequestOperationsCompositeListenerFactory(),
                mock(Tracer.class),
                mock(TaskResourceTrackingService.class),
                mock(IndicesService.class),
                SearchExecutionGate.NOOP,
                SearchCancelAfterTimeIntervalResolver.NOOP
            );

            // Actual test cases start here:

            {
                ResolvedIndices resolvedIndices = action.resolveIndices(new SearchRequest("index_*"));
                assertEquals(ResolvedIndices.of("index_a1", "index_a2"), resolvedIndices);
            }

            {
                ResolvedIndices resolvedIndices = action.resolveIndices(new SearchRequest("alias_a"));
                assertEquals(ResolvedIndices.of("alias_a"), resolvedIndices);
            }

            {
                // The generated pitId will contain the indices "idx" and "idy"
                String pitId = PitTestsUtil.getPitId();
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(pitId));
                ResolvedIndices resolvedIndices = action.resolveIndices(new SearchRequest().source(searchSourceBuilder));
                assertEquals(ResolvedIndices.of("idx", "idy"), resolvedIndices);
            }

            {
                ResolvedIndices resolvedIndices = action.resolveIndices(new SearchRequest("index_*", "remote1:remote_index"));
                assertEquals(
                    ResolvedIndices.of("index_a1", "index_a2")
                        .withRemoteIndices(
                            Map.of("remote1", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN))
                        ),
                    resolvedIndices
                );
            }

        } finally {
            for (MockTransportService mockTransportService : mockTransportServices) {
                mockTransportService.close();
            }
        }
    }
}

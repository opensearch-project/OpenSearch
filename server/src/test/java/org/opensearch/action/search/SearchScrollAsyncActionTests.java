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
import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.UUIDs;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.InternalScrollSearchRequest;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Transport;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class SearchScrollAsyncActionTests extends OpenSearchTestCase {

    public void testSendRequestsToNodes() throws InterruptedException {

        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId(UUIDs.randomBase64UUID(), 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId(UUIDs.randomBase64UUID(), 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId(UUIDs.randomBase64UUID(), 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId(UUIDs.randomBase64UUID(), 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId(UUIDs.randomBase64UUID(), 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<
            SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, dummyListener(), null, request, null) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null, OriginalIndices.NONE)
                    );
                    searchActionListener.onResponse(testSearchPhaseResult);
                }).start();
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                assertEquals(1, movedCounter.incrementAndGet());
                return new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        latch.countDown();
                    }
                };
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                results.setOnce(shardId, result);
            }
        };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(0, shardSearchFailures.length);
        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            assertNotNull(results.get(i));
            assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
            assertEquals(context[i].getNode(), results.get(i).node.getId());
        }
    }

    public void testFailNextPhase() throws InterruptedException {

        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("a", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("b", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("c", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("d", 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse o) {
                try {
                    fail("got a result");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    assertTrue(e instanceof SearchPhaseExecutionException);
                    SearchPhaseExecutionException ex = (SearchPhaseExecutionException) e;
                    assertEquals("BOOM", ex.getCause().getMessage());
                    assertEquals("TEST_PHASE", ex.getPhaseName());
                    assertEquals("Phase failed", ex.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        };
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<
            SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, listener, null, request, null) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null, OriginalIndices.NONE)
                    );
                    searchActionListener.onResponse(testSearchPhaseResult);
                }).start();
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                assertEquals(1, movedCounter.incrementAndGet());
                return new SearchPhase("TEST_PHASE") {
                    @Override
                    public void run() throws IOException {
                        throw new IllegalArgumentException("BOOM");
                    }
                };
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                results.setOnce(shardId, result);
            }
        };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(0, shardSearchFailures.length);
        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            assertNotNull(results.get(i));
            assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
            assertEquals(context[i].getNode(), results.get(i).node.getId());
        }
    }

    public void testNodeNotAvailable() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 0))
        );
        // node2 is not available
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<
            SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, dummyListener(), null, request, null) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                try {
                    assertNotEquals("node2 is not available", "node2", connection.getNode().getId());
                } catch (NullPointerException e) {
                    logger.warn(e);
                }
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null, OriginalIndices.NONE)
                    );
                    searchActionListener.onResponse(testSearchPhaseResult);
                }).start();
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                assertEquals(1, movedCounter.incrementAndGet());
                return new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        latch.countDown();
                    }
                };
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                results.setOnce(shardId, result);
            }
        };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(1, shardSearchFailures.length);
        assertEquals(
            "IllegalArgumentException[scroll_id references node [node2] which was not found in the cluster]",
            shardSearchFailures[0].reason()
        );

        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            if (context[i].getNode().equals("node2")) {
                assertNull(results.get(i));
            } else {
                assertNotNull(results.get(i));
                assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
                assertEquals(context[i].getNode(), results.get(i).node.getId());
            }
        }
    }

    public void testShardFailures() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<
            SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, dummyListener(), null, request, null) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    if (internalRequest.contextId().getId() == 17) {
                        searchActionListener.onFailure(new IllegalArgumentException("BOOM on shard"));
                    } else {
                        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult =
                            new SearchAsyncActionTests.TestSearchPhaseResult(internalRequest.contextId(), connection.getNode());
                        testSearchPhaseResult.setSearchShardTarget(
                            new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null, OriginalIndices.NONE)
                        );
                        searchActionListener.onResponse(testSearchPhaseResult);
                    }
                }).start();
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                assertEquals(1, movedCounter.incrementAndGet());
                return new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        latch.countDown();
                    }
                };
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                results.setOnce(shardId, result);
            }
        };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(1, shardSearchFailures.length);
        assertEquals("IllegalArgumentException[BOOM on shard]", shardSearchFailures[0].reason());

        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            if (context[i].getSearchContextId().getId() == 17) {
                assertNull(results.get(i));
            } else {
                assertNotNull(results.get(i));
                assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
                assertEquals(context[i].getNode(), results.get(i).node.getId());
            }
        }
    }

    public void testAllShardsFailed() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse o) {
                try {
                    fail("got a result");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    assertTrue(e instanceof SearchPhaseExecutionException);
                    SearchPhaseExecutionException ex = (SearchPhaseExecutionException) e;
                    assertEquals("BOOM on shard", ex.getCause().getMessage());
                    assertEquals("query", ex.getPhaseName());
                    assertEquals("all shards failed", ex.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        };
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<
            SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, listener, null, request, null) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> searchActionListener.onFailure(new IllegalArgumentException("BOOM on shard"))).start();
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                fail("don't move all shards failed");
                return null;
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                results.setOnce(shardId, result);
            }
        };

        action.run();
        latch.await();
        SearchContextIdForNode[] context = scrollId.getContext();

        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(context.length, shardSearchFailures.length);
        assertEquals("IllegalArgumentException[BOOM on shard]", shardSearchFailures[0].reason());

        for (int i = 0; i < results.length(); i++) {
            assertNull(results.get(i));
        }
    }

    private static ParsedScrollId getParsedScrollId(SearchContextIdForNode... idsForNodes) {
        List<SearchContextIdForNode> searchContextIdForNodes = Arrays.asList(idsForNodes);
        Collections.shuffle(searchContextIdForNodes, random());
        return new ParsedScrollId("", "test", searchContextIdForNodes.toArray(new SearchContextIdForNode[0]));
    }

    private ActionListener<SearchResponse> dummyListener() {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                fail("dummy");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
    }
}

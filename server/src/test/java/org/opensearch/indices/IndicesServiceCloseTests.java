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

package org.opensearch.indices;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.node.MockNode;
import org.opensearch.node.Node;
import org.opensearch.node.NodeValidationException;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;
import org.opensearch.transport.nio.MockNioTransportPlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class IndicesServiceCloseTests extends OpenSearchTestCase {

    private static class DummyQuery extends Query {

        private final int id;

        DummyQuery(int id) {
            this.id = id;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && id == ((IndicesServiceCloseTests.DummyQuery) obj).id;
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + id;
        }

        @Override
        public String toString(String field) {
            return "dummy";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long l) throws IOException {
                            return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                        }

                        @Override
                        public long cost() {
                            return 0;
                        }
                    };
                }

                @Override
                public int count(LeafReaderContext context) {
                    return -1;
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

    }

    private Node startNode() throws NodeValidationException {
        final Path tempDir = createTempDir();
        String nodeName = "node_s_0";
        Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", random().nextLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent())
            .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
            .put("transport.type", getTestTransportType())
            .put(dataNode())
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // turning on the real memory circuit breaker leads to spurious test failures. As have no full control over heap usage, we
            // turn it off for these tests.
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey(), nodeName)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();

        Node node = new MockNode(
            settings,
            Arrays.asList(MockNioTransportPlugin.class, MockHttpTransport.TestPlugin.class, InternalSettingsPlugin.class),
            true
        );
        node.start();
        return node;
    }

    public void testCloseEmptyIndicesService() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertTrue(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
    }

    public void testCloseNonEmptyIndicesService() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );

        assertEquals(2, indicesService.indicesRefCount.refCount());
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));

        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertTrue(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
    }

    public void testCloseWithIncedRefStore() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        shard.store().incRef();
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));

        shard.store().decRef();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertTrue(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
    }

    public void testCloseWhileOngoingRequest() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );
        node.client().prepareIndex("test").setId("1").setSource(Collections.emptyMap()).get();
        OpenSearchAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        searcher.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
    }

    public void testCloseAfterRequestHasUsedQueryCache() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                )
        );
        node.client().prepareIndex("test").setId("1").setSource(Collections.singletonMap("foo", 3L)).get();
        OpenSearchAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndicesQueryCache cache = indicesService.getIndicesQueryCache();

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        Query query = new DummyQuery(1);
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
        searcher.count(query);
        assertEquals(1L, cache.getStats(shard.shardId()).getCacheSize());

        searcher.close();
        assertEquals(2, indicesService.indicesRefCount.refCount());
        assertEquals(1L, cache.getStats(shard.shardId()).getCacheSize());

        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
    }

    public void testCloseWhileOngoingRequestUsesQueryCache() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                )
        );
        node.client().prepareIndex("test").setId("1").setSource(Collections.singletonMap("foo", 3L)).get();
        OpenSearchAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndicesQueryCache cache = indicesService.getIndicesQueryCache();

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        Query query = new DummyQuery(1);
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
        searcher.count(query);
        assertEquals(1L, cache.getStats(shard.shardId()).getCacheSize());

        searcher.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
    }

    public void testCloseWhileOngoingRequestUsesRequestCache() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(
            node.client()
                .admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                )
        );
        node.client().prepareIndex("test").setId("1").setSource(Collections.singletonMap("foo", 3L)).get();
        OpenSearchAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndicesRequestCache cache = indicesService.indicesRequestCache;

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertEquals(0L, cache.count());
        IndicesService.IndexShardCacheEntity cacheEntity = new IndicesService.IndexShardCacheEntity(shard) {
            @Override
            public long ramBytesUsed() {
                return 42;
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public Object getCacheIdentity() {
                return shard;
            }
        };
        cache.getOrCompute(cacheEntity, () -> new BytesArray("bar"), searcher.getDirectoryReader(), new BytesArray("foo"));
        assertEquals(1L, cache.count());

        searcher.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertEquals(0L, cache.count());
    }
}

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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader.DelegatingCacheHelper;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.ShardLockObtainFailedException;
import org.opensearch.gateway.GatewayMetaState;
import org.opensearch.gateway.LocalAllocateDangledIndices;
import org.opensearch.gateway.MetaStateService;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.similarity.NonNegativeScoresSimilarity;
import org.opensearch.indices.IndicesService.ShardDeletionCheckResult;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.TestSearchContext;
import org.opensearch.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesServiceTests extends OpenSearchSingleNodeTestCase {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }

    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(TestPlugin.class, FooEnginePlugin.class, BarEnginePlugin.class))
            .collect(Collectors.toList());
    }

    public static class FooEnginePlugin extends Plugin implements EnginePlugin {

        static class FooEngineFactory implements EngineFactory {

            @Override
            public Engine newReadWriteEngine(final EngineConfig config) {
                return new InternalEngine(config);
            }

        }

        private static final Setting<Boolean> FOO_INDEX_SETTING = Setting.boolSetting(
            "index.foo_index",
            false,
            Setting.Property.IndexScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(FOO_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            if (FOO_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new FooEngineFactory());
            } else {
                return Optional.empty();
            }
        }

    }

    public static class BarEnginePlugin extends Plugin implements EnginePlugin {

        static class BarEngineFactory implements EngineFactory {

            @Override
            public Engine newReadWriteEngine(final EngineConfig config) {
                return new InternalEngine(config);
            }

        }

        private static final Setting<Boolean> BAR_INDEX_SETTING = Setting.boolSetting(
            "index.bar_index",
            false,
            Setting.Property.IndexScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(BAR_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            if (BAR_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new BarEngineFactory());
            } else {
                return Optional.empty();
            }
        }

    }

    public static class TestPlugin extends Plugin implements MapperPlugin {

        public TestPlugin() {}

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", KeywordFieldMapper.PARSER);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSimilarity("fake-similarity", (settings, indexCreatedVersion, scriptService) -> new BM25Similarity());
        }
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCanDeleteShardContent() {
        IndicesService indicesService = getIndicesService();
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        ShardId shardId = new ShardId(meta.getIndex(), 0);
        assertEquals(
            "no shard location",
            indicesService.canDeleteShardContent(shardId, indexSettings),
            ShardDeletionCheckResult.NO_FOLDER_FOUND
        );
        IndexService test = createIndex("test");
        shardId = new ShardId(test.index(), 0);
        assertTrue(test.hasShard(0));
        assertEquals(
            "shard is allocated",
            indicesService.canDeleteShardContent(shardId, test.getIndexSettings()),
            ShardDeletionCheckResult.STILL_ALLOCATED
        );
        test.removeShard(0, "boom");
        assertEquals(
            "shard is removed",
            indicesService.canDeleteShardContent(shardId, test.getIndexSettings()),
            ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE
        );
        ShardId notAllocated = new ShardId(test.index(), 100);
        assertEquals(
            "shard that was never on this node should NOT be deletable",
            indicesService.canDeleteShardContent(notAllocated, test.getIndexSettings()),
            ShardDeletionCheckResult.NO_FOLDER_FOUND
        );
    }

    public void testDeleteIndexStore() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexMetadata firstMetadata = clusterService.state().metadata().index("test");
        assertTrue(test.hasShard(0));
        ShardPath firstPath = ShardPath.loadShardPath(
            logger,
            getNodeEnvironment(),
            new ShardId(test.index(), 0),
            test.getIndexSettings().customDataPath()
        );

        expectThrows(IllegalStateException.class, () -> indicesService.deleteIndexStore("boom", firstMetadata));
        assertTrue(firstPath.exists());

        GatewayMetaState gwMetaState = getInstanceFromNode(GatewayMetaState.class);
        Metadata meta = gwMetaState.getMetadata();
        assertNotNull(meta);
        assertNotNull(meta.index("test"));
        assertAcked(client().admin().indices().prepareDelete("test"));

        assertFalse(firstPath.exists());

        meta = gwMetaState.getMetadata();
        assertNotNull(meta);
        assertNull(meta.index("test"));

        test = createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareFlush("test").get();
        assertHitCount(client().prepareSearch("test").get(), 1);
        IndexMetadata secondMetadata = clusterService.state().metadata().index("test");
        assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(1));
        ShardPath secondPath = ShardPath.loadShardPath(
            logger,
            getNodeEnvironment(),
            new ShardId(test.index(), 0),
            test.getIndexSettings().customDataPath()
        );
        assertTrue(secondPath.exists());

        expectThrows(IllegalStateException.class, () -> indicesService.deleteIndexStore("boom", secondMetadata));
        assertTrue(secondPath.exists());

        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");
    }

    public void testPendingTasks() throws Exception {
        final IndexService indexService = createIndex("test");
        final Index index = indexService.index();
        final IndexSettings indexSettings = indexService.getIndexSettings();

        final IndexShard indexShard = indexService.getShardOrNull(0);
        assertNotNull(indexShard);
        assertTrue(indexShard.routingEntry().started());

        final ShardPath shardPath = indexShard.shardPath();
        assertEquals(
            ShardPath.loadShardPath(logger, getNodeEnvironment(), indexShard.shardId(), indexSettings.customDataPath()),
            shardPath
        );

        final IndicesService indicesService = getIndicesService();
        expectThrows(
            ShardLockObtainFailedException.class,
            () -> indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0))
        );
        assertTrue(shardPath.exists());

        int numPending = 1;
        if (randomBoolean()) {
            indicesService.addPendingDelete(indexShard.shardId(), indexSettings);
        } else {
            if (randomBoolean()) {
                numPending++;
                indicesService.addPendingDelete(indexShard.shardId(), indexSettings);
            }
            indicesService.addPendingDelete(index, indexSettings);
        }

        assertAcked(client().admin().indices().prepareClose("test"));
        assertTrue(shardPath.exists());
        ensureGreen("test");

        assertEquals(indicesService.numPendingDeletes(index), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        expectThrows(
            ShardLockObtainFailedException.class,
            () -> indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0))
        );

        assertEquals(indicesService.numPendingDeletes(index), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        final boolean hasBogus = randomBoolean();
        if (hasBogus) {
            indicesService.addPendingDelete(new ShardId(index, 0), indexSettings);
            indicesService.addPendingDelete(new ShardId(index, 1), indexSettings);
            indicesService.addPendingDelete(new ShardId("bogus", "_na_", 1), indexSettings);
            assertEquals(indicesService.numPendingDeletes(index), numPending + 2);
            assertTrue(indicesService.hasUncompletedPendingDeletes());
        }

        assertAcked(client().admin().indices().prepareDelete("test"));
        assertBusy(() -> {
            try {
                indicesService.processPendingDeletes(index, indexSettings, TimeValue.timeValueMillis(0));
                assertEquals(indicesService.numPendingDeletes(index), 0);
            } catch (final Exception e) {
                fail(e.getMessage());
            }
        });
        assertBusy(() -> {
            assertThat(indicesService.hasUncompletedPendingDeletes(), equalTo(hasBogus)); // "bogus" index has not been removed
            assertFalse(shardPath.exists());
        });
    }

    public void testVerifyIfIndexContentDeleted() throws Exception {
        final Index index = new Index("test", UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final NodeEnvironment nodeEnv = getNodeEnvironment();
        final MetaStateService metaStateService = getInstanceFromNode(MetaStateService.class);

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        metaStateService.writeIndex("test index being created", indexMetadata);
        final Metadata metadata = Metadata.builder(clusterService.state().metadata()).put(indexMetadata, true).build();
        final ClusterState csWithIndex = new ClusterState.Builder(clusterService.state()).metadata(metadata).build();
        try {
            indicesService.verifyIndexIsDeleted(index, csWithIndex);
            fail("Should not be able to delete index contents when the index is part of the cluster state.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Cannot delete index"));
        }

        final ClusterState withoutIndex = new ClusterState.Builder(csWithIndex).metadata(
            Metadata.builder(csWithIndex.metadata()).remove(index.getName())
        ).build();
        indicesService.verifyIndexIsDeleted(index, withoutIndex);
        assertFalse("index files should be deleted", FileSystemUtils.exists(nodeEnv.indexPaths(index)));
    }

    public void testDanglingIndicesWithAliasConflict() throws Exception {
        final String indexName = "test-idx1";
        final String alias = "test-alias";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        createIndex(indexName);

        // create the alias for the index
        client().admin().indices().prepareAliases().addAlias(indexName, alias).get();
        final ClusterState originalState = clusterService.state();

        // try to import a dangling index with the same name as the alias, it should fail
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(alias).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadata), ActionListener.wrap(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), equalTo(originalState));

        // remove the alias
        client().admin().indices().prepareAliases().removeAlias(indexName, alias).get();

        // now try importing a dangling index with the same name as the alias, it should succeed.
        latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadata), ActionListener.wrap(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), not(originalState));
        assertNotNull(clusterService.state().getMetadata().index(alias));
    }

    public void testDanglingIndicesWithLaterVersion() throws Exception {
        final String indexNameLater = "test-idxnewer";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState originalState = clusterService.state();

        // import an index with minor version incremented by one over cluster cluster-manager version, it should be ignored
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettingsLater = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(Version.CURRENT.id + 10000))
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        final IndexMetadata indexMetadataLater = new IndexMetadata.Builder(indexNameLater).settings(idxSettingsLater)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        CountDownLatch latch = new CountDownLatch(1);
        dangling.allocateDangled(Arrays.asList(indexMetadataLater), ActionListener.wrap(latch::countDown));
        latch.await();
        assertThat(clusterService.state(), equalTo(originalState));
    }

    /**
     * This test checks an edge case where, if a node had an index (lets call it A with UUID 1), then
     * deleted it (so a tombstone entry for A will exist in the cluster state), then created
     * a new index A with UUID 2, then shutdown, when the node comes back online, it will look at the
     * tombstones for deletions, and it should proceed with trying to delete A with UUID 1 and not
     * throw any errors that the index still exists in the cluster state.  This is a case of ensuring
     * that tombstones that have the same name as current valid indices don't cause confusion by
     * trying to delete an index that exists.
     * See https://github.com/elastic/elasticsearch/issues/18054
     */
    public void testIndexAndTombstoneWithSameNameOnStartup() throws Exception {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final Index tombstonedIndex = new Index(indexName, UUIDs.randomBase64UUID());
        final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(tombstonedIndex).build();
        final Metadata metadata = Metadata.builder().put(indexMetadata, true).indexGraveyard(graveyard).build();
        final ClusterState clusterState = new ClusterState.Builder(new ClusterName("testCluster")).metadata(metadata).build();
        // if all goes well, this won't throw an exception, otherwise, it will throw an IllegalStateException
        indicesService.verifyIndexIsDeleted(tombstonedIndex, clusterState);
    }

    /**
     * Tests that teh {@link MapperService} created by {@link IndicesService#createIndexMapperService(IndexMetadata)} contains
     * custom types and similarities registered by plugins
     */
    public void testStandAloneMapperServiceWithPlugins() throws IOException {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .put(IndexModule.SIMILARITY_SETTINGS_PREFIX + ".test.type", "fake-similarity")
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
        assertNotNull(mapperService.documentMapperParser().parserContext().typeParser("fake-mapper"));
        Similarity sim = mapperService.documentMapperParser().parserContext().getSimilarity("test").get();
        assertThat(sim, instanceOf(NonNegativeScoresSimilarity.class));
        sim = ((NonNegativeScoresSimilarity) sim).getDelegate();
        assertThat(sim, instanceOf(BM25Similarity.class));
    }

    public void testStatsByShardDoesNotDieFromExpectedExceptions() {
        final int shardCount = randomIntBetween(2, 5);
        final int failedShardId = randomIntBetween(0, shardCount - 1);

        final Index index = new Index("test-index", "abc123");
        // the shard that is going to fail
        final ShardId shardId = new ShardId(index, failedShardId);

        final List<IndexShard> shards = new ArrayList<>(shardCount);
        final List<IndexShardStats> shardStats = new ArrayList<>(shardCount - 1);

        final IndexShardState state = randomFrom(IndexShardState.values());
        final String message = "TEST - expected";

        final RuntimeException expectedException = randomFrom(
            new IllegalIndexShardStateException(shardId, state, message),
            new AlreadyClosedException(message)
        );

        // this allows us to control the indices that exist
        final IndicesService mockIndicesService = mock(IndicesService.class);
        final IndexService indexService = mock(IndexService.class);

        // generate fake shards and their responses
        for (int i = 0; i < shardCount; ++i) {
            final IndexShard shard = mock(IndexShard.class);

            shards.add(shard);

            if (failedShardId != i) {
                final IndexShardStats successfulShardStats = mock(IndexShardStats.class);

                shardStats.add(successfulShardStats);

                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenReturn(successfulShardStats);
            } else {
                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenThrow(expectedException);
            }
        }

        when(mockIndicesService.iterator()).thenReturn(Collections.singleton(indexService).iterator());
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indexService.index()).thenReturn(index);

        // real one, which has a logger defined
        final IndicesService indicesService = getIndicesService();

        final Map<Index, List<IndexShardStats>> indexStats = indicesService.statsByShard(mockIndicesService, CommonStatsFlags.ALL);

        assertThat(indexStats.isEmpty(), equalTo(false));
        assertThat("index not defined", indexStats.containsKey(index), equalTo(true));
        assertThat("unexpected shard stats", indexStats.get(index), equalTo(shardStats));
    }

    public void testIsMetadataField() {
        IndicesService indicesService = getIndicesService();
        assertFalse(indicesService.isMetadataField(randomAlphaOfLengthBetween(10, 15)));
        for (String builtIn : IndicesModule.getBuiltInMetadataFields()) {
            assertTrue(indicesService.isMetadataField(builtIn));
        }
    }

    public void testGetEngineFactory() throws IOException {
        final IndicesService indicesService = getIndicesService();

        final Boolean[] values = new Boolean[] { true, false, null };
        for (final Boolean value : values) {
            final String indexName = "foo-" + value;
            final Index index = new Index(indexName, UUIDs.randomBase64UUID());
            final Settings.Builder builder = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
            if (value != null) {
                builder.put(FooEnginePlugin.FOO_INDEX_SETTING.getKey(), value);
            }

            final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(builder.build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            final IndexService indexService = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
            if (value != null && value) {
                assertThat(indexService.getEngineFactory(), instanceOf(FooEnginePlugin.FooEngineFactory.class));
            } else {
                assertThat(indexService.getEngineFactory(), instanceOf(InternalEngineFactory.class));
            }
        }
    }

    public void testConflictingEngineFactories() {
        final String indexName = "foobar";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .put(FooEnginePlugin.FOO_INDEX_SETTING.getKey(), true)
            .put(BarEnginePlugin.BAR_INDEX_SETTING.getKey(), true)
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        final IndicesService indicesService = getIndicesService();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> indicesService.createIndex(indexMetadata, Collections.emptyList(), false)
        );
        final String pattern =
            ".*multiple engine factories provided for \\[foobar/.*\\]: \\[.*FooEngineFactory\\],\\[.*BarEngineFactory\\].*";
        assertThat(e, hasToString(new RegexMatcher(pattern)));
    }

    public void testClusterRemoteTranslogBufferIntervalDefault() {
        IndicesService indicesService = getIndicesService();
        assertEquals(
            IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
            indicesService.getRemoteStoreSettings().getClusterRemoteTranslogBufferInterval()
        );
    }

    public void testDirectoryReaderWithoutDelegatingCacheHelperNotCacheable() throws IOException {
        IndicesService indicesService = getIndicesService();
        final IndexService indexService = createIndex("test");
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.requestCache()).thenReturn(true);

        TestSearchContext context = getTestContext(indexService, 0);
        IndexReader.CacheHelper notDelegatingCacheHelper = mock(IndexReader.CacheHelper.class);
        DelegatingCacheHelper delegatingCacheHelper = mock(DelegatingCacheHelper.class);
        for (boolean useDelegatingCacheHelper : new boolean[] { true, false }) {
            IndexReader.CacheHelper cacheHelper = useDelegatingCacheHelper ? delegatingCacheHelper : notDelegatingCacheHelper;
            setupMocksForCanCache(context, cacheHelper);
            assertEquals(useDelegatingCacheHelper, indicesService.canCache(request, context));
        }
    }

    public void testCanCacheSizeNonzero() {
        // Requests should only be cached if their size is <= INDICES_REQUEST_CACHE_MAX_SIZE_TO_CACHE_SETTING.
        final IndexService indexService = createIndex("test");
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.requestCache()).thenReturn(null);

        TestSearchContext sizeZeroContext = getTestContext(indexService, 0);
        TestSearchContext sizeNonzeroContext = getTestContext(indexService, 10);

        // Test for an IndicesService with the default setting value of 0
        IndicesService indicesService = getIndicesService();
        DelegatingCacheHelper cacheHelper = mock(DelegatingCacheHelper.class);
        Map<TestSearchContext, Boolean> expectedResultMap = Map.of(sizeZeroContext, true, sizeNonzeroContext, false);

        for (Map.Entry<TestSearchContext, Boolean> entry : expectedResultMap.entrySet()) {
            TestSearchContext context = entry.getKey();
            setupMocksForCanCache(context, cacheHelper);
            assertEquals(entry.getValue(), indicesService.canCache(request, context));
        }
        // Simulate the cluster setting update by manually calling setCanCacheSizeNonzeroRequests
        int maxCacheableSize = 40;
        indicesService.setMaxSizeInRequestCache(maxCacheableSize);
        TestSearchContext sizeEqualsThresholdContext = getTestContext(indexService, maxCacheableSize);
        TestSearchContext sizeAboveThresholdContext = getTestContext(indexService, maxCacheableSize + 5);
        expectedResultMap = Map.of(sizeZeroContext, true, sizeEqualsThresholdContext, true, sizeAboveThresholdContext, false);

        for (Map.Entry<TestSearchContext, Boolean> entry : expectedResultMap.entrySet()) {
            TestSearchContext context = entry.getKey();
            setupMocksForCanCache(context, cacheHelper);
            assertEquals(entry.getValue(), indicesService.canCache(request, context));
        }
    }

    private void setupMocksForCanCache(TestSearchContext context, IndexReader.CacheHelper cacheHelper) {
        ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        context.setSearcher(searcher);
        DirectoryReader reader = mock(DirectoryReader.class);
        when(searcher.getDirectoryReader()).thenReturn(reader);
        when(searcher.getIndexReader()).thenReturn(reader);
        when(reader.getReaderCacheHelper()).thenReturn(cacheHelper);
    }

    private TestSearchContext getTestContext(IndexService indexService, int size) {
        return new TestSearchContext(indexService.getBigArrays(), indexService) {
            @Override
            public SearchType searchType() {
                return SearchType.QUERY_THEN_FETCH;
            }

            @Override
            public int size() {
                return size;
            }
        };
    }
}

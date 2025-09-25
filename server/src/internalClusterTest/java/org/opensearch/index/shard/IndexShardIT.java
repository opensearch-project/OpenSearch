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

package org.opensearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.LockObtainFailedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.InternalClusterInfoService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.engine.NoOpEngine;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.TestTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.checkpoint.MergedSegmentPublisher;
import org.opensearch.indices.replication.checkpoint.ReferencedSegmentsPublisher;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.opensearch.index.shard.IndexShardTestCase.getTranslog;
import static org.opensearch.index.shard.IndexShardTestCase.recoverFromStore;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static org.mockito.Mockito.mock;

public class IndexShardIT extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testLockTryingToDelete() throws Exception {
        createIndex("test");
        ensureGreen();
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);

        ClusterService cs = getInstanceFromNode(ClusterService.class);
        final Index index = cs.state().metadata().index("test").getIndex();
        Path[] shardPaths = env.availableShardPaths(new ShardId(index, 0));
        logger.info("--> paths: [{}]", (Object) shardPaths);
        // Should not be able to acquire the lock because it's already open
        try {
            NodeEnvironment.acquireFSLockForPaths(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), shardPaths);
            fail("should not have been able to acquire the lock");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
        // Test without the regular shard lock to assume we can acquire it
        // (worst case, meaning that the shard lock could be acquired and
        // we're green to delete the shard's directory)
        ShardLock sLock = new DummyShardLock(new ShardId(index, 0));
        try {
            env.deleteShardDirectoryUnderLock(sLock, IndexSettingsModule.newIndexSettings("test", Settings.EMPTY));
            fail("should not have been able to delete the directory");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
    }

    public void testDurableFlagHasEffect() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test").setId("1").setSource("{}", MediaTypeRegistry.JSON).get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        Translog translog = getTranslog(shard);
        Predicate<Translog> needsSync = (tlog) -> {
            // we can't use tlog.needsSync() here since it also takes the global checkpoint into account
            // we explicitly want to check here if our durability checks are taken into account so we only
            // check if we are synced upto the current write location
            Translog.Location lastWriteLocation = tlog.getLastWriteLocation();
            try {
                // the lastWriteLocaltion has a Integer.MAX_VALUE size so we have to create a new one
                return tlog.ensureSynced(new Translog.Location(lastWriteLocation.generation, lastWriteLocation.translogLocation, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        setDurability(shard, Translog.Durability.REQUEST);
        assertFalse(needsSync.test(translog));
        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareIndex("test").setId("2").setSource("{}", MediaTypeRegistry.JSON).get();
        assertTrue(needsSync.test(translog));
        setDurability(shard, Translog.Durability.REQUEST);
        client().prepareDelete("test", "1").get();
        assertFalse(needsSync.test(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareDelete("test", "2").get();
        assertTrue(translog.syncNeeded());
        setDurability(shard, Translog.Durability.REQUEST);
        assertNoFailures(
            client().prepareBulk()
                .add(client().prepareIndex("test").setId("3").setSource("{}", MediaTypeRegistry.JSON))
                .add(client().prepareDelete("test", "1"))
                .get()
        );
        assertFalse(needsSync.test(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        assertNoFailures(
            client().prepareBulk()
                .add(client().prepareIndex("test").setId("4").setSource("{}", MediaTypeRegistry.JSON))
                .add(client().prepareDelete("test", "3"))
                .get()
        );
        setDurability(shard, Translog.Durability.REQUEST);
        assertTrue(needsSync.test(translog));
    }

    private void setDurability(IndexShard shard, Translog.Durability durability) {
        client().admin()
            .indices()
            .prepareUpdateSettings(shard.shardId().getIndexName())
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability.name()).build())
            .get();
        assertEquals(durability, shard.getTranslogDurability());
    }

    public void testUpdatePriority() {
        assertAcked(
            client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 200))
        );
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        assertEquals(200, indexService.getIndexSettings().getSettings().getAsInt(IndexMetadata.SETTING_PRIORITY, 0).intValue());
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 400).build())
            .get();
        assertEquals(400, indexService.getIndexSettings().getSettings().getAsInt(IndexMetadata.SETTING_PRIORITY, 0).intValue());
    }

    public void testIndexDirIsDeletedWhenShardRemoved() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataDir().resolve(randomAlphaOfLength(10));
        logger.info("--> idxPath: [{}]", idxPath);
        Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_DATA_PATH, idxPath).build();
        createIndex("test", idxSettings);
        ensureGreen("test");
        client().prepareIndex("test").setId("1").setSource("{}", MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE).get();
        SearchResponse response = client().prepareSearch("test").get();
        assertHitCount(response, 1L);
        client().admin().indices().prepareDelete("test").get();
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(idxPath);
    }

    public void testExpectedShardSizeIsPresent() throws InterruptedException {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
        );
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test").setSource("{}", MediaTypeRegistry.JSON).get();
        }
        ensureGreen("test");
        InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) getInstanceFromNode(ClusterInfoService.class);
        clusterInfoService.refresh();
        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        Long test = clusterInfoService.getClusterInfo()
            .getShardSize(state.getRoutingTable().index("test").getShards().get(0).primaryShard());
        assertNotNull(test);
        assertTrue(test > 0);
    }

    public void testIndexCanChangeCustomDataPath() throws Exception {
        final String index = "test-custom-data-path";
        final Path sharedDataPath = getInstanceFromNode(Environment.class).sharedDataDir().resolve(randomAsciiLettersOfLength(10));
        final Path indexDataPath = sharedDataPath.resolve("start-" + randomAsciiLettersOfLength(10));

        logger.info("--> creating index [{}] with data_path [{}]", index, indexDataPath);
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_DATA_PATH, indexDataPath.toAbsolutePath().toString())
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .put(IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED.getKey(), false)
                .build()
        );
        client().prepareIndex(index).setId("1").setSource("foo", "bar").setRefreshPolicy(IMMEDIATE).get();
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0).get(), 1L);

        logger.info("--> closing the index [{}]", index);
        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(ActiveShardCount.DEFAULT));
        logger.info("--> index closed, re-opening...");
        assertAcked(client().admin().indices().prepareOpen(index));
        logger.info("--> index re-opened");
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0).get(), 1L);

        // Now, try closing and changing the settings
        logger.info("--> closing the index [{}] before updating data_path", index);
        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(ActiveShardCount.DEFAULT));

        // race condition: async flush may cause translog file deletion resulting in an inconsistent stream from
        // Files.walk below during copy phase
        // temporarily disable refresh to avoid any flushes or syncs that may inadvertently cause the deletion
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1").build())
        );

        final Path newIndexDataPath = sharedDataPath.resolve("end-" + randomAlphaOfLength(10));
        IOUtils.rm(newIndexDataPath);

        logger.info("--> copying data on disk from [{}] to [{}]", indexDataPath, newIndexDataPath);
        assert Files.exists(newIndexDataPath) == false : "new index data path directory should not exist!";
        try (Stream<Path> stream = Files.walk(indexDataPath)) {
            stream.forEach(path -> {
                try {
                    if (path.endsWith(".lock") == false) {
                        Files.copy(path, newIndexDataPath.resolve(indexDataPath.relativize(path)));
                    }
                } catch (final Exception e) {
                    logger.error("Failed to copy data path directory", e);
                    fail();
                }
            });
        }

        logger.info("--> updating data_path to [{}] for index [{}]", newIndexDataPath, index);
        // update data path and re-enable refresh
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_DATA_PATH, newIndexDataPath.toAbsolutePath().toString())
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), IndexSettings.DEFAULT_REFRESH_INTERVAL.toString())
                        .build()
                )
                .setIndicesOptions(IndicesOptions.fromOptions(true, false, true, true))
        );

        logger.info("--> settings updated and files moved, re-opening index");
        assertAcked(client().admin().indices().prepareOpen(index));
        logger.info("--> index re-opened");
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0).get(), 1L);

        assertAcked(client().admin().indices().prepareDelete(index));
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(newIndexDataPath.toAbsolutePath());
    }

    public void testMaybeFlush() throws Exception {
        createIndex(
            "test",
            Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST).build()
        );
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldPeriodicallyFlush());
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put(
                        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                        new ByteSizeValue(135 /* size of the operation + one generation header&footer*/, ByteSizeUnit.BYTES)
                    )
                    .build()
            )
            .get();
        client().prepareIndex("test")
            .setId("0")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
            .get();
        assertFalse(shard.shouldPeriodicallyFlush());
        shard.applyIndexOperationOnPrimary(
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            new SourceToParse("test", "1", new BytesArray("{}"), MediaTypeRegistry.JSON),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        assertTrue(shard.shouldPeriodicallyFlush());
        final Translog translog = getTranslog(shard);
        assertEquals(2, translog.stats().getUncommittedOperations());
        assertThat(shard.flushStats().getTotal(), equalTo(0L));
        client().prepareIndex("test")
            .setId("2")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
            .get();
        assertThat(shard.getLastKnownGlobalCheckpoint(), equalTo(2L));
        assertBusy(() -> { // this is async
            assertFalse(shard.shouldPeriodicallyFlush());
            assertThat(shard.flushStats().getPeriodic(), equalTo(1L));
            assertThat(shard.flushStats().getTotal(), equalTo(1L));
        });
        shard.sync();
        assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(2L));
        assertThat("last commit [" + shard.commitStats().getUserData() + "]", translog.stats().getUncommittedOperations(), equalTo(0));
        long size = Math.max(translog.stats().getUncommittedSizeInBytes(), Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1);
        logger.info(
            "--> current translog size: [{}] num_ops [{}] generation [{}]",
            translog.stats().getUncommittedSizeInBytes(),
            translog.stats().getUncommittedOperations(),
            translog.getGeneration()
        );
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(size, ByteSizeUnit.BYTES))
                    .build()
            )
            .get();
        client().prepareDelete("test", "2").get();
        logger.info(
            "--> translog size after delete: [{}] num_ops [{}] generation [{}]",
            translog.stats().getUncommittedSizeInBytes(),
            translog.stats().getUncommittedOperations(),
            translog.getGeneration()
        );
        assertBusy(() -> { // this is async
            final TranslogStats translogStats = translog.stats();
            final CommitStats commitStats = shard.commitStats();
            final FlushStats flushStats = shard.flushStats();
            logger.info(
                "--> translog stats [{}] gen [{}] commit_stats [{}] flush_stats [{}/{}]",
                Strings.toString(MediaTypeRegistry.JSON, translogStats),
                translog.getGeneration().translogFileGeneration,
                commitStats.getUserData(),
                flushStats.getPeriodic(),
                flushStats.getTotal()
            );
            assertFalse(shard.shouldPeriodicallyFlush());
        });
        shard.sync();
        assertEquals(0, translog.stats().getUncommittedOperations());
    }

    public void testMaybeRollTranslogGeneration() throws Exception {
        final int generationThreshold = randomIntBetween(64, 512);
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.translog.generation_threshold_size", generationThreshold + "b")
            .build();
        createIndexWithSimpleMappings("test", settings);
        ensureGreen("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        int rolls = 0;
        final Translog translog = getTranslog(shard);
        final long generation = translog.currentFileGeneration();
        final int numberOfDocuments = randomIntBetween(32, 128);
        for (int i = 0; i < numberOfDocuments; i++) {
            assertThat(translog.currentFileGeneration(), equalTo(generation + rolls));
            final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                new SourceToParse("test", "1", new BytesArray("{}"), MediaTypeRegistry.JSON),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            final Translog.Location location = result.getTranslogLocation();
            shard.afterWriteOperation();
            if (location.translogLocation + location.size > generationThreshold) {
                // wait until the roll completes
                assertBusy(() -> assertFalse(shard.shouldRollTranslogGeneration()));
                rolls++;
                assertThat(translog.currentFileGeneration(), equalTo(generation + rolls));
            }
        }
    }

    public void testStressMaybeFlushOrRollTranslogGeneration() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldPeriodicallyFlush());
        final boolean flush = randomBoolean();
        final Settings settings;
        if (flush) {
            // size of the operation plus the overhead of one generation.
            settings = Settings.builder().put("index.translog.flush_threshold_size", "125b").build();
        } else {
            // size of the operation plus header and footer
            settings = Settings.builder().put("index.translog.generation_threshold_size", "117b").build();
        }
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
        client().prepareIndex("test")
            .setId("0")
            .setSource("{}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
            .get();
        assertFalse(shard.shouldPeriodicallyFlush());
        final AtomicBoolean running = new AtomicBoolean(true);
        final int numThreads = randomIntBetween(2, 4);
        final Thread[] threads = new Thread[numThreads];
        final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                while (running.get()) {
                    shard.afterWriteOperation();
                }
            });
            threads[i].start();
        }
        barrier.await();
        final CheckedRunnable<Exception> check;
        if (flush) {
            final FlushStats initialStats = shard.flushStats();
            client().prepareIndex("test").setId("1").setSource("{}", MediaTypeRegistry.JSON).get();
            check = () -> {
                assertFalse(shard.shouldPeriodicallyFlush());
                final FlushStats currentStats = shard.flushStats();
                String msg = String.format(
                    Locale.ROOT,
                    "flush stats: total=[%d vs %d], periodic=[%d vs %d]",
                    initialStats.getTotal(),
                    currentStats.getTotal(),
                    initialStats.getPeriodic(),
                    currentStats.getPeriodic()
                );
                assertThat(
                    msg,
                    currentStats.getPeriodic(),
                    either(equalTo(initialStats.getPeriodic() + 1)).or(equalTo(initialStats.getPeriodic() + 2))
                );
                assertThat(
                    msg,
                    currentStats.getTotal(),
                    either(equalTo(initialStats.getTotal() + 1)).or(equalTo(initialStats.getTotal() + 2))
                );
            };
        } else {
            final long generation = getTranslog(shard).currentFileGeneration();
            client().prepareIndex("test").setId("1").setSource("{}", MediaTypeRegistry.JSON).get();
            check = () -> {
                assertFalse(shard.shouldRollTranslogGeneration());
                assertEquals(generation + 1, getTranslog(shard).currentFileGeneration());
            };
        }
        assertBusy(check);
        running.set(false);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        check.run();
    }

    public void testFlushStats() throws Exception {
        final IndexService indexService = createIndex("test");
        ensureGreen();
        Settings settings = Settings.builder().put("index.translog.flush_threshold_size", "" + between(200, 300) + "b").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
        final int numDocs = between(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", MediaTypeRegistry.JSON).get();
        }
        // A flush stats may include the new total count but the old period count - assert eventually.
        assertBusy(() -> {
            final FlushStats flushStats = client().admin().indices().prepareStats("test").clear().setFlush(true).get().getTotal().flush;
            assertThat(flushStats.getPeriodic(), allOf(equalTo(flushStats.getTotal()), greaterThan(0L)));
        });
        assertBusy(() -> assertThat(indexService.getShard(0).shouldPeriodicallyFlush(), equalTo(false)));
        settings = Settings.builder().put("index.translog.flush_threshold_size", (String) null).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();

        client().prepareIndex("test").setId(UUIDs.randomBase64UUID()).setSource("{}", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareFlush("test").setForce(randomBoolean()).setWaitIfOngoing(true).get();
        final FlushStats flushStats = client().admin().indices().prepareStats("test").clear().setFlush(true).get().getTotal().flush;
        assertThat(flushStats.getTotal(), greaterThan(flushStats.getPeriodic()));
    }

    public void testShardHasMemoryBufferOnTranslogRecover() throws Throwable {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test").setId("0").setSource("{\"foo\" : \"bar\"}", MediaTypeRegistry.JSON).get();
        client().prepareDelete("test", "0").get();
        client().prepareIndex("test").setId("1").setSource("{\"foo\" : \"bar\"}", MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE).get();

        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
        shard.close("simon says", false, false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        List<Exception> failures = new ArrayList<>();
        IndexingOperationListener listener = new IndexingOperationListener() {

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                try {
                    assertNotNull(shardRef.get());
                    // this is all IMC needs to do - check current memory and refresh
                    assertTrue(shardRef.get().getIndexBufferRAMBytesUsed() > 0);
                    shardRef.get().refresh("test");
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                try {
                    assertNotNull(shardRef.get());
                    // this is all IMC needs to do - check current memory and refresh
                    assertTrue(shardRef.get().getIndexBufferRAMBytesUsed() > 0);
                    shardRef.get().refresh("test");
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            }
        };
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        final IndexShard newShard = newIndexShard(
            indexService,
            shard,
            wrapper,
            getInstanceFromNode(CircuitBreakerService.class),
            env.nodeId(),
            getInstanceFromNode(ClusterService.class),
            listener
        );
        shardRef.set(newShard);
        recoverShard(newShard);

        try {
            ExceptionsHelper.rethrowAndSuppress(failures);
        } finally {
            newShard.close("just do it", randomBoolean(), false);
        }
    }

    public static final IndexShard recoverShard(IndexShard newShard) throws IOException {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        recoverFromStore(newShard);
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        return newShard;
    }

    public static final IndexShard newIndexShard(
        final IndexService indexService,
        final IndexShard shard,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper,
        final CircuitBreakerService cbs,
        final String nodeId,
        final ClusterService clusterService,
        final IndexingOperationListener... listeners
    ) throws IOException {
        ShardRouting initializingShardRouting = getInitializingShardRouting(shard.routingEntry());
        return new IndexShard(
            initializingShardRouting,
            indexService.getIndexSettings(),
            shard.shardPath(),
            shard.store(),
            indexService.getIndexSortSupplier(),
            indexService.cache(),
            indexService.mapperService(),
            indexService.similarityService(),
            shard.getEngineFactory(),
            shard.getEngineConfigFactory(),
            indexService.getIndexEventListener(),
            wrapper,
            indexService.getThreadPool(),
            indexService.getBigArrays(),
            null,
            Collections.emptyList(),
            Arrays.asList(listeners),
            () -> {},
            RetentionLeaseSyncer.EMPTY,
            cbs,
            (indexSettings, shardRouting) -> new InternalTranslogFactory(),
            SegmentReplicationCheckpointPublisher.EMPTY,
            null,
            null,
            nodeId,
            null,
            DefaultRemoteStoreSettings.INSTANCE,
            false,
            IndexShardTestUtils.getFakeDiscoveryNodes(initializingShardRouting),
            mock(Function.class),
            new MergedSegmentWarmerFactory(null, null, null),
            false,
            OpenSearchTestCase::randomBoolean,
            () -> indexService.getIndexSettings().getRefreshInterval(),
            indexService.getRefreshMutex(),
            clusterService.getClusterApplierService(),
            MergedSegmentPublisher.EMPTY,
            ReferencedSegmentsPublisher.EMPTY,
            null
        );
    }

    private static ShardRouting getInitializingShardRouting(ShardRouting existingShardRouting) {
        ShardRouting shardRouting = newShardRouting(
            existingShardRouting.shardId(),
            existingShardRouting.currentNodeId(),
            null,
            existingShardRouting.primary(),
            ShardRoutingState.INITIALIZING,
            existingShardRouting.allocationId()
        );
        shardRouting = shardRouting.updateUnassigned(
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "fake recovery"),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        return shardRouting;
    }

    public void testInvalidateIndicesRequestCacheWhenRollbackEngine() throws Exception {
        createIndex(
            "test",
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.refresh_interval", -1).build()
        );
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexShard shard = indicesService.getShardOrNull(new ShardId(resolveIndex("test"), 0));
        final SearchRequest countRequest = new SearchRequest("test").source(new SearchSourceBuilder().size(0));
        final long numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", MediaTypeRegistry.JSON).get();
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        shard.refresh("test");
        assertThat(client().search(countRequest).actionGet().getHits().getTotalHits().value(), equalTo(numDocs));
        assertThat(shard.getLocalCheckpoint(), equalTo(shard.seqNoStats().getMaxSeqNo()));

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllPrimaryOperationsPermits(ActionListener.wrap(r -> {
            try {
                shard.resetEngineToGlobalCheckpoint();
            } finally {
                r.close();
                engineResetLatch.countDown();
            }
        }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));
        engineResetLatch.await();

        final long moreDocs = between(10, 20);
        for (int i = 0; i < moreDocs; i++) {
            client().prepareIndex("test").setId(Long.toString(i + numDocs)).setSource("{}", MediaTypeRegistry.JSON).get();
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(
                "numDocs=" + numDocs + " moreDocs=" + moreDocs,
                (long) searcher.getIndexReader().numDocs(),
                equalTo(numDocs + moreDocs)
            );
        }
        assertThat(
            "numDocs=" + numDocs + " moreDocs=" + moreDocs,
            client().search(countRequest).actionGet().getHits().getTotalHits().value(),
            equalTo(numDocs + moreDocs)
        );
    }

    public void testShardChangesWithDefaultDocType() throws Exception {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.translog.flush_threshold_size", "512mb") // do not flush
            .put("index.soft_deletes.enabled", true)
            .build();
        IndexService indexService = createIndexWithSimpleMappings("index", settings, "title", "type=keyword");
        int numOps = between(1, 10);
        for (int i = 0; i < numOps; i++) {
            if (randomBoolean()) {
                client().prepareIndex("index").setId(randomFrom("1", "2")).setSource("{}", MediaTypeRegistry.JSON).get();
            } else {
                client().prepareDelete("index", randomFrom("1", "2")).get();
            }
        }
        IndexShard shard = indexService.getShard(0);
        try (
            Translog.Snapshot luceneSnapshot = shard.newChangesSnapshot("test", 0, numOps - 1, true, randomBoolean());
            Translog.Snapshot translogSnapshot = getTranslog(shard).newSnapshot()
        ) {
            List<Translog.Operation> opsFromLucene = TestTranslog.drainSnapshot(luceneSnapshot, true);
            List<Translog.Operation> opsFromTranslog = TestTranslog.drainSnapshot(translogSnapshot, true);
            assertThat(opsFromLucene, equalTo(opsFromTranslog));
        }
    }

    /**
     * Test that the {@link org.opensearch.index.engine.NoOpEngine} takes precedence over other
     * engine factories if the index is closed.
     */
    public void testNoOpEngineFactoryTakesPrecedence() {
        final String indexName = "closed-index";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen();

        assertAcked(client().admin().indices().prepareClose(indexName));
        ensureGreen();

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState clusterState = clusterService.state();

        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());

        for (IndexShard indexShard : indexService) {
            assertThat(indexShard.getEngine(), instanceOf(NoOpEngine.class));
        }
    }

    public void testLimitNumberOfRetainedTranslogFiles() throws Exception {
        String indexName = "test";
        int translogRetentionTotalFiles = randomIntBetween(0, 50);
        Settings.Builder settings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING.getKey(), translogRetentionTotalFiles);
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), new ByteSizeValue(between(1, 1024 * 1024)));
        }
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), TimeValue.timeValueMillis(between(1, 10_000)));
        }
        IndexService indexService = createIndex(indexName, settings.build());
        IndexShard shard = indexService.getShard(0);
        shard.rollTranslogGeneration();
        CheckedRunnable<IOException> checkTranslog = () -> {
            try (Stream<Path> files = Files.list(getTranslog(shard).location()).sorted(Comparator.reverseOrder())) {
                long totalFiles = files.filter(f -> f.getFileName().toString().endsWith(Translog.TRANSLOG_FILE_SUFFIX)).count();
                assertThat(totalFiles, either(lessThanOrEqualTo((long) translogRetentionTotalFiles)).or(equalTo(1L)));
            }
        };
        for (int i = 0; i < 100; i++) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", MediaTypeRegistry.JSON).get();
            if (randomInt(100) < 10) {
                client().admin().indices().prepareFlush(indexName).setWaitIfOngoing(true).get();
                checkTranslog.run();
            }
            if (randomInt(100) < 10) {
                shard.rollTranslogGeneration();
            }
        }
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();
        checkTranslog.run();
    }

    /**
     * Asserts that there are no files in the specified path
     */
    private void assertPathHasBeenCleared(Path path) {
        logger.info("--> checking that [{}] has been cleared", path);
        int count = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (Files.exists(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path file : stream) {
                    // Skip files added by Lucene's ExtraFS
                    if (file.getFileName().toString().startsWith("extra")) {
                        continue;
                    }
                    logger.info("--> found file: [{}]", file.toAbsolutePath().toString());
                    if (Files.isDirectory(file)) {
                        assertPathHasBeenCleared(file);
                    } else if (Files.isRegularFile(file)) {
                        count++;
                        sb.append(file.toAbsolutePath().toString());
                        sb.append("\n");
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        sb.append("]");
        assertThat(count + " files exist that should have been cleaned:\n" + sb.toString(), count, equalTo(0));
    }

    private static void assertAllIndicesRemovedAndDeletionCompleted(Iterable<IndicesService> indicesServices) throws Exception {
        for (IndicesService indicesService : indicesServices) {
            assertBusy(() -> assertFalse(indicesService.iterator().hasNext()), 1, TimeUnit.MINUTES);
            assertBusy(() -> assertFalse(indicesService.hasUncompletedPendingDeletes()), 1, TimeUnit.MINUTES);
        }
    }
}

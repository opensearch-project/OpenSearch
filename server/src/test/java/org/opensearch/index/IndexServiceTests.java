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

package org.opensearch.index;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.opensearch.Version;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.shard.IndexShardTestCase.getEngine;
import static org.opensearch.test.InternalSettingsPlugin.TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.core.IsEqual.equalTo;

/** Unit test(s) for IndexService */
public class IndexServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public static CompressedXContent filter(QueryBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedXContent(builder.toString());
    }

    public void testBaseAsyncTask() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<CountDownLatch> latch2 = new AtomicReference<>(new CountDownLatch(1));
        final AtomicInteger count = new AtomicInteger();
        IndexService.BaseAsyncTask task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(1)) {
            @Override
            protected void runInternal() {
                final CountDownLatch l1 = latch.get();
                final CountDownLatch l2 = latch2.get();
                count.incrementAndGet();
                assertTrue("generic threadpool is configured", Thread.currentThread().getName().contains("[generic]"));
                l1.countDown();
                try {
                    l2.await();
                } catch (InterruptedException e) {
                    fail("interrupted");
                }
                if (randomBoolean()) { // task can throw exceptions!!
                    if (randomBoolean()) {
                        throw new RuntimeException("foo");
                    } else {
                        throw new RuntimeException("bar");
                    }
                }
            }

            @Override
            protected String getThreadPool() {
                return ThreadPool.Names.GENERIC;
            }
        };

        latch.get().await();
        latch.set(new CountDownLatch(1));
        assertEquals(1, count.get());
        // here we need to swap first before we let it go otherwise threads might be very fast and run that task twice due to
        // random exception and the schedule interval is 1ms
        latch2.getAndSet(new CountDownLatch(1)).countDown();
        latch.get().await();
        assertEquals(2, count.get());
        task.close();
        latch2.get().countDown();
        assertEquals(2, count.get());

        task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(1000000)) {
            @Override
            protected void runInternal() {

            }
        };
        assertTrue(task.mustReschedule());

        // now close the index
        final Index index = indexService.index();
        assertAcked(client().admin().indices().prepareClose(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));

        final IndexService closedIndexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertFalse(task.mustReschedule());
        assertFalse(task.isClosed());
        assertEquals(1000000, task.getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));
        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);

        task = new IndexService.BaseAsyncTask(indexService, TimeValue.timeValueMillis(100000)) {
            @Override
            protected void runInternal() {

            }
        };
        assertTrue(task.mustReschedule());
        assertFalse(task.isClosed());
        assertTrue(task.isScheduled());

        indexService.close("simon says", false);
        assertFalse("no shards left", task.mustReschedule());
        assertTrue(task.isScheduled());
        task.close();
        assertFalse(task.isScheduled());
    }

    public void testRefreshTaskIsUpdated() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        IndexService.AsyncRefreshTask refreshTask = indexService.getRefreshTask();
        assertEquals(1000, refreshTask.getInterval().millis());
        assertTrue(indexService.getRefreshTask().mustReschedule());

        // now disable
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1))
            .get();
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());
        assertFalse(refreshTask.isScheduled());

        // set it to 100ms
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "100ms"))
            .get();
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());

        refreshTask = indexService.getRefreshTask();
        assertTrue(refreshTask.mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertEquals(100, refreshTask.getInterval().millis());

        // set it to 200ms
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "200ms"))
            .get();
        assertNotSame(refreshTask, indexService.getRefreshTask());
        assertTrue(refreshTask.isClosed());

        refreshTask = indexService.getRefreshTask();
        assertTrue(refreshTask.mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertEquals(200, refreshTask.getInterval().millis());

        // set it to 200ms again
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "200ms"))
            .get();
        assertSame(refreshTask, indexService.getRefreshTask());
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertFalse(refreshTask.isClosed());
        assertEquals(200, refreshTask.getInterval().millis());

        // now close the index
        final Index index = indexService.index();
        assertAcked(client().admin().indices().prepareClose(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));

        final IndexService closedIndexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(refreshTask, closedIndexService.getRefreshTask());
        assertFalse(closedIndexService.getRefreshTask().mustReschedule());
        assertFalse(closedIndexService.getRefreshTask().isClosed());
        assertEquals(200, closedIndexService.getRefreshTask().getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));
        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);
        refreshTask = indexService.getRefreshTask();
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(refreshTask.isScheduled());
        assertFalse(refreshTask.isClosed());

        indexService.close("simon says", false);
        assertFalse(refreshTask.isScheduled());
        assertTrue(refreshTask.isClosed());
    }

    public void testFsyncTaskIsRunning() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            .build();
        IndexService indexService = createIndex("test", settings);
        IndexService.AsyncTranslogFSync fsyncTask = indexService.getFsyncTask();
        assertNotNull(fsyncTask);
        assertEquals(5000, fsyncTask.getInterval().millis());
        assertTrue(fsyncTask.mustReschedule());
        assertTrue(fsyncTask.isScheduled());

        // now close the index
        final Index index = indexService.index();
        assertAcked(client().admin().indices().prepareClose(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));

        final IndexService closedIndexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(fsyncTask, closedIndexService.getFsyncTask());
        assertFalse(closedIndexService.getFsyncTask().mustReschedule());
        assertFalse(closedIndexService.getFsyncTask().isClosed());
        assertEquals(5000, closedIndexService.getFsyncTask().getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        assertBusy(() -> assertTrue("Index not found: " + index.getName(), getInstanceFromNode(IndicesService.class).hasIndex(index)));
        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(index);
        assertNotSame(closedIndexService, indexService);
        fsyncTask = indexService.getFsyncTask();
        assertTrue(indexService.getRefreshTask().mustReschedule());
        assertTrue(fsyncTask.isScheduled());
        assertFalse(fsyncTask.isClosed());

        indexService.close("simon says", false);
        assertFalse(fsyncTask.isScheduled());
        assertTrue(fsyncTask.isClosed());

        indexService = createIndex("test1", Settings.EMPTY);
        assertNull(indexService.getFsyncTask());
    }

    public void testRefreshActuallyWorks() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        ensureGreen("test");
        IndexService.AsyncRefreshTask refreshTask = indexService.getRefreshTask();
        assertEquals(1000, refreshTask.getInterval().millis());
        assertTrue(indexService.getRefreshTask().mustReschedule());
        IndexShard shard = indexService.getShard(0);
        client().prepareIndex("test").setId("0").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        // now disable the refresh
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1))
            .get();
        // when we update we reschedule the existing task AND fire off an async refresh to make sure we make everything visible
        // before that this is why we need to wait for the refresh task to be unscheduled and the first doc to be visible
        assertTrue(refreshTask.isClosed());
        refreshTask = indexService.getRefreshTask();
        assertBusy(() -> {
            // this one either becomes visible due to a concurrently running scheduled refresh OR due to the force refresh
            // we are running on updateMetadata if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(1, search.totalHits.value);
            }
        });
        assertFalse(refreshTask.isClosed());
        // refresh every millisecond
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1ms"))
            .get();
        assertTrue(refreshTask.isClosed());
        assertBusy(() -> {
            // this one becomes visible due to the force refresh we are running on updateMetadata if the interval changes
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(2, search.totalHits.value);
            }
        });
        client().prepareIndex("test").setId("2").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertBusy(() -> {
            // this one becomes visible due to the scheduled refresh
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(3, search.totalHits.value);
            }
        });
    }

    public void testAsyncFsyncActuallyWorks() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "100ms") // very often :)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            .build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertTrue(indexService.getRefreshTask().mustReschedule());
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertFalse(shard.isSyncNeeded()));
    }

    public void testRescheduleAsyncFsync() throws Exception {
        final Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            .build();
        final IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertNull(indexService.getFsyncTask());

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC))
            .get();

        assertNotNull(indexService.getFsyncTask());
        assertTrue(indexService.getFsyncTask().mustReschedule());
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertNotNull(indexService.getFsyncTask());
        final IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertFalse(shard.isSyncNeeded()));

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();
        assertNull(indexService.getFsyncTask());

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC))
            .get();
        assertNotNull(indexService.getFsyncTask());
    }

    public void testAsyncTranslogTrimActuallyWorks() throws Exception {
        Settings settings = Settings.builder()
            .put(TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING.getKey(), "100ms") // very often :)
            .build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());
        client().prepareIndex("test").setId("1").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareFlush("test").get();
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), -1)
                    .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), -1)
            )
            .get();
        IndexShard shard = indexService.getShard(0);
        assertBusy(() -> assertThat(IndexShardTestCase.getTranslog(shard).totalOperations(), equalTo(0)));
    }

    public void testAsyncTranslogTrimTaskOnClosedIndex() throws Exception {
        final String indexName = "test";
        IndexService indexService = createIndex(
            indexName,
            Settings.builder().put(TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING.getKey(), "200ms").build()
        );

        Translog translog = IndexShardTestCase.getTranslog(indexService.getShard(0));

        int translogOps = 0;
        final int numDocs = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex()
                .setIndex(indexName)
                .setId(String.valueOf(i))
                .setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON)
                .get();
            translogOps++;
            if (randomBoolean()) {
                client().admin().indices().prepareFlush(indexName).get();
                if (indexService.getIndexSettings().isSoftDeleteEnabled()) {
                    translogOps = 0;
                }
            }
        }
        assertThat(translog.totalOperations(), equalTo(translogOps));
        assertThat(translog.stats().estimatedNumberOfOperations(), equalTo(translogOps));
        assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(ActiveShardCount.DEFAULT));

        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(indexService.index());
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());

        final Engine readOnlyEngine = getEngine(indexService.getShard(0));
        assertBusy(() -> assertTrue(isTranslogEmpty(readOnlyEngine)));

        assertAcked(client().admin().indices().prepareOpen("test").setWaitForActiveShards(ActiveShardCount.DEFAULT));

        indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(indexService.index());
        translog = IndexShardTestCase.getTranslog(indexService.getShard(0));
        assertThat(translog.totalOperations(), equalTo(0));
        assertThat(translog.stats().estimatedNumberOfOperations(), equalTo(0));
    }

    boolean isTranslogEmpty(Engine engine) {
        long tlogSize = engine.translogManager().getTranslogStats().getTranslogSizeInBytes();
        // translog contains 1(or 2 in some corner cases) empty readers.
        return tlogSize == Translog.DEFAULT_HEADER_SIZE_IN_BYTES || tlogSize == 2 * Translog.DEFAULT_HEADER_SIZE_IN_BYTES;
    }

    public void testIllegalFsyncInterval() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "0ms") // disable
            .build();
        try {
            createIndex("test", settings);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse value [0ms] for setting [index.translog.sync_interval], must be >= [100ms]", ex.getMessage());
        }
    }

    public void testUpdateSyncIntervalDynamically() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "10s") // very often :)
            .build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");
        assertNull(indexService.getFsyncTask());

        Settings.Builder builder = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "5s")
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC.name());

        client().admin().indices().prepareUpdateSettings("test").setSettings(builder).get();

        assertNotNull(indexService.getFsyncTask());
        assertTrue(indexService.getFsyncTask().mustReschedule());

        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals("5s", indexMetadata.getSettings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));

        client().admin().indices().prepareClose("test").get();
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "20s"))
            .get();
        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals("20s", indexMetadata.getSettings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()));
    }

    public void testUpdateRemoteTranslogBufferIntervalDynamically() {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "10s").build();
        IndexService indexService = createIndex("test", settings);
        ensureGreen("test");

        Settings.Builder builder = Settings.builder().put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "5s");
        client().admin().indices().prepareUpdateSettings("test").setSettings(builder).get();
        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals("5s", indexMetadata.getSettings().get(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey()));

        client().admin().indices().prepareClose("test").get();
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "20s"))
            .get();
        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals("20s", indexMetadata.getSettings().get(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey()));
    }

    public void testIndexSort() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "0ms") // disable
            .putList("index.sort.field", "sortfield")
            .build();
        try {
            // Integer index sort should be remained to int sort type
            IndexService index = createIndex("test", settings, createTestMapping("integer"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.INT);

            // Long index sort should be remained to long sort type
            index = createIndex("test", settings, createTestMapping("long"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.LONG);

            // Float index sort should be remained to float sort type
            index = createIndex("test", settings, createTestMapping("float"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.FLOAT);

            // Double index sort should be remained to double sort type
            index = createIndex("test", settings, createTestMapping("double"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.DOUBLE);

            // String index sort should be remained to string sort type
            index = createIndex("test", settings, createTestMapping("string"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.STRING);
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse value [0ms] for setting [index.translog.sync_interval], must be >= [100ms]", ex.getMessage());
        }
    }

    public void testIndexSortBackwardCompatible() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "0ms") // disable
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_2_6_1)
            .putList("index.sort.field", "sortfield")
            .build();
        try {
            // Integer index sort should be converted to long sort type
            IndexService index = createIndex("test", settings, createTestMapping("integer"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.LONG);

            // Long index sort should be remained to long sort type
            index = createIndex("test", settings, createTestMapping("long"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.LONG);

            // Float index sort should be remained to float sort type
            index = createIndex("test", settings, createTestMapping("float"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.FLOAT);

            // Double index sort should be remained to double sort type
            index = createIndex("test", settings, createTestMapping("double"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.DOUBLE);

            // String index sort should be remained to string sort type
            index = createIndex("test", settings, createTestMapping("string"));
            assertTrue(index.getIndexSortSupplier().get().getSort()[0].getType() == SortField.Type.STRING);
        } catch (IllegalArgumentException ex) {
            assertEquals("failed to parse value [0ms] for setting [index.translog.sync_interval], must be >= [100ms]", ex.getMessage());
        }
    }

    public void testReplicationTask() throws Exception {
        // create with docrep - task should not schedule
        IndexService indexService = createIndex(
            "docrep_index",
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build()
        );
        final Index index = indexService.index();
        ensureGreen(index.getName());
        IndexService.AsyncReplicationTask task = indexService.getReplicationTask();
        assertFalse(task.isScheduled());
        assertFalse(task.mustReschedule());

        // create for segrep - task should schedule
        indexService = createIndex(
            "segrep_index",
            Settings.builder()
                .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s")
                .build()
        );
        final Index srIndex = indexService.index();
        ensureGreen(srIndex.getName());
        task = indexService.getReplicationTask();
        assertTrue(task.isScheduled());
        assertTrue(task.mustReschedule());
        assertEquals(5000, task.getInterval().millis());

        // test we can update the interval
        client().admin()
            .indices()
            .prepareUpdateSettings("segrep_index")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s"))
            .get();

        IndexService.AsyncReplicationTask updatedTask = indexService.getReplicationTask();
        assertNotSame(task, updatedTask);
        assertFalse(task.isScheduled());
        assertTrue(task.isClosed());
        assertTrue(updatedTask.isScheduled());
        assertTrue(updatedTask.mustReschedule());
        assertEquals(1000, updatedTask.getInterval().millis());
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL_SETTING.getKey(), true)
            .build();
    }

    private static String createTestMapping(String type) {
        return "  \"properties\": {\n"
            + "    \"test\": {\n"
            + "      \"type\": \"text\"\n"
            + "    },\n"
            + "    \"sortfield\": {\n"
            + "      \"type\": \" + type + \"\n"
            + "    }\n"
            + "  }";
    }
}

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
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.opensearch.Version;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService.AsyncRefreshTask;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShard.AsyncShardRefreshTask;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opensearch.index.shard.IndexShardTestCase.getEngine;
import static org.opensearch.test.InternalSettingsPlugin.TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.awaitility.Awaitility.await;

/** Unit test(s) for IndexService */
public class IndexServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
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
        final IndexService closedIndexService = getIndexService(index);
        assertNotSame(indexService, closedIndexService);
        assertFalse(task.mustReschedule());
        assertFalse(task.isClosed());
        assertEquals(1000000, task.getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        indexService = getIndexService(index);
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
        AsyncRefreshTask refreshTask = indexService.getRefreshTask();
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
        final IndexService closedIndexService = getIndexService(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(refreshTask, closedIndexService.getRefreshTask());
        assertFalse(closedIndexService.getRefreshTask().mustReschedule());
        assertFalse(closedIndexService.getRefreshTask().isClosed());
        assertEquals(200, closedIndexService.getRefreshTask().getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        indexService = getIndexService(index);
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
        final IndexService closedIndexService = getIndexService(index);
        assertNotSame(indexService, closedIndexService);
        assertNotSame(fsyncTask, closedIndexService.getFsyncTask());
        assertFalse(closedIndexService.getFsyncTask().mustReschedule());
        assertFalse(closedIndexService.getFsyncTask().isClosed());
        assertEquals(5000, closedIndexService.getFsyncTask().getInterval().millis());

        // now reopen the index
        assertAcked(client().admin().indices().prepareOpen(index.getName()));
        indexService = getIndexService(index);
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
        AsyncRefreshTask refreshTask = indexService.getRefreshTask();
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
                assertEquals(1, search.totalHits.value());
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
                assertEquals(2, search.totalHits.value());
            }
        });
        client().prepareIndex("test").setId("2").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertBusy(() -> {
            // this one becomes visible due to the scheduled refresh
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                TopDocs search = searcher.search(new MatchAllDocsQuery(), 10);
                assertEquals(3, search.totalHits.value());
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
        indexService = getIndexService(indexService.index());
        assertTrue(indexService.getTrimTranslogTask().mustReschedule());

        final Engine readOnlyEngine = getEngine(indexService.getShard(0));
        assertBusy(() -> assertTrue(isTranslogEmpty(readOnlyEngine)));

        assertAcked(client().admin().indices().prepareOpen("test").setWaitForActiveShards(ActiveShardCount.DEFAULT));
        indexService = getIndexService(indexService.index());
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
        runSortFieldTest(Settings.builder(), SortField.Type.INT);
    }

    public void testIndexSortBackwardCompatible() {
        runSortFieldTest(
            Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_2_6_1),
            SortField.Type.LONG
        );
    }

    private void runSortFieldTest(Settings.Builder settingsBuilder, SortField.Type expectedIntType) {
        String[] sortFieldNames = new String[] { "int-sortfield", "long-sortfield", "double-sortfield", "float-sortfield" };
        Settings settings = settingsBuilder.putList("index.sort.field", sortFieldNames).build();
        IndexService index = createIndexWithSimpleMappings(
            "sort-field-test-index",
            settings,
            "int-sortfield",
            "type=integer",
            "long-sortfield",
            "type=long",
            "double-sortfield",
            "type=double",
            "float-sortfield",
            "type=float"
        );
        SortField[] sortFields = index.getIndexSortSupplier().get().getSort();
        Map<String, SortField.Type> map = Arrays.stream(sortFields)
            .filter(s -> s instanceof SortedNumericSortField)
            .map(s -> (SortedNumericSortField) s)
            .collect(Collectors.toMap(SortField::getField, SortedNumericSortField::getNumericType));
        assertThat(map.keySet(), containsInAnyOrder(sortFieldNames));
        assertThat(map.get("int-sortfield"), equalTo(expectedIntType));
        assertThat(map.get("long-sortfield"), equalTo(SortField.Type.LONG));
        assertThat(map.get("float-sortfield"), equalTo(SortField.Type.FLOAT));
        assertThat(map.get("double-sortfield"), equalTo(SortField.Type.DOUBLE));
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

    public void testPublishReferencedSegmentsTask() throws Exception {
        // create with docrep - task should not schedule
        IndexService indexService = createIndex(
            "docrep_index",
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build()
        );
        final Index index = indexService.index();
        ensureGreen(index.getName());
        IndexService.AsyncPublishReferencedSegmentsTask task = indexService.getPublishReferencedSegmentsTask();
        assertFalse(task.isScheduled());
        assertFalse(task.mustReschedule());
        assertFalse(task.shouldRun());

        // create for segrep - task should schedule
        indexService = createIndex(
            "segrep_index",
            Settings.builder()
                .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                .put(IndexSettings.INDEX_PUBLISH_REFERENCED_SEGMENTS_INTERVAL_SETTING.getKey(), "5s")
                .build()
        );

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(RecoverySettings.INDICES_MERGED_SEGMENT_REPLICATION_WARMER_ENABLED_SETTING.getKey(), true)
            )
            .get();

        final Index srIndex = indexService.index();
        ensureGreen(srIndex.getName());
        task = indexService.getPublishReferencedSegmentsTask();
        assertTrue(task.isScheduled());
        assertTrue(task.mustReschedule());
        assertEquals(5000, task.getInterval().millis());
        assertTrue(task.shouldRun());

        // test update the refresh interval, AsyncPublishReferencedSegmentsTask should not be updated
        client().admin()
            .indices()
            .prepareUpdateSettings("segrep_index")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "5s"))
            .get();

        IndexService.AsyncPublishReferencedSegmentsTask updatedTask = indexService.getPublishReferencedSegmentsTask();
        assertSame(task, updatedTask);
        assertTrue(task.isScheduled());
        assertTrue(task.mustReschedule());
        assertEquals(5000, task.getInterval().millis());
        assertTrue(task.shouldRun());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().putNull(RecoverySettings.INDICES_MERGED_SEGMENT_REPLICATION_WARMER_ENABLED_SETTING.getKey())
            )
            .get();

        // test we can update the publishReferencedSegmentsInterval
        client().admin()
            .indices()
            .prepareUpdateSettings("segrep_index")
            .setSettings(Settings.builder().put(IndexSettings.INDEX_PUBLISH_REFERENCED_SEGMENTS_INTERVAL_SETTING.getKey(), "1s"))
            .get();

        updatedTask = indexService.getPublishReferencedSegmentsTask();
        assertNotSame(task, updatedTask);
        assertFalse(task.isScheduled());
        assertTrue(task.isClosed());
        assertTrue(updatedTask.isScheduled());
        assertTrue(updatedTask.mustReschedule());
        assertEquals(1000, updatedTask.getInterval().millis());
        assertFalse(task.shouldRun());
    }

    public void testBaseAsyncTaskWithFixedIntervalDisabled() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        CountDownLatch latch = new CountDownLatch(1);
        try (
            IndexService.BaseAsyncTask task = new IndexService.BaseAsyncTask(
                indexService,
                TimeValue.timeValueSeconds(5),
                () -> Boolean.FALSE
            ) {
                @Override
                protected void runInternal() {
                    try {
                        Thread.sleep(2000);
                        latch.countDown();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        ) {
            // With refresh fixed interval disabled, the sleep duration is always the refresh interval
            long sleepDuration = task.getSleepDuration().seconds();
            assertEquals(5, sleepDuration);
            task.run();
            latch.await();
            sleepDuration = task.getSleepDuration().seconds();
            assertEquals(0, latch.getCount());
            indexService.close("test", false);
            assertEquals(5, sleepDuration);
        }
    }

    public void testBaseAsyncTaskWithFixedIntervalEnabled() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        CountDownLatch latch = new CountDownLatch(1);
        try (
            IndexService.BaseAsyncTask task = new IndexService.BaseAsyncTask(
                indexService,
                TimeValue.timeValueSeconds(5),
                () -> Boolean.TRUE
            ) {
                @Override
                protected void runInternal() {
                    try {
                        Thread.sleep(2000);
                        latch.countDown();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        ) {
            // In zero state, we have a random sleep duration
            long sleepDurationNanos = task.getSleepDuration().nanos();
            assertTrue(sleepDurationNanos > 0);
            task.run();
            latch.await();
            // Since we have refresh taking up 2s, then the next refresh should have sleep duration of 3s. Here we check
            // the sleep duration to be non-zero since the sleep duration is calculated dynamically.
            sleepDurationNanos = task.getSleepDuration().nanos();
            assertTrue(sleepDurationNanos > 0);
            assertEquals(0, latch.getCount());
            indexService.close("test", false);
            assertBusy(() -> { assertEquals(TimeValue.ZERO, task.getSleepDuration()); });
        }
    }

    public void testBaseAsyncTaskWithFixedIntervalEnabledAndLongerRefresh() throws Exception {
        IndexService indexService = createIndex("test", Settings.EMPTY);
        CountDownLatch latch = new CountDownLatch(1);
        try (
            IndexService.BaseAsyncTask task = new IndexService.BaseAsyncTask(
                indexService,
                TimeValue.timeValueSeconds(1),
                () -> Boolean.TRUE
            ) {
                @Override
                protected void runInternal() {
                    try {
                        Thread.sleep(2000);
                        latch.countDown();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        ) {
            // In zero state, we have a random sleep duration
            long sleepDurationNanos = task.getSleepDuration().nanos();
            assertTrue(sleepDurationNanos > 0);
            task.run();
            latch.await();
            indexService.close("test", false);
            // Since we have refresh taking up 2s and refresh interval as 1s, then the next refresh should happen immediately.
            sleepDurationNanos = task.getSleepDuration().nanos();
            assertEquals(0, sleepDurationNanos);
            assertEquals(0, latch.getCount());
        }
    }

    public void testRefreshTaskUpdatesWithDynamicShardLevelRefreshes() throws Exception {
        // By default, parallel shard refresh is disabled
        Settings settings = client().admin().cluster().prepareState().get().getState().getMetadata().settings();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        assertFalse(indicesService.isShardLevelRefreshEnabled());

        // Create an index and verify no searchable docs
        String indexName = "test-index";
        IndexService indexService = createIndex(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 0);
        assertFalse(indexService.isShardLevelRefreshEnabled());

        // Index a doc and verify that it is searchable
        client().prepareIndex(indexName).setId("0").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertBusy(() -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 1));

        IndexShard shard = indexService.getShard(0);

        // Verify that index level refresh task is non-null
        assertNotNull(indexService.getRefreshTask());
        // Verify that shard level refresh task is null
        assertNull(shard.getRefreshTask());

        // Change refresh interval
        AsyncRefreshTask indexRefreshTask = indexService.getRefreshTask();
        assertEquals(1, indexRefreshTask.getInterval().seconds());

        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1100ms"))
            .get();
        assertNotSame(indexRefreshTask, indexService.getRefreshTask());
        assertTrue(indexRefreshTask.isClosed());
        assertFalse(indexRefreshTask.isScheduled());
        assertEquals(1100, indexService.getRefreshTask().getInterval().millis());

        // Verify that shard level refresh task is null
        assertNull(shard.getRefreshTask());

        // Ingest another doc and see refresh is still working
        client().prepareIndex(indexName).setId("1").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertBusy(() -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 2));

        // Enable parallel shard refresh
        indexRefreshTask = indexService.getRefreshTask();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IndicesService.CLUSTER_REFRESH_SHARD_LEVEL_ENABLED_SETTING.getKey(), true))
            .get();
        settings = client().admin().cluster().prepareState().get().getState().getMetadata().settings();
        assertTrue(indicesService.isShardLevelRefreshEnabled());
        assertNotSame(indexRefreshTask, indexService.getRefreshTask());
        assertTrue(indexRefreshTask.isClosed());
        assertFalse(indexRefreshTask.isScheduled());
        assertNull(indexService.getRefreshTask());
        assertTrue(indexService.isShardLevelRefreshEnabled());

        // Ingest another doc and see refresh is still working
        client().prepareIndex(indexName).setId("2").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertBusy(() -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 3));

        // Verify that shard level refresh task is not null
        AsyncShardRefreshTask shardRefreshTask = shard.getRefreshTask();
        assertNotNull(shardRefreshTask);
        assertFalse(shardRefreshTask.isClosed());
        assertTrue(shardRefreshTask.isScheduled());
        assertEquals(1100, shardRefreshTask.getInterval().millis());

        // Change refresh interval
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1200ms"))
            .get();
        assertNotSame(shardRefreshTask, shard.getRefreshTask());
        assertTrue(shardRefreshTask.isClosed());
        assertFalse(shardRefreshTask.isScheduled());
        assertEquals(1200, shard.getRefreshTask().getInterval().millis());

        // Ingest another doc and see refresh is still working
        client().prepareIndex(indexName).setId("3").setSource("{\"foo\": \"bar\"}", MediaTypeRegistry.JSON).get();
        assertBusy(() -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), 4));

        // Disable parallel shard refresh
        shardRefreshTask = shard.getRefreshTask();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IndicesService.CLUSTER_REFRESH_SHARD_LEVEL_ENABLED_SETTING.getKey(), false))
            .get();
        settings = client().admin().cluster().prepareState().get().getState().getMetadata().settings();
        assertFalse(indicesService.isShardLevelRefreshEnabled());
        assertNotSame(shardRefreshTask, shard.getRefreshTask());
        assertTrue(shardRefreshTask.isClosed());
        assertFalse(shardRefreshTask.isScheduled());
        assertNull(shard.getRefreshTask());
        assertEquals(1200, indexService.getRefreshTask().getInterval().millis());
        assertFalse(indexService.isShardLevelRefreshEnabled());

        // OS test case fails if test leaves behind transient cluster setting so need to clear it.
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull("*")).get();
    }

    private IndexService getIndexService(Index index) {
        return await().atMost(10, TimeUnit.SECONDS)
            .until(() -> getInstanceFromNode(IndicesService.class).indexService(index), notNullValue());
    }
}

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

package org.opensearch.index.shard;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.arrayContaining;

/**
 * Tests how {@linkplain RefreshListeners} interacts with {@linkplain InternalEngine}.
 */
public class RefreshListenersTests extends OpenSearchTestCase {
    private RefreshListeners listeners;
    private Engine engine;
    private volatile int maxListeners;
    private ThreadPool threadPool;
    private Store store;
    private MeanMetric refreshMetric;

    @Before
    public void setupListeners() throws Exception {
        // Setup dependencies of the listeners
        maxListeners = randomIntBetween(1, 1000);
        // Now setup the InternalEngine which is much more complicated because we aren't mocking anything
        threadPool = new TestThreadPool(getTestName());
        refreshMetric = new MeanMetric();
        listeners = new RefreshListeners(
            () -> maxListeners,
            () -> engine.refresh("too-many-listeners"),
            logger,
            threadPool.getThreadContext(),
            refreshMetric
        );

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
        ShardId shardId = new ShardId(new Index("index", "_na_"), 1);
        String allocationId = UUIDs.randomBase64UUID(random());
        Directory directory = newDirectory();
        store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            createTempDir("translog"),
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );
        Engine.EventListener eventListener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
                // we don't need to notify anybody in this test
            }
        };
        store.createEmpty(Version.CURRENT.luceneVersion);
        final long primaryTerm = randomNonNegativeLong();
        final String translogUUID = Translog.createEmptyTranslog(
            translogConfig.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm
        );
        store.associateIndexWithNewTranslog(translogUUID);
        EngineConfig config = new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(newMergePolicy())
            .analyzer(iwc.getAnalyzer())
            .similarity(iwc.getSimilarity())
            .codecService(new CodecService(null, indexSettings, logger))
            .eventListener(eventListener)
            .queryCache(IndexSearcher.getDefaultQueryCache())
            .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(Collections.singletonList(listeners))
            .internalRefreshListener(Collections.emptyList())
            .circuitBreakerService(new NoneCircuitBreakerService())
            .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(() -> primaryTerm)
            .tombstoneDocSupplier(EngineTestCase.tombstoneDocSupplier())
            .build();
        engine = new InternalEngine(config);
        engine.translogManager().recoverFromTranslog((s) -> 0, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        listeners.setCurrentRefreshLocationSupplier(engine.translogManager()::getTranslogLastWriteLocation);
    }

    @After
    public void tearDownListeners() throws Exception {
        IOUtils.close(engine, store);
        terminate(threadPool);
    }

    public void testBeforeRefresh() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        DummyRefreshListener listener = new DummyRefreshListener();
        assertFalse(listeners.addOrNotify(index.getTranslogLocation(), listener));
        assertNull(listener.forcedRefresh.get());
        assertEquals(1, listeners.pendingCount());
        engine.refresh("I said so");
        assertFalse(listener.forcedRefresh.get());
        listener.assertNoError();
        assertEquals(0, listeners.pendingCount());
    }

    public void testAfterRefresh() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        engine.refresh("I said so");
        if (randomBoolean()) {
            index(randomFrom("1" /* same document */, "2" /* different document */));
            if (randomBoolean()) {
                engine.refresh("I said so");
            }
        }
        DummyRefreshListener listener = new DummyRefreshListener();
        assertTrue(listeners.addOrNotify(index.getTranslogLocation(), listener));
        assertFalse(listener.forcedRefresh.get());
        listener.assertNoError();
        assertEquals(0, listeners.pendingCount());
    }

    public void testContextIsPreserved() throws IOException, InterruptedException {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult index = index("1");
        CountDownLatch latch = new CountDownLatch(1);
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader("test", "foobar");
            assertFalse(listeners.addOrNotify(index.getTranslogLocation(), forced -> {
                assertEquals("foobar", threadPool.getThreadContext().getHeader("test"));
                latch.countDown();
            }));
        }
        assertNull(threadPool.getThreadContext().getHeader("test"));
        assertEquals(1, latch.getCount());
        engine.refresh("I said so");
        latch.await();
    }

    public void testTooMany() throws Exception {
        assertEquals(0, listeners.pendingCount());
        assertFalse(listeners.refreshNeeded());
        Engine.IndexResult index = index("1");

        // Fill the listener slots
        List<DummyRefreshListener> nonForcedListeners = new ArrayList<>(maxListeners);
        for (int i = 0; i < maxListeners; i++) {
            DummyRefreshListener listener = new DummyRefreshListener();
            nonForcedListeners.add(listener);
            listeners.addOrNotify(index.getTranslogLocation(), listener);
            assertTrue(listeners.refreshNeeded());
        }

        // We shouldn't have called any of them
        for (DummyRefreshListener listener : nonForcedListeners) {
            assertNull("Called listener too early!", listener.forcedRefresh.get());
        }
        assertEquals(maxListeners, listeners.pendingCount());

        // Add one more listener which should cause a refresh.
        DummyRefreshListener forcingListener = new DummyRefreshListener();
        listeners.addOrNotify(index.getTranslogLocation(), forcingListener);
        assertTrue("Forced listener wasn't forced?", forcingListener.forcedRefresh.get());
        forcingListener.assertNoError();

        // That forces all the listeners through. It would be on the listener ThreadPool but we've made all of those execute immediately.
        for (DummyRefreshListener listener : nonForcedListeners) {
            assertEquals("Expected listener called with unforced refresh!", Boolean.FALSE, listener.forcedRefresh.get());
            listener.assertNoError();
        }
        assertFalse(listeners.refreshNeeded());
        assertEquals(0, listeners.pendingCount());
    }

    public void testClose() throws Exception {
        assertEquals(0, listeners.pendingCount());
        Engine.IndexResult refreshedOperation = index("1");
        engine.refresh("I said so");
        Engine.IndexResult unrefreshedOperation = index("1");
        {
            /* Closing flushed pending listeners as though they were refreshed. Since this can only happen when the index is closed and no
             * longer useful there doesn't seem much point in sending the listener some kind of "I'm closed now, go away" enum value. */
            DummyRefreshListener listener = new DummyRefreshListener();
            assertFalse(listeners.addOrNotify(unrefreshedOperation.getTranslogLocation(), listener));
            assertNull(listener.forcedRefresh.get());
            listeners.close();
            assertFalse(listener.forcedRefresh.get());
            listener.assertNoError();
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
        {
            // If you add a listener for an already refreshed location then it'll just fire even if closed
            DummyRefreshListener listener = new DummyRefreshListener();
            assertTrue(listeners.addOrNotify(refreshedOperation.getTranslogLocation(), listener));
            assertFalse(listener.forcedRefresh.get());
            listener.assertNoError();
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
        {
            // But adding a listener to a non-refreshed location will fail
            DummyRefreshListener listener = new DummyRefreshListener();
            Exception e = expectThrows(
                IllegalStateException.class,
                () -> listeners.addOrNotify(unrefreshedOperation.getTranslogLocation(), listener)
            );
            assertEquals("can't wait for refresh on a closed index", e.getMessage());
            assertNull(listener.forcedRefresh.get());
            assertFalse(listeners.refreshNeeded());
            assertEquals(0, listeners.pendingCount());
        }
    }

    /**
     * Attempts to add a listener at the same time as a refresh occurs by having a background thread force a refresh as fast as it can while
     * adding listeners. This can catch the situation where a refresh happens right as the listener is being added such that the listener
     * misses the refresh and has to catch the next one. If the listener wasn't able to properly catch the next one then this would fail.
     */
    public void testConcurrentRefresh() throws Exception {
        AtomicBoolean run = new AtomicBoolean(true);
        Thread refresher = new Thread(() -> {
            while (run.get()) {
                engine.refresh("test");
            }
        });
        refresher.start();
        try {
            for (int i = 0; i < 100; i++) {
                Engine.IndexResult index = index("1");
                DummyRefreshListener listener = new DummyRefreshListener();
                boolean immediate = listeners.addOrNotify(index.getTranslogLocation(), listener);
                if (immediate) {
                    assertNotNull(listener.forcedRefresh.get());
                } else {
                    assertBusy(() -> assertNotNull(listener.forcedRefresh.get()), 1, TimeUnit.MINUTES);
                }
                assertFalse(listener.forcedRefresh.get());
                listener.assertNoError();
            }
        } finally {
            run.set(false);
            refresher.join();
        }
    }

    /**
     * Uses a bunch of threads to index, wait for refresh, and non-realtime get documents to validate that they are visible after waiting
     * regardless of what crazy sequence of events causes the refresh listener to fire.
     */
    public void testLotsOfThreads() throws Exception {
        int threadCount = between(3, 10);
        maxListeners = between(1, threadCount * 2);

        // This thread just refreshes every once in a while to cause trouble.
        Cancellable refresher = threadPool.scheduleWithFixedDelay(() -> engine.refresh("because test"), timeValueMillis(100), Names.SAME);

        // These threads add and block until the refresh makes the change visible and then do a non-realtime get.
        Thread[] indexers = new Thread[threadCount];
        for (int thread = 0; thread < threadCount; thread++) {
            final String threadId = String.format(Locale.ROOT, "%04d", thread);
            indexers[thread] = new Thread(() -> {
                for (int iteration = 1; iteration <= 50; iteration++) {
                    try {
                        String testFieldValue = String.format(Locale.ROOT, "%s%04d", threadId, iteration);
                        Engine.IndexResult index = index(threadId, testFieldValue);
                        assertEquals(iteration, index.getVersion());

                        DummyRefreshListener listener = new DummyRefreshListener();
                        listeners.addOrNotify(index.getTranslogLocation(), listener);
                        assertBusy(() -> assertNotNull("listener never called", listener.forcedRefresh.get()), 1, TimeUnit.MINUTES);
                        if (threadCount < maxListeners) {
                            assertFalse(listener.forcedRefresh.get());
                        }
                        listener.assertNoError();

                        Engine.Get get = new Engine.Get(false, false, threadId, new Term(IdFieldMapper.NAME, threadId));
                        try (Engine.GetResult getResult = engine.get(get, engine::acquireSearcher)) {
                            assertTrue("document not found", getResult.exists());
                            assertEquals(iteration, getResult.version());
                            StoredFields storedFields = getResult.docIdAndVersion().reader.storedFields();
                            org.apache.lucene.document.Document document = storedFields.document(getResult.docIdAndVersion().docId);
                            assertThat(document.getValues("test"), arrayContaining(testFieldValue));
                        }
                    } catch (Exception t) {
                        throw new RuntimeException("failure on the [" + iteration + "] iteration of thread [" + threadId + "]", t);
                    }
                }
            });
            indexers[thread].start();
        }

        for (Thread indexer : indexers) {
            indexer.join();
        }
        refresher.cancel();
    }

    public void testDisallowAddListeners() throws Exception {
        assertEquals(0, listeners.pendingCount());
        DummyRefreshListener listener = new DummyRefreshListener();
        assertFalse(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
        engine.refresh("I said so");
        assertFalse(listener.forcedRefresh.get());
        listener.assertNoError();

        try (Releasable releaseable1 = listeners.forceRefreshes()) {
            listener = new DummyRefreshListener();
            assertTrue(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
            assertTrue(listener.forcedRefresh.get());
            listener.assertNoError();
            assertEquals(0, listeners.pendingCount());

            try (Releasable releaseable2 = listeners.forceRefreshes()) {
                listener = new DummyRefreshListener();
                assertTrue(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
                assertTrue(listener.forcedRefresh.get());
                listener.assertNoError();
                assertEquals(0, listeners.pendingCount());
            }

            listener = new DummyRefreshListener();
            assertTrue(listeners.addOrNotify(index("1").getTranslogLocation(), listener));
            assertTrue(listener.forcedRefresh.get());
            listener.assertNoError();
            assertEquals(0, listeners.pendingCount());
        }

        assertFalse(listeners.addOrNotify(index("1").getTranslogLocation(), new DummyRefreshListener()));
        assertEquals(1, listeners.pendingCount());
    }

    private Engine.IndexResult index(String id) throws IOException {
        return index(id, "test");
    }

    private Engine.IndexResult index(String id, String testFieldValue) throws IOException {
        Document document = new Document();
        document.add(new TextField("test", testFieldValue, Field.Store.YES));
        Field idField = new Field("_id", id, IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(idField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesReference source = new BytesArray(new byte[] { 1 });
        ParsedDocument doc = new ParsedDocument(
            versionField,
            seqID,
            id,
            null,
            Arrays.asList(document),
            source,
            MediaTypeRegistry.JSON,
            null
        );
        Engine.Index index = new Engine.Index(new Term("_id", doc.id()), engine.config().getPrimaryTermSupplier().getAsLong(), doc);
        return engine.index(index);
    }

    private static class DummyRefreshListener implements Consumer<Boolean> {
        /**
         * When the listener is called this captures it's only argument.
         */
        final AtomicReference<Boolean> forcedRefresh = new AtomicReference<>();
        private volatile Exception error;

        @Override
        public void accept(Boolean forcedRefresh) {
            try {
                Boolean oldValue = this.forcedRefresh.getAndSet(forcedRefresh);
                assertNull("Listener called twice", oldValue);
            } catch (Exception e) {
                error = e;
            }
        }

        public void assertNoError() {
            if (error != null) {
                throw new RuntimeException(error);
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.chaos;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterUtil;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.RefreshFailedEngineException;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.common.util.FeatureFlags.CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class InternalEngineOnDiskWriterFullTests extends EngineTestCase {

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testAddDocumentOnDiskFull() throws IOException {
        final Path storeDirPath = createTempDir();
        final Path translogPath = createTempDir();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), newFSDirectory(storeDirPath));
        IndexWriterFactory indexWriterFactory = (directory, iwc) -> {
            MergeScheduler ms = iwc.getMergeScheduler();
            IndexWriterUtil.suppressMergePolicyException(ms);
            return new IndexWriter(directory, iwc);
        };
        boolean hitException = false;
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );

        int flushedDocCount = 0;

        try (
            Store store = createStore(dir);
            InternalEngine engine = createEngine(
                indexSettings,
                store,
                translogPath,
                newMergePolicy(),
                indexWriterFactory,
                null,
                globalCheckpoint::get
            )
        ) {
            int numDocsFirstSegment = 300;
            long diskUsage = dir.sizeInBytes();
            // Start with 10 bytes more than we are currently using:
            long diskFree = diskUsage + TestUtil.nextInt(random(), 10, 20);
            dir.setTrackDiskUsage(true);
            dir.setMaxSizeInBytes(diskFree);
            try {
                for (int i = 0; i < numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), TENANT_SOURCE, null);
                    engine.index(indexForDoc(doc));
                    if (i % 5 == 0) {
                        engine.refresh("Testing");
                    }

                    if (i % 20 == 0) {
                        flushedDocCount = i + 1;
                        engine.flush();
                    }
                }
            } catch (Exception ex) {
                hitException = true;
            }

            assertTrue(hitException);
            ParsedDocument doc = testParsedDocument("-1", null, testDocument(), B_1, null);
            assertThrows(AlreadyClosedException.class, () -> engine.index(indexForDoc(doc)));
        }

        try (Store store = createStore(newFSDirectory(storeDirPath))) {
            try (
                InternalEngine engine = createEngine(
                    indexSettings,
                    store,
                    translogPath,
                    newMergePolicy(),
                    indexWriterFactory,
                    null,
                    globalCheckpoint::get
                )
            ) {
                engine.refresh("Testing");
                // Ensure that whenever Engine re initialises,correctly. All documents may not be present in case translog
                // does not gets persisted on node and translog remains in buffer during the crash.
                try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                    final TotalHitCountCollector collector = new TotalHitCountCollector();
                    searcher.search(new MatchAllDocsQuery(), collector);
                    assertThat(collector.getTotalHits(), greaterThanOrEqualTo(flushedDocCount));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        }
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testUpdateOrDeleteDocumentOnDiskFull() throws IOException {
        final Path storeDirPath = createTempDir();
        final Path translogPath = createTempDir();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), newFSDirectory(storeDirPath));
        IndexWriterFactory indexWriterFactory = (directory, iwc) -> {
            MergeScheduler ms = iwc.getMergeScheduler();
            IndexWriterUtil.suppressMergePolicyException(ms);
            return new IndexWriter(directory, iwc);
        };
        boolean hitException = false;
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );

        int flushedDocCount = 0;
        int delCountAtFlush = 0;

        try (
            Store store = createStore(dir);
            InternalEngine engine = createEngine(
                indexSettings,
                store,
                translogPath,
                newMergePolicy(),
                indexWriterFactory,
                null,
                globalCheckpoint::get
            )
        ) {
            int numDocsFirstSegment = 300;
            AtomicInteger delCount = new AtomicInteger(0);
            long diskUsage = dir.sizeInBytes();
            // Start with 10 bytes more than we are currently using:
            long diskFree = diskUsage + TestUtil.nextInt(random(), 10, 20);
            dir.setTrackDiskUsage(true);
            dir.setMaxSizeInBytes(diskFree);
            try {
                for (int i = 0; i < numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), TENANT_SOURCE, null);
                    Engine.Index operation = indexForDoc(doc);
                    engine.index(operation);
                    if (i % 2 == 0) {
                        engine.delete(new Engine.Delete(operation.id(), operation.uid(), operation.primaryTerm()));
                        delCount.incrementAndGet();
                    } else if (i % 3 == 0) {
                        engine.index(indexForDoc(doc));
                    }

                    if (i % 5 == 0) {
                        engine.refresh("Testing");
                    }

                    if (i % 20 == 0) {
                        flushedDocCount = i + 1;
                        delCountAtFlush = delCount.get();
                        engine.flush();
                    }
                }
            } catch (Exception ex) {
                hitException = true;
            }

            assertTrue(hitException);
            ParsedDocument doc = testParsedDocument("-1", null, testDocument(), B_1, null);
            assertThrows(AlreadyClosedException.class, () -> engine.index(indexForDoc(doc)));
        }

        try (Store store = createStore(newFSDirectory(storeDirPath))) {
            try (
                InternalEngine engine = createEngine(
                    indexSettings,
                    store,
                    translogPath,
                    newMergePolicy(),
                    indexWriterFactory,
                    null,
                    globalCheckpoint::get
                )
            ) {
                engine.refresh("Testing");
                // Ensure that whenever Engine re initialises,correctly. All documents may not be present in case translog
                // does not gets persisted on node and translog remains in buffer during the crash.
                try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                    final TotalHitCountCollector collector = new TotalHitCountCollector();
                    searcher.search(new MatchAllDocsQuery(), collector);
                    assertThat(collector.getTotalHits(), greaterThanOrEqualTo(flushedDocCount - delCountAtFlush));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        }
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testEngineRefreshOnDiskFull() throws IOException {
        final Path storeDirPath = createTempDir();
        final Path translogPath = createTempDir();
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), newFSDirectory(storeDirPath));
        IndexWriterFactory indexWriterFactory = (directory, iwc) -> {
            MergeScheduler ms = iwc.getMergeScheduler();
            IndexWriterUtil.suppressMergePolicyException(ms);
            return new IndexWriter(directory, iwc);
        };
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );

        int flushedDocCount = 0;
        try (
            Store store = createStore(dir);
            InternalEngine engine = createEngine(
                indexSettings,
                store,
                translogPath,
                newMergePolicy(),
                indexWriterFactory,
                null,
                globalCheckpoint::get
            )
        ) {
            int numDocsFirstSegment = randomIntBetween(50, 100);
            for (int i = 0; i < numDocsFirstSegment; i++) {
                String id = Integer.toString(i);
                ParsedDocument doc = testParsedDocument(id, null, testContextSpecificDocument(), TENANT_SOURCE, null);
                engine.index(indexForDoc(doc));
                if (i % 20 == 0) {
                    flushedDocCount = i + 1;
                    engine.flush();
                }
            }

            long diskUsage = dir.sizeInBytes();
            // Start with 100 bytes more than we are currently using:
            long diskFree = diskUsage + TestUtil.nextInt(random(), 10, 20);
            dir.setTrackDiskUsage(true);
            dir.setMaxSizeInBytes(diskFree);
            for (int i = numDocsFirstSegment; i < numDocsFirstSegment + 60; i++) {
                String id = Integer.toString(i);
                ParsedDocument doc = testParsedDocument(id, null, testContextSpecificDocument(), TENANT_SOURCE, null);
                engine.index(indexForDoc(doc));
            }

            expectThrows(RefreshFailedEngineException.class, () -> engine.refresh("testing"));
            ParsedDocument doc = testParsedDocument("-1", null, testDocument(), B_1, null);
            assertThrows(AlreadyClosedException.class, () -> engine.index(indexForDoc(doc)));
        }

        try (Store store = createStore(newFSDirectory(storeDirPath))) {
            try (
                InternalEngine engine = createEngine(
                    indexSettings,
                    store,
                    translogPath,
                    newMergePolicy(),
                    indexWriterFactory,
                    null,
                    globalCheckpoint::get
                )
            ) {
                engine.refresh("Testing");
                // Ensure that whenever Engine re initialises,correctly. All documents may not be present in case translog
                // does not gets persisted on node and translog remains in buffer during the crash.
                try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
                    final TotalHitCountCollector collector = new TotalHitCountCollector();
                    searcher.search(new MatchAllDocsQuery(), collector);
                    assertThat(collector.getTotalHits(), greaterThanOrEqualTo(flushedDocCount));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        }
    }
}

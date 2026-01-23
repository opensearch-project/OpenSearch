/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.chaos;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterUtil;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.InternalEngine;
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

public class InternalEngineOnOutOfFileDescriptorsTests extends EngineTestCase {

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testAddDocumentOnOutOfFileDescriptors() throws IOException {
        final Path storeDirPath = createTempDir();
        final Path translogPath = createTempDir();
        double rate = 0.0;
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), newFSDirectory(storeDirPath));
        dir.setRandomIOExceptionRateOnOpen(rate);
        IndexWriterFactory indexWriterFactory = (directory, iwc) -> {
            MergeScheduler ms = iwc.getMergeScheduler();
            IndexWriterUtil.suppressMergePolicyException(ms);
            return new IndexWriter(directory, iwc);
        };
        boolean hitException = false;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );

        AtomicInteger docCount = new AtomicInteger(0);
        int docCountAtFlush = 0;
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
            try {
                for (int i = 0; i < numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), TENANT_SOURCE, null);
                    engine.index(indexForDoc(doc));
                    docCount.incrementAndGet();

                    if (i % 20 == 0) {
                        docCountAtFlush = docCount.get();
                        engine.flush();
                    }
                }
            } catch (IOException ex) {
                hitException = true;
            }

            assertFalse(hitException);
            assertTrue(DirectoryReader.indexExists(dir));

            try {
                engine.refresh("testing");
            } catch (EngineException e) {
                hitException = true;
            }

            assertTrue(DirectoryReader.indexExists(dir));
            rate = 1.0;
            dir.setRandomIOExceptionRateOnOpen(rate);
            try {
                for (int i = numDocsFirstSegment; i < numDocsFirstSegment + numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), TENANT_SOURCE, null);
                    engine.index(indexForDoc(doc));
                }

                engine.refresh("testing");
            } catch (EngineException e) {
                hitException = true;
            }

            assertTrue(hitException);
            assertTrue(DirectoryReader.indexExists(dir));
            rate = 0.0;
            dir.setRandomIOExceptionRateOnOpen(rate);
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
                    assertThat(collector.getTotalHits(), greaterThanOrEqualTo(docCountAtFlush));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        }
    }

    @LockFeatureFlag(CONTEXT_AWARE_MIGRATION_EXPERIMENTAL_FLAG)
    public void testUpdateOrDeleteDocumentOnOutOfFileDescriptors() throws IOException {
        final Path storeDirPath = createTempDir();
        final Path translogPath = createTempDir();
        double rate = 0.0;
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), newFSDirectory(storeDirPath));
        dir.setRandomIOExceptionRateOnOpen(rate);
        IndexWriterFactory indexWriterFactory = (directory, iwc) -> {
            MergeScheduler ms = iwc.getMergeScheduler();
            IndexWriterUtil.suppressMergePolicyException(ms);
            return new IndexWriter(directory, iwc);
        };
        boolean hitException = false;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultSettings.getSettings())
                .put(IndexSettings.INDEX_CONTEXT_AWARE_ENABLED_SETTING.getKey(), true)
                .build()
        );

        AtomicInteger docCount = new AtomicInteger(0);
        AtomicInteger delCount = new AtomicInteger(0);
        int docCountAtFlush = 0;
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
            int numDocsFirstSegment = randomIntBetween(50, 100);
            try {
                for (int i = 0; i < numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), TENANT_SOURCE, null);
                    Engine.Index operation = indexForDoc(doc);
                    engine.index(operation);
                    docCount.incrementAndGet();

                    if (i % 2 == 0) {
                        engine.delete(new Engine.Delete(operation.id(), operation.uid(), operation.primaryTerm()));
                        delCount.incrementAndGet();
                    } else if (i % 3 == 0) {
                        engine.index(indexForDoc(doc));
                    }

                    if (i % 20 == 0) {
                        docCountAtFlush = docCount.get();
                        delCountAtFlush = delCount.get();
                        engine.flush();
                    }
                }
            } catch (IOException ex) {
                hitException = true;
            }

            assertFalse(hitException);
            assertTrue(DirectoryReader.indexExists(dir));

            try {
                engine.refresh("testing");
            } catch (EngineException e) {
                hitException = true;
            }

            assertTrue(DirectoryReader.indexExists(dir));
            rate = 1.0;
            dir.setRandomIOExceptionRateOnOpen(rate);
            try {
                for (int i = numDocsFirstSegment; i < numDocsFirstSegment + numDocsFirstSegment; i++) {
                    String id = Integer.toString(i);
                    ParsedDocument doc = testParsedDocument(id, null, testDocument(), TENANT_SOURCE, null);
                    engine.index(indexForDoc(doc));
                }

                engine.refresh("testing");
            } catch (EngineException e) {
                hitException = true;
            }

            assertTrue(hitException);
            assertTrue(DirectoryReader.indexExists(dir));
            rate = 0.0;
            dir.setRandomIOExceptionRateOnOpen(rate);
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
                    assertThat(collector.getTotalHits(), greaterThanOrEqualTo(docCountAtFlush - delCountAtFlush));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        }
    }
}

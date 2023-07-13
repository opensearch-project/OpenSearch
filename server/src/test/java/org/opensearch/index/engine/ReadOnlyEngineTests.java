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

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.opensearch.common.lucene.index.OpenSearchDirectoryReader.getOpenSearchDirectoryReader;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class ReadOnlyEngineTests extends EngineTestCase {

    public void testReadOnlyEngine() throws Exception {
        IOUtils.close(engine, store);
        Engine readOnlyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            final SeqNoStats lastSeqNoStats;
            final List<DocIdSeqNoAndSource> lastDocIds;
            try (InternalEngine engine = createEngine(config)) {
                Engine.Get get = null;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if (get == null || rarely()) {
                        get = newGet(randomBoolean(), doc);
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                }
                engine.translogManager().syncTranslog();
                globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                engine.flush();
                readOnlyEngine = new ReadOnlyEngine(
                    engine.engineConfig,
                    engine.getSeqNoStats(globalCheckpoint.get()),
                    engine.translogManager().getTranslogStats(),
                    false,
                    Function.identity(),
                    true
                );
                lastSeqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                lastDocIds = getDocIds(engine, true);
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getProcessedLocalCheckpoint(), equalTo(readOnlyEngine.getPersistedLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                for (int i = 0; i < numDocs; i++) {
                    if (randomBoolean()) {
                        String delId = Integer.toString(i);
                        engine.delete(new Engine.Delete(delId, newUid(delId), primaryTerm.get()));
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                }
                Engine.Searcher external = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL);
                Engine.Searcher internal = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                assertSame(external.getIndexReader(), internal.getIndexReader());
                assertThat(external.getIndexReader(), instanceOf(DirectoryReader.class));
                DirectoryReader dirReader = external.getDirectoryReader();
                OpenSearchDirectoryReader esReader = getOpenSearchDirectoryReader(dirReader);
                IndexReader.CacheHelper helper = esReader.getReaderCacheHelper();
                assertNotNull(helper);
                assertEquals(helper.getKey(), dirReader.getReaderCacheHelper().getKey());

                IOUtils.close(external, internal);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getProcessedLocalCheckpoint(), equalTo(readOnlyEngine.getPersistedLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                try (Engine.GetResult getResult = readOnlyEngine.get(get, readOnlyEngine::acquireSearcher)) {
                    assertTrue(getResult.exists());
                }
            }
            // Close and reopen the main engine
            try (InternalEngine recoveringEngine = new InternalEngine(config)) {
                TranslogHandler translogHandler = createTranslogHandler(config.getIndexSettings(), recoveringEngine);
                recoveringEngine.translogManager()
                    .recoverFromTranslog(translogHandler, recoveringEngine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getProcessedLocalCheckpoint(), equalTo(readOnlyEngine.getPersistedLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
            }
        } finally {
            IOUtils.close(readOnlyEngine);
        }
    }

    public void testEnsureMaxSeqNoIsEqualToGlobalCheckpoint() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final int numDocs = scaledRandomIntBetween(10, 100);
            try (InternalEngine engine = createEngine(config)) {
                long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    maxSeqNo = engine.getProcessedLocalCheckpoint();
                }
                engine.translogManager().syncTranslog();
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint() - 1);
                engine.flushAndClose();

                IllegalStateException exception = expectThrows(
                    IllegalStateException.class,
                    () -> new ReadOnlyEngine(config, null, null, true, Function.identity(), true) {
                        @Override
                        protected boolean assertMaxSeqNoEqualsToGlobalCheckpoint(final long maxSeqNo, final long globalCheckpoint) {
                            // we don't want the assertion to trip in this test
                            return true;
                        }
                    }
                );
                assertThat(
                    exception.getMessage(),
                    equalTo(
                        "Maximum sequence number ["
                            + maxSeqNo
                            + "] from last commit does not match global checkpoint ["
                            + globalCheckpoint.get()
                            + "]"
                    )
                );
            }
        }
    }

    public void testReadOnly() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty(Version.CURRENT.luceneVersion);
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, new TranslogStats(), true, Function.identity(), true)) {
                Class<? extends Throwable> expectedException = LuceneTestCase.TEST_ASSERTS_ENABLED
                    ? AssertionError.class
                    : UnsupportedOperationException.class;
                expectThrows(expectedException, () -> readOnlyEngine.index(null));
                expectThrows(expectedException, () -> readOnlyEngine.delete(null));
                expectThrows(expectedException, () -> readOnlyEngine.noOp(null));
            }
        }
    }

    public void testReadOldIndices() throws Exception {
        IOUtils.close(engine, store);
        // The index has one document in it, so the checkpoint cannot be NO_OPS_PERFORMED
        final AtomicLong globalCheckpoint = new AtomicLong(0);
        final String pathToTestIndex = "/indices/bwc/es-6.3.0/testIndex-es-6.3.0.zip";
        Path tmp = createTempDir();
        TestUtil.unzip(getClass().getResourceAsStream(pathToTestIndex), tmp);
        FeatureFlagSetter.set(FeatureFlags.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY);
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                .build()
        );
        try (Store store = createStore(newFSDirectory(tmp))) {
            EngineConfig config = config(indexSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, new TranslogStats(), true, Function.identity(), true)) {
                assertVisibleCount(readOnlyEngine, 1, false);
            }
        }
    }

    public void testReadOldIndicesFailure() throws IOException {
        IOUtils.close(engine, store);
        // The index has one document in it, so the checkpoint cannot be NO_OPS_PERFORMED
        final AtomicLong globalCheckpoint = new AtomicLong(0);
        final String pathToTestIndex = "/indices/bwc/es-6.3.0/testIndex-es-6.3.0.zip";
        Path tmp = createTempDir();
        TestUtil.unzip(getClass().getResourceAsStream(pathToTestIndex), tmp);
        try (Store store = createStore(newFSDirectory(tmp))) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            try {
                new ReadOnlyEngine(config, null, new TranslogStats(), true, Function.identity(), true);
            } catch (UncheckedIOException e) {
                assertEquals(IndexFormatTooOldException.class, e.getCause().getClass());
            }
        }
    }

    /**
     * Test that {@link ReadOnlyEngine#verifyEngineBeforeIndexClosing()} never fails
     * whatever the value of the global checkpoint to check is.
     */
    public void testVerifyShardBeforeIndexClosingIsNoOp() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty(Version.CURRENT.luceneVersion);
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, new TranslogStats(), true, Function.identity(), true)) {
                globalCheckpoint.set(randomNonNegativeLong());
                try {
                    readOnlyEngine.verifyEngineBeforeIndexClosing();
                } catch (final IllegalStateException e) {
                    fail("Read-only engine pre-closing verifications failed");
                }
            }
        }
    }

    public void testRecoverFromTranslogAppliesNoOperations() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(i);
                }
                engine.translogManager().syncTranslog();
                engine.flushAndClose();
            }
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity(), true)) {
                final TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings(), readOnlyEngine);
                readOnlyEngine.translogManager()
                    .recoverFromTranslog(translogHandler, readOnlyEngine.getProcessedLocalCheckpoint(), randomNonNegativeLong());

                assertThat(translogHandler.appliedOperations(), equalTo(0L));
            }
        }
    }

    public void testTranslogStats() throws IOException {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final boolean softDeletesEnabled = config.getIndexSettings().isSoftDeleteEnabled();
            final int numDocs = frequently() ? scaledRandomIntBetween(10, 200) : 0;
            int uncommittedDocs = 0;

            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    globalCheckpoint.set(i);
                    if (rarely()) {
                        engine.flush();
                        uncommittedDocs = 0;
                    } else {
                        uncommittedDocs += 1;
                    }
                }

                assertThat(
                    engine.translogManager().getTranslogStats().estimatedNumberOfOperations(),
                    equalTo(softDeletesEnabled ? uncommittedDocs : numDocs)
                );
                assertThat(engine.translogManager().getTranslogStats().getUncommittedOperations(), equalTo(uncommittedDocs));
                assertThat(engine.translogManager().getTranslogStats().getTranslogSizeInBytes(), greaterThan(0L));
                assertThat(engine.translogManager().getTranslogStats().getUncommittedSizeInBytes(), greaterThan(0L));
                assertThat(engine.translogManager().getTranslogStats().getEarliestLastModifiedAge(), greaterThan(0L));

                engine.flush(true, true);
            }

            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity(), true)) {
                assertThat(
                    readOnlyEngine.translogManager().getTranslogStats().estimatedNumberOfOperations(),
                    equalTo(softDeletesEnabled ? 0 : numDocs)
                );
                assertThat(readOnlyEngine.translogManager().getTranslogStats().getUncommittedOperations(), equalTo(0));
                assertThat(readOnlyEngine.translogManager().getTranslogStats().getTranslogSizeInBytes(), greaterThan(0L));
                assertThat(readOnlyEngine.translogManager().getTranslogStats().getUncommittedSizeInBytes(), greaterThan(0L));
                assertThat(readOnlyEngine.translogManager().getTranslogStats().getEarliestLastModifiedAge(), greaterThan(0L));
            }
        }
    }
}

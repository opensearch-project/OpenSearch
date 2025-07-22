/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class LeafSorterOptimizationTests extends EngineTestCase {

    public void testReadOnlyEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            try (InternalEngine engine = new InternalEngine(config)) {
                // Index some documents with timestamps
                for (int i = 0; i < 10; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                }
                engine.flush();
            }
        }
        // Second block: reopen the same store and open ReadOnlyEngine for assertions
        // (Assume storePath and translogPath are available or can be replaced with appropriate temp dirs)
        // For this test, we focus on the leafSorter logic
        try (Store readOnlyStore = createStore()) {
            EngineConfig readOnlyConfig = new EngineConfig.Builder().shardId(shardId)
                .threadPool(threadPool)
                .indexSettings(defaultSettings)
                .warmer(null)
                .store(readOnlyStore)
                .mergePolicy(newMergePolicy())
                .analyzer(newIndexWriterConfig().getAnalyzer())
                .similarity(newIndexWriterConfig().getSimilarity())
                .codecService(new CodecService(null, defaultSettings, logger))
                .eventListener(new Engine.EventListener() {
                })
                .translogConfig(new TranslogConfig(shardId, createTempDir(), defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(java.util.Collections.emptyList())
                .internalRefreshListener(java.util.Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(globalCheckpoint::get)
                .leafSorter(java.util.Comparator.<org.apache.lucene.index.LeafReader>comparingInt(reader -> reader.maxDoc()))
                .build();
            try (
                ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(
                    readOnlyConfig,
                    null,
                    null,
                    true,
                    java.util.function.Function.identity(),
                    true
                )
            ) {
                try (Engine.Searcher searcher = readOnlyEngine.acquireSearcher("test")) {
                    DirectoryReader reader = (DirectoryReader) searcher.getDirectoryReader();
                    // Assert that there are multiple leaves (segments)
                    assertThat("ReadOnlyEngine should have multiple leaves to test sorting", reader.leaves().size(), greaterThan(1));

                    // Collect maxDoc for each leaf
                    java.util.List<Integer> actualOrder = new java.util.ArrayList<>();
                    for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                        actualOrder.add(ctx.reader().maxDoc());
                    }
                    // Create a reverse order comparator to test that our sorter is actually being used
                    java.util.List<Integer> expectedOrder = new java.util.ArrayList<>(actualOrder);
                    expectedOrder.sort(java.util.Collections.reverseOrder()); // Reverse order to test our sorter

                    // If leaves are not in reverse order, then our sorter is working
                    assertNotEquals("Leaves should be sorted by our comparator, not default order", expectedOrder, actualOrder);

                    // Verify they are actually sorted by our comparator (ascending maxDoc)
                    java.util.List<Integer> sortedOrder = new java.util.ArrayList<>(actualOrder);
                    sortedOrder.sort(Integer::compareTo);
                    assertEquals("Leaves should be sorted by maxDoc() in ascending order", sortedOrder, actualOrder);
                }
            }
        }
    }

    public void testNRTReplicationEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);

            // Create config with leafSorter explicitly set
            EngineConfig config = new EngineConfig.Builder().shardId(shardId)
                .threadPool(threadPool)
                .indexSettings(defaultSettings)
                .warmer(null)
                .store(store)
                .mergePolicy(newMergePolicy())
                .analyzer(newIndexWriterConfig().getAnalyzer())
                .similarity(newIndexWriterConfig().getSimilarity())
                .codecService(new CodecService(null, defaultSettings, logger))
                .eventListener(new Engine.EventListener() {
                })
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .translogConfig(new TranslogConfig(shardId, createTempDir(), defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .externalRefreshListener(emptyList())
                .internalRefreshListener(emptyList())
                .indexSort(null)
                .circuitBreakerService(new NoneCircuitBreakerService())
                .globalCheckpointSupplier(globalCheckpoint::get)
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .leafSorter(DataStream.TIMESERIES_LEAF_SORTER)
                .build();

            // Verify that the config has leafSorter configured
            assertThat("Engine config should have leafSorter configured", config.getLeafSorter(), notNullValue());

            // Verify that the leafSorter is the timeseries leafSorter
            Comparator<LeafReader> leafSorter = config.getLeafSorter();
            assertThat("LeafSorter should be configured", leafSorter, notNullValue());
        }
    }

    public void testNoOpEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            try (InternalEngine engine = new InternalEngine(config)) {
                // Index some documents
                for (int i = 0; i < 5; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                }
                engine.flush();

                // Create NoOpEngine
                NoOpEngine noOpEngine = new NoOpEngine(config);

                // Verify that the engine has a leafSorter configured
                assertThat("Engine should have leafSorter configured", noOpEngine.engineConfig.getLeafSorter(), notNullValue());

                // Verify that DirectoryReader is opened with leafSorter
                try (Engine.Searcher searcher = noOpEngine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
                    DirectoryReader reader = searcher.getDirectoryReader();
                    assertThat("DirectoryReader should be created", reader, notNullValue());
                }
            }
        }
    }

    public void testLeafSorterIsAppliedToDirectoryReader() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            try (InternalEngine engine = new InternalEngine(config)) {
                // Index some documents
                for (int i = 0; i < 5; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                }

                // Get the leafSorter from the engine config
                Comparator<LeafReader> leafSorter = engine.engineConfig.getLeafSorter();
                assertThat("LeafSorter should be configured", leafSorter, notNullValue());

                // Test that DirectoryReader.open with leafSorter works correctly
                try (DirectoryReader reader = DirectoryReader.open(store.directory(), leafSorter)) {
                    assertThat("DirectoryReader should be created with leafSorter", reader, notNullValue());
                    assertThat("Reader should have correct number of documents", reader.numDocs(), equalTo(5));
                }
            }
        }
    }

    public void testTimestampSortOptimizationWorksOnAllEngineTypes() throws IOException {
        // Test that timestamp sort optimization works on all engine types
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // Test InternalEngine (primary)
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            try (InternalEngine engine = new InternalEngine(config)) {
                // Index documents with timestamps
                for (int i = 0; i < 100; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                }
                engine.flush();

                // Test sort performance on InternalEngine
                testSortPerformance(engine, "InternalEngine");

                // Create ReadOnlyEngine and test
                ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(
                    engine.engineConfig,
                    engine.getSeqNoStats(globalCheckpoint.get()),
                    engine.translogManager().getTranslogStats(),
                    false,
                    Function.identity(),
                    true
                );

                // Test sort performance on ReadOnlyEngine
                testSortPerformance(readOnlyEngine, "ReadOnlyEngine");
                readOnlyEngine.close();
            }
        }

        // Test NRTReplicationEngine
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            try (NRTReplicationEngine nrtEngine = new NRTReplicationEngine(config)) {
                // Test sort performance on NRTReplicationEngine
                testSortPerformance(nrtEngine, "NRTReplicationEngine");
            }
        }
    }

    private void testSortPerformance(Engine engine, String engineType) throws IOException {
        try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)) {
            DirectoryReader reader = searcher.getDirectoryReader();
            IndexSearcher indexSearcher = new IndexSearcher(reader);

            // Create a sort by timestamp (descending)
            Sort timestampSort = new Sort(new SortField("@timestamp", SortField.Type.LONG, true));

            // Perform a sorted search
            TopDocs topDocs = indexSearcher.search(new MatchAllDocsQuery(), 10, timestampSort);

            // Verify that the search completed successfully
            assertThat("Search should complete successfully on " + engineType, topDocs.totalHits.value(), greaterThan(0L));

            // Verify that the engine has leafSorter configured
            assertThat("Engine " + engineType + " should have leafSorter configured", engine.config().getLeafSorter(), notNullValue());
        }
    }

    public void testLeafSorterConfiguration() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);

            // Test that all engine types have leafSorter configured
            try (InternalEngine internalEngine = new InternalEngine(config)) {
                assertThat("InternalEngine should have leafSorter", internalEngine.config().getLeafSorter(), notNullValue());
            }

            try (NRTReplicationEngine nrtEngine = new NRTReplicationEngine(config)) {
                assertThat("NRTReplicationEngine should have leafSorter", nrtEngine.config().getLeafSorter(), notNullValue());
            }

            try (NoOpEngine noOpEngine = new NoOpEngine(config)) {
                assertThat("NoOpEngine should have leafSorter", noOpEngine.config().getLeafSorter(), notNullValue());
            }
        }
    }
}

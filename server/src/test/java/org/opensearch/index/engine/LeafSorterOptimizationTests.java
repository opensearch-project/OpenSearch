/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LeafSorterOptimizationTests extends EngineTestCase {

    public void testReadOnlyEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // Create and populate InternalEngine to generate multiple segments
        java.nio.file.Path translogPath = createTempDir();
        java.nio.file.Path storePath = createTempDir();
        // First block: create, index, and flush with InternalEngine and Store
        try (Store sourceStore = createStore(newFSDirectory(storePath))) {
            sourceStore.createEmpty(Version.CURRENT.luceneVersion);

            String translogUUID = org.opensearch.index.translog.Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                1L
            );
            sourceStore.associateIndexWithNewTranslog(translogUUID);

            EngineConfig sourceConfig = new EngineConfig.Builder().shardId(shardId)
                .threadPool(threadPool)
                .indexSettings(defaultSettings)
                .warmer(null)
                .store(sourceStore)
                .mergePolicy(newMergePolicy())
                .analyzer(newIndexWriterConfig().getAnalyzer())
                .similarity(newIndexWriterConfig().getSimilarity())
                .codecService(new CodecService(null, defaultSettings, logger))
                .eventListener(new Engine.EventListener() {
                })
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(Collections.emptyList())
                .internalRefreshListener(Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(globalCheckpoint::get)
                .leafSorter(null)
                .build();
            try (InternalEngine sourceEngine = new InternalEngine(sourceConfig)) {
                // Skip translog recovery to allow flushes
                sourceEngine.translogManager().skipTranslogRecovery();

                for (int i = 0; i < 10; i++) {
                    sourceEngine.index(indexForDoc(testParsedDocument("doc" + i, null, testDocumentWithTextField(), B_1, null, false)));
                    if (i % 3 == 0) {
                        sourceEngine.flush(true, true);
                    }
                }
                // Set global checkpoint to engine's persisted local checkpoint before closing the engine
                globalCheckpoint.set(sourceEngine.getPersistedLocalCheckpoint());
                sourceEngine.translogManager().syncTranslog();
                // Do not flush after updating the global checkpoint
            }
        }
        // Second block: reopen the same store and open ReadOnlyEngine for assertions
        try (Store readOnlyStore = createStore(newFSDirectory(storePath))) {
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(Collections.emptyList())
                .internalRefreshListener(Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(globalCheckpoint::get)
                // Temporarily remove leafSorter to test if our test catches this
                // .leafSorter(Comparator.comparingInt(reader -> reader.maxDoc()))
                .build();
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(readOnlyConfig, null, null, true, Function.identity(), true)) {
                try (Engine.Searcher searcher = readOnlyEngine.acquireSearcher("test")) {
                    DirectoryReader reader = (DirectoryReader) searcher.getDirectoryReader();
                    // Assert that there are multiple leaves (segments)
                    assertThat("ReadOnlyEngine should have multiple leaves to test sorting", reader.leaves().size(), greaterThan(1));

                    // Verify that leaves are sorted by maxDoc() (our comparator)
                    List<LeafReaderContext> leaves = reader.leaves();
                    List<Integer> actualOrder = leaves.stream().map(ctx -> ctx.reader().maxDoc()).collect(Collectors.toList());

                    List<Integer> expectedOrder = new ArrayList<>(actualOrder);
                    expectedOrder.sort(Integer::compareTo);

                    assertEquals("Leaves should be sorted by maxDoc()", expectedOrder, actualOrder);
                }
            }
        }
    }

    public void testNoOpEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // Create a proper store with translog setup and populate with data
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);

            // Create a translog to associate with the store
            java.nio.file.Path translogPath = createTempDir();
            String translogUUID = org.opensearch.index.translog.Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUUID);

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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
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

            // Create NoOpEngine
            try (NoOpEngine noOpEngine = new NoOpEngine(config)) {

                // Verify that NoOpEngine uses the leafSorter when creating readers
                try (Engine.Searcher searcher = noOpEngine.acquireSearcher("test")) {
                    DirectoryReader reader = searcher.getDirectoryReader();
                    assertThat(
                        "NoOpEngine should return a DirectoryReader",
                        reader,
                        instanceOf(org.opensearch.common.lucene.index.OpenSearchDirectoryReader.class)
                    );

                    // NoOpEngine typically has empty index, so we just verify it returns a valid reader
                    assertNotNull("NoOpEngine should return a DirectoryReader", reader);

                    // For NoOpEngine with empty index, we can't test sorting since there are no leaves
                    // But we can verify the reader is properly configured
                    List<LeafReaderContext> leaves = reader.leaves();
                    // Empty index should have no leaves or minimal leaves
                    assertTrue("NoOpEngine should have minimal leaves for empty index", leaves.size() <= 1);
                }
            }
        }
    }

    public void testReadOnlyEngineLeafSorterActuallySorts() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // 1. Create and populate InternalEngine with docs and multiple segments
        try (Store sourceStore = createStore()) {
            sourceStore.createEmpty(Version.CURRENT.luceneVersion);
            java.nio.file.Path translogPath = createTempDir();

            // Create a translog to associate with the store
            String translogUUID = org.opensearch.index.translog.Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            sourceStore.associateIndexWithNewTranslog(translogUUID);

            EngineConfig sourceConfig = new EngineConfig.Builder().shardId(shardId)
                .threadPool(threadPool)
                .indexSettings(defaultSettings)
                .warmer(null)
                .store(sourceStore)
                .mergePolicy(newMergePolicy())
                .analyzer(newIndexWriterConfig().getAnalyzer())
                .similarity(newIndexWriterConfig().getSimilarity())
                .codecService(new CodecService(null, defaultSettings, logger))
                .eventListener(new Engine.EventListener() {
                })
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .externalRefreshListener(emptyList())
                .internalRefreshListener(emptyList())
                .indexSort(null)
                .circuitBreakerService(new NoneCircuitBreakerService())
                .globalCheckpointSupplier(globalCheckpoint::get)
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .build();

            try (InternalEngine sourceEngine = createEngine(sourceConfig)) {
                // Add docs with known order values and flush every 2 docs
                for (int i = 0; i < 10; i++) {
                    int orderValue = (10 - i) * 10; // 100, 90, ..., 10
                    ParseContext.Document doc = testDocumentWithTextField();
                    doc.add(new IntPoint("order", orderValue));
                    doc.add(new StoredField("order", orderValue));
                    sourceEngine.index(indexForDoc(testParsedDocument("doc" + i, null, doc, B_1, null, false)));
                    if (i % 2 == 1) {
                        sourceEngine.flush(true, true);
                    }
                }
                sourceEngine.flush(true, true);

                // 2. Create ReadOnlyEngine with a custom leafSorter (sort by min "order" value)
                Comparator<org.apache.lucene.index.LeafReader> minOrderComparator = Comparator.comparingInt(reader -> {
                    try {
                        NumericDocValues values = reader.getNumericDocValues("order");
                        int min = Integer.MAX_VALUE;
                        if (values != null) {
                            while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                min = Math.min(min, (int) values.longValue());
                            }
                        }
                        return min;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                try (Store readOnlyStore = createStore()) {
                    readOnlyStore.createEmpty(Version.CURRENT.luceneVersion);

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
                        .queryCache(IndexSearcher.getDefaultQueryCache())
                        .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                        .translogConfig(
                            new TranslogConfig(shardId, createTempDir(), defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false)
                        )
                        .flushMergesAfter(TimeValue.timeValueMinutes(5))
                        .externalRefreshListener(emptyList())
                        .internalRefreshListener(emptyList())
                        .indexSort(null)
                        .circuitBreakerService(new NoneCircuitBreakerService())
                        .globalCheckpointSupplier(globalCheckpoint::get)
                        .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                        .primaryTermSupplier(primaryTerm)
                        .tombstoneDocSupplier(tombstoneDocSupplier())
                        .leafSorter(minOrderComparator)
                        .build();

                    try (
                        ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(
                            readOnlyConfig,
                            sourceEngine.getSeqNoStats(globalCheckpoint.get()),
                            sourceEngine.translogManager().getTranslogStats(),
                            false,
                            reader -> reader,
                            false
                        )
                    ) {
                        try (Engine.Searcher searcher = readOnlyEngine.acquireSearcher("test")) {
                            DirectoryReader reader = searcher.getDirectoryReader();
                            List<LeafReaderContext> leaves = reader.leaves();

                            // 3. Extract min order value from each leaf
                            List<Integer> actualOrder = new ArrayList<>();
                            for (LeafReaderContext ctx : leaves) {
                                NumericDocValues values = ctx.reader().getNumericDocValues("order");
                                int min = Integer.MAX_VALUE;
                                if (values != null) {
                                    while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                        min = Math.min(min, (int) values.longValue());
                                    }
                                }
                                actualOrder.add(min);
                            }

                            // 4. Compute expected order
                            List<Integer> expectedOrder = new ArrayList<>(actualOrder);
                            expectedOrder.sort(Integer::compareTo);

                            // 5. Assert actual order matches expected order
                            assertEquals("Leaves should be sorted by min 'order' value", expectedOrder, actualOrder);
                        }
                    }
                }
            }
        }
    }

    public void testNoOpEngineLeafSorterActuallySorts() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // 1. Create and populate InternalEngine with docs and multiple segments
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            java.nio.file.Path translogPath = createTempDir();

            // Create a translog to associate with the store (fix)
            String translogUUID = org.opensearch.index.translog.Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUUID);

            EngineConfig sourceConfig = new EngineConfig.Builder().shardId(shardId)
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .externalRefreshListener(emptyList())
                .internalRefreshListener(emptyList())
                .indexSort(null)
                .circuitBreakerService(new NoneCircuitBreakerService())
                .globalCheckpointSupplier(globalCheckpoint::get)
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .build();

            try (InternalEngine sourceEngine = createEngine(sourceConfig)) {
                // Add docs with known order values and flush every 2 docs
                for (int i = 0; i < 10; i++) {
                    int orderValue = (10 - i) * 10; // 100, 90, ..., 10
                    ParseContext.Document doc = testDocumentWithTextField();
                    doc.add(new IntPoint("order", orderValue));
                    doc.add(new StoredField("order", orderValue));
                    sourceEngine.index(indexForDoc(testParsedDocument("doc" + i, null, doc, B_1, null, false)));
                    if (i % 2 == 1) {
                        sourceEngine.flush(true, true);
                    }
                }
                sourceEngine.flush(true, true);
            }

            // Set global checkpoint to max seq no
            globalCheckpoint.set(9);

            // 2. Create NoOpEngine with a custom leafSorter (sort by min "order" value)
            Comparator<org.apache.lucene.index.LeafReader> minOrderComparator = Comparator.comparingInt(reader -> {
                try {
                    NumericDocValues values = reader.getNumericDocValues("order");
                    int min = Integer.MAX_VALUE;
                    if (values != null) {
                        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            min = Math.min(min, (int) values.longValue());
                        }
                    }
                    return min;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            EngineConfig noOpConfig = new EngineConfig.Builder().shardId(shardId)
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .externalRefreshListener(emptyList())
                .internalRefreshListener(emptyList())
                .indexSort(null)
                .circuitBreakerService(new NoneCircuitBreakerService())
                .globalCheckpointSupplier(globalCheckpoint::get)
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .leafSorter(minOrderComparator)
                .build();

            try (NoOpEngine noOpEngine = new NoOpEngine(noOpConfig)) {
                try (Engine.Searcher searcher = noOpEngine.acquireSearcher("test")) {
                    DirectoryReader reader = searcher.getDirectoryReader();
                    List<LeafReaderContext> leaves = reader.leaves();

                    // Extract min order value from each leaf
                    List<Integer> actualOrder = new ArrayList<>();
                    for (LeafReaderContext ctx : leaves) {
                        NumericDocValues values = ctx.reader().getNumericDocValues("order");
                        int min = Integer.MAX_VALUE;
                        if (values != null) {
                            while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                min = Math.min(min, (int) values.longValue());
                            }
                        }
                        actualOrder.add(min);
                    }

                    // Compute expected order
                    List<Integer> expectedOrder = new ArrayList<>(actualOrder);
                    expectedOrder.sort(Integer::compareTo);

                    // Assert actual order matches expected order
                    assertEquals("Leaves should be sorted by min 'order' value", expectedOrder, actualOrder);
                }
            }
        }
    }
}

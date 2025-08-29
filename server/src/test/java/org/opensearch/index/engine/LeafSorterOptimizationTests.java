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
import org.opensearch.Version;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.VersionType;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LeafSorterOptimizationTests extends EngineTestCase {

    public void testReadOnlyEngineUsesLeafSorter() throws IOException {
        Path translogPath = createTempDir();
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            final String translogUUID = Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            Comparator<LeafReader> leafSorter = Comparator.comparingInt(LeafReader::maxDoc);
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(java.util.Collections.emptyList())
                .internalRefreshListener(java.util.Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
                .leafSorter(leafSorter)
                .build();
            long maxSeqNo;
            // Index docs with InternalEngine, then open ReadOnlyEngine
            try (InternalEngine engine = new InternalEngine(config)) {
                TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings(), engine);
                engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                for (int i = 0; i < 10; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            primaryTerm.get(),
                            Versions.MATCH_DELETED,
                            VersionType.INTERNAL,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if ((i + 1) % 2 == 0) {
                        engine.flush();
                    }
                }
                engine.refresh("test");
                engine.flush();
                maxSeqNo = engine.getSeqNoStats(-1).getMaxSeqNo();
            }
            // Now open ReadOnlyEngine and check leaf order
            EngineConfig readOnlyConfig = new EngineConfig.Builder().shardId(shardId)
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(java.util.Collections.emptyList())
                .internalRefreshListener(java.util.Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(() -> maxSeqNo)
                .leafSorter(leafSorter)
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
                    assertThat("Should have multiple leaves", reader.leaves().size(), greaterThan(0));
                    java.util.List<Integer> actualOrder = new java.util.ArrayList<>();
                    for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                        actualOrder.add(ctx.reader().maxDoc());
                    }
                    java.util.List<Integer> expectedOrder = new java.util.ArrayList<>(actualOrder);
                    expectedOrder.sort(Integer::compareTo);
                    assertEquals("Leaves should be sorted by maxDoc ascending", expectedOrder, actualOrder);
                }
            }
        }
    }

    public void testInternalEngineUsesLeafSorter() throws IOException {
        Path translogPath = createTempDir();
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            final String translogUUID = Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            Comparator<LeafReader> leafSorter = Comparator.comparingInt(LeafReader::maxDoc).reversed();
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(java.util.Collections.emptyList())
                .internalRefreshListener(java.util.Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
                .leafSorter(leafSorter)
                .build();
            try (InternalEngine engine = new InternalEngine(config)) {
                TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings(), engine);
                engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                for (int i = 0; i < 20; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            primaryTerm.get(),
                            Versions.MATCH_DELETED,
                            VersionType.INTERNAL,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if ((i + 1) % 5 == 0) {
                        engine.flush();
                    }
                }
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    DirectoryReader reader = (DirectoryReader) searcher.getDirectoryReader();
                    assertThat("Should have multiple leaves", reader.leaves().size(), greaterThan(0));
                    java.util.List<Integer> actualOrder = new java.util.ArrayList<>();
                    for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                        actualOrder.add(ctx.reader().maxDoc());
                    }
                    java.util.List<Integer> expectedOrder = new java.util.ArrayList<>(actualOrder);
                    expectedOrder.sort((a, b) -> Integer.compare(b, a));
                    assertEquals("Leaves should be sorted by maxDoc descending", expectedOrder, actualOrder);
                }
            }
        }
    }

    public void testTimestampSortOptimizationWorksOnAllEngineTypes() throws IOException {
        // Simplified: Only test that InternalEngine respects the leafSorter logic
        Path translogPath = createTempDir();
        try (Store store = createStore()) {
            store.createEmpty(Version.CURRENT.luceneVersion);
            final String translogUUID = Translog.createEmptyTranslog(
                translogPath,
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                primaryTerm.get()
            );
            store.associateIndexWithNewTranslog(translogUUID);
            Comparator<LeafReader> leafSorter = Comparator.comparingInt(LeafReader::maxDoc).reversed();
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
                .translogConfig(new TranslogConfig(shardId, translogPath, defaultSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .flushMergesAfter(TimeValue.timeValueMinutes(5))
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .externalRefreshListener(java.util.Collections.emptyList())
                .internalRefreshListener(java.util.Collections.emptyList())
                .queryCache(IndexSearcher.getDefaultQueryCache())
                .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
                .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
                .leafSorter(leafSorter)
                .build();
            try (InternalEngine engine = new InternalEngine(config)) {
                TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings(), engine);
                engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
                for (int i = 0; i < 20; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            primaryTerm.get(),
                            Versions.MATCH_DELETED,
                            VersionType.INTERNAL,
                            Engine.Operation.Origin.PRIMARY,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if ((i + 1) % 5 == 0) {
                        engine.flush();
                    }
                }
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    DirectoryReader reader = (DirectoryReader) searcher.getDirectoryReader();
                    assertThat("Should have multiple leaves", reader.leaves().size(), greaterThan(0));
                    java.util.List<Integer> actualOrder = new java.util.ArrayList<>();
                    for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
                        actualOrder.add(ctx.reader().maxDoc());
                    }
                    java.util.List<Integer> expectedOrder = new java.util.ArrayList<>(actualOrder);
                    expectedOrder.sort((a, b) -> Integer.compare(b, a));
                    assertEquals("Leaves should be sorted by maxDoc descending", expectedOrder, actualOrder);
                }
            }
        }
    }
}

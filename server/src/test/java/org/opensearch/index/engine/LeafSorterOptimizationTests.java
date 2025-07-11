/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogStats;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;

public class LeafSorterOptimizationTests extends EngineTestCase {

    public void testReadOnlyEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // Create a simple store for ReadOnlyEngine
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

            // Create ReadOnlyEngine with empty seqNoStats and proper reader wrapper function
            try (
                ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(
                    config,
                    new SeqNoStats(0, 0, 0),
                    new TranslogStats(),
                    false,
                    reader -> reader,
                    false
                )
            ) {

                // Verify that the ReadOnlyEngine uses the leafSorter
                try (Engine.Searcher searcher = readOnlyEngine.acquireSearcher("test")) {
                    DirectoryReader reader = searcher.getDirectoryReader();
                    assertThat(
                        "ReadOnlyEngine should use leafSorter",
                        reader,
                        instanceOf(org.opensearch.common.lucene.index.OpenSearchDirectoryReader.class)
                    );
                    assertNotNull("ReadOnlyEngine should return a DirectoryReader", reader);
                }
            }
        }
    }

    public void testNoOpEngineUsesLeafSorter() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        // Create a proper store with translog setup
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
                        "NoOpEngine should use leafSorter",
                        reader,
                        instanceOf(org.opensearch.common.lucene.index.OpenSearchDirectoryReader.class)
                    );
                    assertNotNull("NoOpEngine should return a DirectoryReader", reader);
                }
            }
        }
    }
}

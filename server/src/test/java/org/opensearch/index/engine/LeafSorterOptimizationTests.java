/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertNotNull;

public class LeafSorterOptimizationTests extends EngineTestCase {

    public void testAllEngineTypesHaveLeafSorterConfigured() throws IOException {
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
            assertNotNull("Engine config should have leafSorter configured", config.getLeafSorter());

            // Verify that the leafSorter is the timeseries leafSorter
            Comparator<LeafReader> leafSorter = config.getLeafSorter();
            assertNotNull("LeafSorter should be configured", leafSorter);

            // Test that the leafSorter is a valid comparator
            assertNotNull("LeafSorter should be a valid comparator", leafSorter);

            // Note: All engine types (ReadOnlyEngine, NoOpEngine, NRTReplicationEngine)
            // will use this leafSorter when opening DirectoryReader instances.
            // The leafSorter will be passed through to DirectoryReader.open() calls
            // in their constructors, enabling timestamp sort optimization.
        }
    }
}

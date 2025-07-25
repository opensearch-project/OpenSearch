/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Version;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;

import java.io.IOException;

import static java.util.Collections.emptyList;

public class NRTReplicationReaderManagerTests extends EngineTestCase {

    public void testCreateNRTreaderManager() throws IOException {
        try (final Store store = createStore()) {
            store.createEmpty(Version.LATEST);
            final DirectoryReader reader = DirectoryReader.open(store.directory());
            final SegmentInfos initialInfos = ((StandardDirectoryReader) reader).getSegmentInfos();

            // Create a minimal engine config for testing
            EngineConfig testConfig = new EngineConfig.Builder().shardId(shardId)
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
                .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
                .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
                .primaryTermSupplier(primaryTerm)
                .tombstoneDocSupplier(tombstoneDocSupplier())
                .build();

            NRTReplicationReaderManager readerManager = new NRTReplicationReaderManager(
                OpenSearchDirectoryReader.wrap(reader, shardId),
                (files) -> {},
                (files) -> {},
                testConfig
            );
            assertEquals(initialInfos, readerManager.getSegmentInfos());
            try (final OpenSearchDirectoryReader acquire = readerManager.acquire()) {
                assertNull(readerManager.refreshIfNeeded(acquire));
            }

            // create an updated infos
            final SegmentInfos infos_2 = readerManager.getSegmentInfos().clone();
            infos_2.changed();

            readerManager.updateSegments(infos_2);
            assertEquals(infos_2, readerManager.getSegmentInfos());
            try (final OpenSearchDirectoryReader acquire = readerManager.acquire()) {
                final StandardDirectoryReader standardReader = NRTReplicationReaderManager.unwrapStandardReader(acquire);
                assertEquals(infos_2, standardReader.getSegmentInfos());
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.lucene.index.IndexWriterConfig;
import org.opensearch.Version;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.env.ShardLock;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.lang.Math.random;
import static java.util.Collections.emptyList;
import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;


/**
 * Benchmark for {@link org.opensearch.index.engine.NRTReplicationEngine} to verify impact of commits
 */
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Fork(1)
@BenchmarkMode(Mode.All)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class NRTReplicationEngineBenchmark {

    @Benchmark
    public void testCommitOnReplica() throws IOException {
        int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", "bar"+i, Field.Store.NO));
            indexWriter.addDocument(document);

            System.out.println("--> Files in latest commit " + Arrays.toString(SegmentInfos.readLatestCommit(indexWriter.getDirectory()).files(true).toArray()));
            nrtReplicationEngine.updateSegments(SegmentInfos.readLatestCommit(indexWriter.getDirectory()));
        }
    }
    // Writer to generate segments
    IndexWriter indexWriter;

    // Test subject to be shared across benchmark
    NRTReplicationEngine nrtReplicationEngine;

    @Setup
    public void createNRTEngine() throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(Lucene.STANDARD_ANALYZER);
        Directory writerDirectory = new NIOFSDirectory(Paths.get("."));
        indexWriter = new IndexWriter(writerDirectory, iwc);

        Map<String, String> userData = new HashMap<>();
        userData.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID(new Random()));
//        userData.put(Translog.TRANSLOG_UUID_KEY, null);
        userData.put(LOCAL_CHECKPOINT_KEY, String.valueOf(-1));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(-1));
        indexWriter.setLiveCommitData(userData.entrySet());
        indexWriter.commit();

        Index index = new Index("test-index", IndexMetadata.INDEX_UUID_NA_VALUE);
        IndexSettings indexSettings = getIndexSettings(index);

        Directory replicaStoreDirectory = new NIOFSDirectory(Paths.get("replica_store"));
        final ShardId shardId = new ShardId(index.getName(), index.getUUID(), 0);
        Store store = new Store(shardId, indexSettings, replicaStoreDirectory, new DummyShardLock(shardId));

        final TranslogConfig translogConfig = new TranslogConfig(shardId, Paths.get("xlog"), indexSettings, BigArrays.NON_RECYCLING_INSTANCE);

        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

        EngineConfig engineConfig = new EngineConfig.Builder().shardId(shardId)
//            .threadPool(ThreadPool.ThreadPoolType.fromType("GENERIC"))
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .analyzer(iwc.getAnalyzer())
            .similarity(iwc.getSimilarity())
//            .codecService(new CodecService(null))
            .eventListener(null)
            .queryCache(IndexSearcher.getDefaultQueryCache())
            .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(1))
            .externalRefreshListener(emptyList())
            .internalRefreshListener(emptyList())
//            .indexSort(indexSort)
//            .circuitBreakerService(breakerService)
            .globalCheckpointSupplier(globalCheckpoint::get)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(() -> 1)
//            .tombstoneDocSupplier(tombstoneDocSupplier())
            .build();

        if (Lucene.indexExists(store.directory()) == false) {
            store.createEmpty(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(
                engineConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                1
            );
            store.associateIndexWithNewTranslog(translogUuid);
        }

        nrtReplicationEngine = new NRTReplicationEngine(engineConfig);
    }

    public class DummyShardLock extends ShardLock {

        public DummyShardLock(ShardId id) {
            super(id);
        }

        @Override
        protected void closeInternal() {}
    }

    public IndexSettings getIndexSettings(Index index) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(index.getName()).settings(build).build();
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        return new IndexSettings(metadata, Settings.EMPTY, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }

//    public EngineConfig getReplicaEngineConfig() {
//        final IndexWriterConfig iwc = newIndexWriterConfig();
//        final TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
//        final Engine.EventListener eventListener = new Engine.EventListener() {
//        }; // we don't need to notify anybody in this test
//        final List<ReferenceManager.RefreshListener> extRefreshListenerList = externalRefreshListener == null
//            ? emptyList()
//            : Collections.singletonList(externalRefreshListener);
//        final List<ReferenceManager.RefreshListener> intRefreshListenerList = internalRefreshListener == null
//            ? emptyList()
//            : Collections.singletonList(internalRefreshListener);
//        final LongSupplier globalCheckpointSupplier;
//        final Supplier<RetentionLeases> retentionLeasesSupplier;
//        if (maybeGlobalCheckpointSupplier == null) {
//            assert maybeRetentionLeasesSupplier == null;
//            final ReplicationTracker replicationTracker = new ReplicationTracker(
//                shardId,
//                allocationId.getId(),
//                indexSettings,
//                randomNonNegativeLong(),
//                SequenceNumbers.NO_OPS_PERFORMED,
//                update -> {},
//                () -> 0L,
//                (leases, listener) -> listener.onResponse(new ReplicationResponse()),
//                () -> SafeCommitInfo.EMPTY
//            );
//            globalCheckpointSupplier = replicationTracker;
//            retentionLeasesSupplier = replicationTracker::getRetentionLeases;
//        } else {
//            assert maybeRetentionLeasesSupplier != null;
//            globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
//            retentionLeasesSupplier = maybeRetentionLeasesSupplier;
//        }
//        return new EngineConfig.Builder().shardId(shardId)
//            .threadPool(threadPool)
//            .indexSettings(indexSettings)
//            .warmer(null)
//            .store(store)
//            .mergePolicy(mergePolicy)
//            .analyzer(iwc.getAnalyzer())
//            .similarity(iwc.getSimilarity())
//            .codecService(new CodecService(null, logger))
//            .eventListener(eventListener)
//            .queryCache(IndexSearcher.getDefaultQueryCache())
//            .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
//            .translogConfig(translogConfig)
//            .flushMergesAfter(TimeValue.timeValueMinutes(5))
//            .externalRefreshListener(extRefreshListenerList)
//            .internalRefreshListener(intRefreshListenerList)
//            .indexSort(indexSort)
//            .circuitBreakerService(breakerService)
//            .globalCheckpointSupplier(globalCheckpointSupplier)
//            .retentionLeasesSupplier(retentionLeasesSupplier)
//            .primaryTermSupplier(primaryTerm)
//            .tombstoneDocSupplier(tombstoneDocSupplier())
//            .build();
//    }
}

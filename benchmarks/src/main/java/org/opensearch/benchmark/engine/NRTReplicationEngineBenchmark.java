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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.lucene.index.IndexWriterConfig;
import org.opensearch.Version;
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
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyList;
import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;

/**
 * Benchmark for {@link org.opensearch.index.engine.NRTReplicationEngine} to verify commit performance
 */
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class NRTReplicationEngineBenchmark {
    private final Random random = new Random();
    private final AtomicInteger idGenerator = new AtomicInteger(1);

    // Writer to generate segments, mimicking primary shard
    private IndexWriter indexWriter;

    // Test subject to be shared across benchmark
    private NRTReplicationEngine nrtReplicationEngine;

    private Directory writerDirectory;
    private Directory replicaStoreDirectory;

    private File writerStorePath;
    private File replicaStorePath;
    private File replicaTranslogPath;

    @Benchmark
    public void testCommit_10Docs() throws IOException {
        indexDocumentsAndPersist(10);
        nrtReplicationEngine.updateSegments(SegmentInfos.readLatestCommit(indexWriter.getDirectory()));
    }

    @Benchmark
    public void testCommit_100Docs() throws IOException {
        indexDocumentsAndPersist(100);
        nrtReplicationEngine.updateSegments(SegmentInfos.readLatestCommit(indexWriter.getDirectory()));
    }

    @Benchmark
    public void testCommit_1000Docs() throws IOException {
        indexDocumentsAndPersist(1000);
        nrtReplicationEngine.updateSegments(SegmentInfos.readLatestCommit(indexWriter.getDirectory()));
    }

    @Benchmark
    public void testCommit_10000Docs() throws IOException {
        indexDocumentsAndPersist(10000);
        nrtReplicationEngine.updateSegments(SegmentInfos.readLatestCommit(indexWriter.getDirectory()));
    }

    @Benchmark
    public void testCommit_With_ForceMerge() throws IOException {
        indexWriter.forceMerge(1, true);
        indexDocumentsAndPersist(100);
        nrtReplicationEngine.updateSegments(SegmentInfos.readLatestCommit(indexWriter.getDirectory()));
    }

    private void indexDocumentsAndPersist(int numberOfDocs) throws IOException {
        for (int i = 0; i < numberOfDocs; i++) {
            Document document = new Document();
            document.add(new StringField("field", "bar" + idGenerator.getAndIncrement(), Field.Store.NO));
            indexWriter.addDocument(document);
        }
        // commit, sync and copy to replica directory
        indexWriter.commit();
        SegmentInfos latest = SegmentInfos.readLatestCommit(indexWriter.getDirectory());
        FileCleanUp.copyFiles(latest.files(true), writerStorePath.getPath(), replicaStorePath.getPath());
    }

    @TearDown
    public void tearDown() throws IOException {
        indexWriter.close();
        writerDirectory.close();
        replicaStoreDirectory.close();

        nrtReplicationEngine.close();
        // Delete directory and it's content
        FileCleanUp.cleanUpDirectory(writerStorePath);
        FileCleanUp.cleanUpDirectory(replicaStorePath);
        FileCleanUp.cleanUpDirectory(replicaTranslogPath);
    }

    @Setup
    public void createNRTEngine() throws IOException {
        writerStorePath = new File("primary_store");
        replicaStorePath = new File("replica_store");
        replicaTranslogPath = new File("transactions_log");

        // Create index writer
        IndexWriterConfig iwc = new IndexWriterConfig(Lucene.STANDARD_ANALYZER);
        writerDirectory = new NIOFSDirectory(writerStorePath.toPath());
        indexWriter = new IndexWriter(writerDirectory, iwc);

        // Create user data used in replica engine during instantiation
        Map<String, String> userData = new HashMap<>();
        userData.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID(random));
        userData.put(Translog.TRANSLOG_UUID_KEY, UUIDs.randomBase64UUID(random));
        userData.put(LOCAL_CHECKPOINT_KEY, String.valueOf(-1));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(-1));
        indexWriter.setLiveCommitData(userData.entrySet());
        indexWriter.commit();

        // Create replica store and config
        Index index = new Index("test-index", IndexMetadata.INDEX_UUID_NA_VALUE);
        IndexSettings indexSettings = getIndexSettings(index);
        replicaStoreDirectory = new NIOFSDirectory(replicaStorePath.toPath());
        final ShardId shardId = new ShardId(index.getName(), index.getUUID(), 0);
        Store replicaStore = new Store(shardId, indexSettings, replicaStoreDirectory, new DummyShardLock(shardId));

        final TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            replicaTranslogPath.toPath(),
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE
        );

        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        EngineConfig engineConfig = new EngineConfig.Builder().shardId(shardId)
            .indexSettings(indexSettings)
            .store(replicaStore)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .analyzer(iwc.getAnalyzer())
            .similarity(iwc.getSimilarity())
            .eventListener(null)
            .queryCache(IndexSearcher.getDefaultQueryCache())
            .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(1))
            .externalRefreshListener(emptyList())
            .internalRefreshListener(emptyList())
            .globalCheckpointSupplier(globalCheckpoint::get)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(() -> 1)
            .build();

        if (Lucene.indexExists(replicaStore.directory()) == false) {
            replicaStore.createEmpty(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(
                engineConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                1
            );
            replicaStore.associateIndexWithNewTranslog(translogUuid);
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

    static class FileCleanUp {
        private static void cleanUpDirectory(File directory) {
            if (directory.isDirectory()) {
                for (File file : directory.listFiles()) {
                    file.delete();
                }
                directory.delete();
            }
        }

        // Copies file to replica store, mimicking segment replication
        private static void copyFiles(Collection<String> files, String writerStorePath, String replicaStorePath) throws IOException {
            for (String fileName : files) {
                Path sourcePath = Paths.get(writerStorePath + "/" + fileName);
                Path targetPath = Paths.get(replicaStorePath + "/" + fileName);
                if (Files.exists(targetPath) == false) {
                    Files.copy(sourcePath, targetPath);
                }
            }
        }
    }
}

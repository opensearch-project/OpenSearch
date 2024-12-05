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

package org.opensearch.indices.recovery;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Numbers;
import org.opensearch.common.Randomness;
import org.opensearch.common.SetOnce;
import org.opensearch.common.UUIDs;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardRelocatedException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.RunUnderPrimaryPermit;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.test.CorruptionUtils;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This covers test cases for {@link RecoverySourceHandler} and {@link LocalStorePeerRecoverySourceHandler}.
 */
public class LocalStorePeerRecoverySourceHandlerTests extends OpenSearchTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
    );
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    private ThreadPool threadPool;
    private Executor recoveryExecutor;

    @Before
    public void setUpThreadPool() {
        if (randomBoolean()) {
            threadPool = new TestThreadPool(getTestName());
            recoveryExecutor = threadPool.generic();
        } else {
            // verify that both sending and receiving files can be completed with a single thread
            threadPool = new TestThreadPool(
                getTestName(),
                new FixedExecutorBuilder(Settings.EMPTY, "recovery_executor", between(1, 16), between(16, 128), "recovery_executor")
            );
            recoveryExecutor = threadPool.executor("recovery_executor");
        }
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testSendFiles() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Store store = newStore(createTempDir());
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot metadata = store.getMetadata();
        ReplicationLuceneIndex luceneIndex = new ReplicationLuceneIndex();
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
            luceneIndex.addFileDetail(md.name(), md.length(), false);
        }
        Store targetStore = newStore(createTempDir());
        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, luceneIndex, "", logger, () -> {});
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                ActionListener.completeWith(listener, () -> {
                    multiFileWriter.writeFileChunk(md, position, content, lastChunk);
                    return null;
                });
            }
        };
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            null,
            new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 5),
            between(1, 5)
        );
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        sendFilesFuture.actionGet();
        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata();
        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
        assertEquals(metas.size(), recoveryDiff.identical.size());
        assertEquals(0, recoveryDiff.different.size());
        assertEquals(0, recoveryDiff.missing.size());
        IndexReader reader = DirectoryReader.open(targetStore.directory());
        assertEquals(numDocs, reader.maxDoc());
        IOUtils.close(reader, store, multiFileWriter, targetStore);
    }

    public StartRecoveryRequest getStartRecoveryRequest() throws IOException {
        Store.MetadataSnapshot metadataSnapshot = randomBoolean()
            ? Store.MetadataSnapshot.EMPTY
            : new Store.MetadataSnapshot(
                Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()),
                randomIntBetween(0, 100)
            );
        return new StartRecoveryRequest(
            shardId,
            null,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ? SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong()
        );
    }

    public void testSendSnapshotSendsOps() throws IOException {
        final int fileChunkSizeInBytes = between(1, 4096);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final List<Translog.Operation> operations = new ArrayList<>();
        final int initialNumberOfDocs = randomIntBetween(10, 1000);
        for (int i = 0; i < initialNumberOfDocs; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, 1, SequenceNumbers.UNASSIGNED_SEQ_NO, true)));
        }
        final int numberOfDocsWithValidSequenceNumbers = randomIntBetween(10, 1000);
        for (int i = initialNumberOfDocs; i < initialNumberOfDocs + numberOfDocsWithValidSequenceNumbers; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, 1, i - initialNumberOfDocs, true)));
        }
        final long startingSeqNo = randomIntBetween(0, numberOfDocsWithValidSequenceNumbers - 1);
        final long endingSeqNo = randomLongBetween(startingSeqNo, numberOfDocsWithValidSequenceNumbers - 1);

        final Queue<Translog.Operation> shippedOps = ConcurrentCollections.newQueue();
        final AtomicLong checkpointOnTarget = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        RecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            @Override
            public void indexTranslogOperations(
                List<Translog.Operation> operations,
                int totalTranslogOps,
                long timestamp,
                long msu,
                RetentionLeases retentionLeases,
                long mappingVersion,
                ActionListener<Long> listener
            ) {
                shippedOps.addAll(operations);
                if (randomBoolean()) {
                    checkpointOnTarget.addAndGet(between(1, 20));
                }
                listener.onResponse(checkpointOnTarget.get());
            }
        };
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            new AsyncRecoveryTarget(recoveryTarget, threadPool.generic()),
            threadPool,
            request,
            fileChunkSizeInBytes,
            between(1, 10),
            between(1, 10)
        );
        PlainActionFuture<RecoverySourceHandler.SendSnapshotResult> future = new PlainActionFuture<>();
        handler.phase2(
            startingSeqNo,
            endingSeqNo,
            newTranslogSnapshot(operations, Collections.emptyList()),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            RetentionLeases.EMPTY,
            randomNonNegativeLong(),
            future
        );
        final int expectedOps = (int) (endingSeqNo - startingSeqNo + 1);
        RecoverySourceHandler.SendSnapshotResult result = future.actionGet();
        assertThat(result.sentOperations, equalTo(expectedOps));
        List<Translog.Operation> sortedShippedOps = shippedOps.stream()
            .sorted(Comparator.comparing(Translog.Operation::seqNo))
            .collect(Collectors.toList());
        assertThat(shippedOps.size(), equalTo(expectedOps));
        for (int i = 0; i < shippedOps.size(); i++) {
            assertThat(sortedShippedOps.get(i), equalTo(operations.get(i + (int) startingSeqNo + initialNumberOfDocs)));
        }
        assertThat(result.targetLocalCheckpoint, equalTo(checkpointOnTarget.get()));
    }

    public void testSendSnapshotStopOnError() throws Exception {
        final int fileChunkSizeInBytes = between(1, 10 * 1024);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final List<Translog.Operation> ops = new ArrayList<>();
        for (int numOps = between(1, 256), i = 0; i < numOps; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            ops.add(new Translog.Index(index, new Engine.IndexResult(1, 1, i, true)));
        }
        final AtomicBoolean wasFailed = new AtomicBoolean();
        RecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            @Override
            public void indexTranslogOperations(
                List<Translog.Operation> operations,
                int totalTranslogOps,
                long timestamp,
                long msu,
                RetentionLeases retentionLeases,
                long mappingVersion,
                ActionListener<Long> listener
            ) {
                if (randomBoolean()) {
                    listener.onResponse(SequenceNumbers.NO_OPS_PERFORMED);
                } else {
                    listener.onFailure(new RuntimeException("test - failed to index"));
                    wasFailed.set(true);
                }
            }
        };
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            new AsyncRecoveryTarget(recoveryTarget, threadPool.generic()),
            threadPool,
            request,
            fileChunkSizeInBytes,
            between(1, 10),
            between(1, 10)
        );
        PlainActionFuture<RecoverySourceHandler.SendSnapshotResult> future = new PlainActionFuture<>();
        final long startingSeqNo = randomLongBetween(0, ops.size() - 1L);
        final long endingSeqNo = randomLongBetween(startingSeqNo, ops.size() - 1L);
        handler.phase2(
            startingSeqNo,
            endingSeqNo,
            newTranslogSnapshot(ops, Collections.emptyList()),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            RetentionLeases.EMPTY,
            randomNonNegativeLong(),
            future
        );
        if (wasFailed.get()) {
            final RecoveryEngineException error = expectThrows(RecoveryEngineException.class, future::actionGet);
            assertThat(error.getMessage(), equalTo("Phase[2] failed to send/replay operations"));
            assertThat(error.getCause().getMessage(), equalTo("test - failed to index"));
        }
    }

    public void testSendOperationsConcurrently() throws Throwable {
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        Set<Long> receivedSeqNos = ConcurrentCollections.newConcurrentSet();
        long maxSeenAutoIdTimestamp = randomBoolean() ? -1 : randomNonNegativeLong();
        long maxSeqNoOfUpdatesOrDeletes = randomBoolean() ? -1 : randomNonNegativeLong();
        RetentionLeases retentionLeases = new RetentionLeases(randomNonNegativeLong(), randomNonNegativeLong(), Collections.emptySet());
        long mappingVersion = randomNonNegativeLong();
        AtomicLong localCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        int numOps = randomIntBetween(0, 1000);
        AtomicBoolean received = new AtomicBoolean();
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void indexTranslogOperations(
                List<Translog.Operation> operations,
                int receivedTotalOps,
                long receivedMaxSeenAutoIdTimestamp,
                long receivedMaxSeqNoOfUpdatesOrDeletes,
                RetentionLeases receivedRetentionLease,
                long receivedMappingVersion,
                ActionListener<Long> listener
            ) {
                received.set(true);
                assertThat(receivedMaxSeenAutoIdTimestamp, equalTo(maxSeenAutoIdTimestamp));
                assertThat(receivedMaxSeqNoOfUpdatesOrDeletes, equalTo(maxSeqNoOfUpdatesOrDeletes));
                assertThat(receivedRetentionLease, equalTo(retentionLeases));
                assertThat(receivedMappingVersion, equalTo(mappingVersion));
                assertThat(receivedTotalOps, equalTo(numOps));
                for (Translog.Operation operation : operations) {
                    receivedSeqNos.add(operation.seqNo());
                }
                if (randomBoolean()) {
                    localCheckpoint.addAndGet(randomIntBetween(1, 100));
                }
                listener.onResponse(localCheckpoint.get());
            }
        };

        PlainActionFuture<RecoverySourceHandler.SendSnapshotResult> sendFuture = new PlainActionFuture<>();
        long startingSeqNo = randomIntBetween(0, 1000);
        long endingSeqNo = startingSeqNo + randomIntBetween(0, 10000);
        List<Translog.Operation> operations = generateOperations(numOps);
        Randomness.shuffle(operations);
        List<Translog.Operation> skipOperations = randomSubsetOf(operations);
        Translog.Snapshot snapshot = newTranslogSnapshot(operations, skipOperations);
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool,
            getStartRecoveryRequest(),
            between(1, 10 * 1024),
            between(1, 5),
            between(1, 5)
        );
        handler.phase2(
            startingSeqNo,
            endingSeqNo,
            snapshot,
            maxSeenAutoIdTimestamp,
            maxSeqNoOfUpdatesOrDeletes,
            retentionLeases,
            mappingVersion,
            sendFuture
        );
        RecoverySourceHandler.SendSnapshotResult sendSnapshotResult = sendFuture.actionGet();
        assertTrue(received.get());
        assertThat(sendSnapshotResult.targetLocalCheckpoint, equalTo(localCheckpoint.get()));
        assertThat(sendSnapshotResult.sentOperations, equalTo(receivedSeqNos.size()));
        Set<Long> sentSeqNos = new HashSet<>();
        for (Translog.Operation op : operations) {
            if (startingSeqNo <= op.seqNo() && op.seqNo() <= endingSeqNo && skipOperations.contains(op) == false) {
                sentSeqNos.add(op.seqNo());
            }
        }
        assertThat(receivedSeqNos, equalTo(sentSeqNos));
    }

    private Engine.Index getIndex(final String id) {
        final ParseContext.Document document = new ParseContext.Document();
        document.add(new TextField("test", "test", Field.Store.YES));
        final Field idField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        final Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        final SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(idField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        final BytesReference source = new BytesArray(new byte[] { 1 });
        final ParsedDocument doc = new ParsedDocument(
            versionField,
            seqID,
            id,
            null,
            Arrays.asList(document),
            source,
            MediaTypeRegistry.JSON,
            null
        );
        return new Engine.Index(new Term("_id", Uid.encodeId(doc.id())), randomNonNegativeLong(), doc);
    }

    public void testHandleCorruptedIndexOnSendSendFiles() throws Throwable {
        Settings settings = Settings.builder()
            .put("indices.recovery.concurrent_streams", 1)
            .put("indices.recovery.concurrent_small_file_streams", 1)
            .build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();

        ReplicationLuceneIndex luceneIndex = new ReplicationLuceneIndex();
        Store.MetadataSnapshot metadata = store.getMetadata();
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
            luceneIndex.addFileDetail(md.name(), md.length(), false);
        }

        CorruptionUtils.corruptFile(
            random(),
            FileSystemUtils.files(
                tempDir,
                (p) -> (p.getFileName().toString().equals("write.lock") || p.getFileName().toString().startsWith("extra")) == false
            )
        );
        Store targetStore = newStore(createTempDir(), false);
        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, luceneIndex, "", logger, () -> {});
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                ActionListener.completeWith(listener, () -> {
                    multiFileWriter.writeFileChunk(md, position, content, lastChunk);
                    return null;
                });
            }
        };
        IndexShard mockShard = mock(IndexShard.class);
        when(mockShard.shardId()).thenReturn(new ShardId("testIndex", "testUUID", 0));
        doAnswer(invocation -> {
            assertFalse(failedEngine.get());
            failedEngine.set(true);
            return null;
        }).when(mockShard).failShard(any(), any());
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            mockShard,
            new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 8),
            between(1, 8)
        );
        SetOnce<Exception> sendFilesError = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        handler.sendFiles(
            store,
            metas.toArray(new StoreFileMetadata[0]),
            () -> 0,
            new LatchedActionListener<>(ActionListener.wrap(r -> sendFilesError.set(null), e -> sendFilesError.set(e)), latch)
        );
        latch.await();
        assertThat(sendFilesError.get(), instanceOf(IOException.class));
        assertNotNull(ExceptionsHelper.unwrapCorruption(sendFilesError.get()));
        failedEngine.get();
        assertTrue(failedEngine.get());
        // ensure all chunk requests have been completed; otherwise some files on the target are left open.
        IOUtils.close(() -> terminate(threadPool), () -> threadPool = null);
        IOUtils.close(store, multiFileWriter, targetStore);
    }

    public void testHandleExceptionOnSendFiles() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot metadata = store.getMetadata();
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
        }
        final boolean throwCorruptedIndexException = randomBoolean();
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                if (throwCorruptedIndexException) {
                    listener.onFailure(new RuntimeException(new CorruptIndexException("foo", "bar")));
                } else {
                    listener.onFailure(new RuntimeException("boom"));
                }
            }
        };
        IndexShard mockShard = mock(IndexShard.class);
        when(mockShard.shardId()).thenReturn(new ShardId("testIndex", "testUUID", 0));
        doAnswer(invocation -> {
            assertFalse(failedEngine.get());
            failedEngine.set(true);
            return null;
        }).when(mockShard).failShard(any(), any());
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            mockShard,
            new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 10),
            between(1, 4)
        );
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        Exception ex = expectThrows(Exception.class, sendFilesFuture::actionGet);
        final IOException unwrappedCorruption = ExceptionsHelper.unwrapCorruption(ex);
        if (throwCorruptedIndexException) {
            assertNotNull(unwrappedCorruption);
            assertEquals(ex.getMessage(), "[File corruption occurred on recovery but checksums are ok]");
        } else {
            assertNull(unwrappedCorruption);
            assertEquals(ex.getMessage(), "boom");
        }
        assertFalse(failedEngine.get());
        IOUtils.close(store);
    }

    public void testThrowExceptionOnPrimaryRelocatedBeforePhase1Started() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean(), anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.isRelocatedPrimary()).thenReturn(true);
        when(shard.acquireSafeIndexCommit()).thenReturn(mock(GatedCloseable.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>) invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), any());

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            );
        if (randomBoolean()) {
            indexMetadata.state(IndexMetadata.State.CLOSE);
        }
        when(shard.indexSettings()).thenReturn(new IndexSettings(indexMetadata.build(), Settings.EMPTY));

        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 8),
            between(1, 8)
        ) {

            @Override
            void phase1(
                IndexCommit snapshot,
                long startingSeqNo,
                IntSupplier translogOps,
                ActionListener<SendFileResult> listener,
                boolean skipCreateRetentionLeaseStep
            ) {
                phase1Called.set(true);
                super.phase1(snapshot, startingSeqNo, translogOps, listener, skipCreateRetentionLeaseStep);
            }

            @Override
            void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
                prepareTargetForTranslogCalled.set(true);
                super.prepareTargetForTranslog(totalTranslogOps, listener);
            }

            @Override
            void phase2(
                long startingSeqNo,
                long endingSeqNo,
                Translog.Snapshot snapshot,
                long maxSeenAutoIdTimestamp,
                long maxSeqNoOfUpdatesOrDeletes,
                RetentionLeases retentionLeases,
                long mappingVersion,
                ActionListener<SendSnapshotResult> listener
            ) throws IOException {
                phase2Called.set(true);
                super.phase2(
                    startingSeqNo,
                    endingSeqNo,
                    snapshot,
                    maxSeenAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    listener
                );
            }

        };
        PlainActionFuture<RecoveryResponse> future = new PlainActionFuture<>();
        expectThrows(IndexShardRelocatedException.class, () -> {
            handler.recoverToTarget(future);
            future.actionGet();
        });
        assertFalse(phase1Called.get());
        assertFalse(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
    }

    /*
    If the replica allocation id is not reflected in source nodes routing table even after retries,
    recoveries should fail
     */
    public void testThrowExceptionOnNoTargetInRouting() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean(), anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.isRelocatedPrimary()).thenReturn(false);
        final org.opensearch.index.shard.ReplicationGroup replicationGroup = mock(org.opensearch.index.shard.ReplicationGroup.class);
        final IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.getByAllocationId(anyString())).thenReturn(null);
        when(shard.getReplicationGroup()).thenReturn(replicationGroup);
        when(replicationGroup.getRoutingTable()).thenReturn(routingTable);
        when(shard.acquireSafeIndexCommit()).thenReturn(mock(GatedCloseable.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>) invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), any());

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            );
        if (randomBoolean()) {
            indexMetadata.state(IndexMetadata.State.CLOSE);
        }
        when(shard.indexSettings()).thenReturn(new IndexSettings(indexMetadata.build(), Settings.EMPTY));

        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 8),
            between(1, 8)
        ) {

            @Override
            void phase1(
                IndexCommit snapshot,
                long startingSeqNo,
                IntSupplier translogOps,
                ActionListener<SendFileResult> listener,
                boolean skipCreateRetentionLeaseStep
            ) {
                phase1Called.set(true);
                super.phase1(snapshot, startingSeqNo, translogOps, listener, skipCreateRetentionLeaseStep);
            }

            @Override
            void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
                prepareTargetForTranslogCalled.set(true);
                super.prepareTargetForTranslog(totalTranslogOps, listener);
            }

            @Override
            void phase2(
                long startingSeqNo,
                long endingSeqNo,
                Translog.Snapshot snapshot,
                long maxSeenAutoIdTimestamp,
                long maxSeqNoOfUpdatesOrDeletes,
                RetentionLeases retentionLeases,
                long mappingVersion,
                ActionListener<SendSnapshotResult> listener
            ) throws IOException {
                phase2Called.set(true);
                super.phase2(
                    startingSeqNo,
                    endingSeqNo,
                    snapshot,
                    maxSeenAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    listener
                );
            }

        };
        PlainActionFuture<RecoveryResponse> future = new PlainActionFuture<>();
        expectThrows(DelayRecoveryException.class, () -> {
            handler.recoverToTarget(future);
            future.actionGet();
        });
        verify(routingTable, times(5)).getByAllocationId(null);
        assertFalse(phase1Called.get());
        assertFalse(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
    }

    /*
    Tests when the replica allocation id is  reflected in source nodes routing table even after 1 retry
    */
    public void testTargetInRoutingInSecondAttempt() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean(), anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.isRelocatedPrimary()).thenReturn(false);
        when(shard.getRetentionLeases()).thenReturn(mock(RetentionLeases.class));
        final org.opensearch.index.shard.ReplicationGroup replicationGroup = mock(org.opensearch.index.shard.ReplicationGroup.class);
        final IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        final ShardRouting shardRouting = mock(ShardRouting.class);
        when(shardRouting.initializing()).thenReturn(true);
        when(shardRouting.currentNodeId()).thenReturn("node");
        when(routingTable.getByAllocationId(any())).thenReturn(null, shardRouting);
        when(shard.getReplicationGroup()).thenReturn(replicationGroup);
        when(replicationGroup.getRoutingTable()).thenReturn(routingTable);
        when(shard.acquireSafeIndexCommit()).thenReturn(mock(GatedCloseable.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>) invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), any());

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            );
        if (randomBoolean()) {
            indexMetadata.state(IndexMetadata.State.CLOSE);
        }
        when(shard.indexSettings()).thenReturn(new IndexSettings(indexMetadata.build(), Settings.EMPTY));

        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 8),
            between(1, 8)
        ) {

            @Override
            void phase1(
                IndexCommit snapshot,
                long startingSeqNo,
                IntSupplier translogOps,
                ActionListener<SendFileResult> listener,
                boolean skipCreateRetentionLeaseStep
            ) {
                phase1Called.set(true);
                super.phase1(snapshot, startingSeqNo, translogOps, listener, skipCreateRetentionLeaseStep);
            }

            @Override
            void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
                prepareTargetForTranslogCalled.set(true);
                super.prepareTargetForTranslog(totalTranslogOps, listener);
            }

            @Override
            void phase2(
                long startingSeqNo,
                long endingSeqNo,
                Translog.Snapshot snapshot,
                long maxSeenAutoIdTimestamp,
                long maxSeqNoOfUpdatesOrDeletes,
                RetentionLeases retentionLeases,
                long mappingVersion,
                ActionListener<SendSnapshotResult> listener
            ) throws IOException {
                phase2Called.set(true);
                super.phase2(
                    startingSeqNo,
                    endingSeqNo,
                    snapshot,
                    maxSeenAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    listener
                );
            }

        };
        handler.waitForAssignmentPropagate(new SetOnce<>());
        verify(routingTable, times(2)).getByAllocationId(null);
    }

    public void testCancellationsDoesNotLeakPrimaryPermits() throws Exception {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final IndexShard shard = mock(IndexShard.class);
        final AtomicBoolean freed = new AtomicBoolean(true);
        when(shard.isRelocatedPrimary()).thenReturn(false);
        doAnswer(invocation -> {
            freed.set(false);
            ((ActionListener<Releasable>) invocation.getArguments()[0]).onResponse(() -> freed.set(true));
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), any());

        Thread cancelingThread = new Thread(() -> cancellableThreads.cancel("test"));
        cancelingThread.start();
        try {
            RunUnderPrimaryPermit.run(() -> {}, "test", shard, cancellableThreads, logger);
        } catch (CancellableThreads.ExecutionCancelledException e) {
            // expected.
        }
        cancelingThread.join();
        // we have to use assert busy as we may be interrupted while acquiring the permit, if so we want to check
        // that the permit is released.
        assertBusy(() -> assertTrue(freed.get()));
    }

    public void testSendFileChunksConcurrently() throws Exception {
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final List<FileChunkResponse> unrepliedChunks = new CopyOnWriteArrayList<>();
        final AtomicInteger sentChunks = new AtomicInteger();
        final TestRecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            final AtomicLong chunkNumberGenerator = new AtomicLong();

            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                final long chunkNumber = chunkNumberGenerator.getAndIncrement();
                logger.info("--> write chunk name={} seq={}, position={}", md.name(), chunkNumber, position);
                unrepliedChunks.add(new FileChunkResponse(chunkNumber, listener));
                sentChunks.incrementAndGet();
            }
        };
        final int maxConcurrentChunks = between(1, 8);
        final int chunkSize = between(1, 32);
        final RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            recoveryTarget,
            threadPool,
            getStartRecoveryRequest(),
            chunkSize,
            maxConcurrentChunks,
            between(1, 10)
        );
        Store store = newStore(createTempDir(), false);
        List<StoreFileMetadata> files = generateFiles(store, between(1, 10), () -> between(1, chunkSize * 20));
        int totalChunks = files.stream().mapToInt(md -> ((int) md.length() + chunkSize - 1) / chunkSize).sum();
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, files.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        assertBusy(() -> {
            assertThat(sentChunks.get(), equalTo(Math.min(totalChunks, maxConcurrentChunks)));
            assertThat(unrepliedChunks, hasSize(sentChunks.get()));
        });

        List<FileChunkResponse> ackedChunks = new ArrayList<>();
        while (sentChunks.get() < totalChunks || unrepliedChunks.isEmpty() == false) {
            List<FileChunkResponse> chunksToAck = randomSubsetOf(between(1, unrepliedChunks.size()), unrepliedChunks);
            unrepliedChunks.removeAll(chunksToAck);
            ackedChunks.addAll(chunksToAck);
            ackedChunks.sort(Comparator.comparing(c -> c.chunkNumber));
            int checkpoint = -1;
            for (int i = 0; i < ackedChunks.size(); i++) {
                if (i != ackedChunks.get(i).chunkNumber) {
                    break;
                } else {
                    checkpoint = i;
                }
            }
            int chunksToSend = Math.min(
                totalChunks - sentChunks.get(),                             // limited by the remaining chunks
                maxConcurrentChunks - (sentChunks.get() - 1 - checkpoint)
            ); // limited by the buffering chunks

            int expectedSentChunks = sentChunks.get() + chunksToSend;
            int expectedUnrepliedChunks = unrepliedChunks.size() + chunksToSend;
            chunksToAck.forEach(c -> c.listener.onResponse(null));
            assertBusy(() -> {
                assertThat(sentChunks.get(), equalTo(expectedSentChunks));
                assertThat(unrepliedChunks, hasSize(expectedUnrepliedChunks));
            });
        }
        sendFilesFuture.actionGet();
        store.close();
    }

    public void testSendFileChunksStopOnError() throws Exception {
        final List<FileChunkResponse> unrepliedChunks = new CopyOnWriteArrayList<>();
        final AtomicInteger sentChunks = new AtomicInteger();
        final TestRecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            final AtomicLong chunkNumberGenerator = new AtomicLong();

            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                final long chunkNumber = chunkNumberGenerator.getAndIncrement();
                logger.info("--> write chunk name={} seq={}, position={}", md.name(), chunkNumber, position);
                unrepliedChunks.add(new FileChunkResponse(chunkNumber, listener));
                sentChunks.incrementAndGet();
            }
        };
        final int maxConcurrentChunks = between(1, 4);
        final int chunkSize = between(1, 16);
        final RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            null,
            new AsyncRecoveryTarget(recoveryTarget, recoveryExecutor),
            threadPool,
            getStartRecoveryRequest(),
            chunkSize,
            maxConcurrentChunks,
            between(1, 5)
        );
        Store store = newStore(createTempDir(), false);
        List<StoreFileMetadata> files = generateFiles(store, between(1, 10), () -> between(1, chunkSize * 20));
        int totalChunks = files.stream().mapToInt(md -> ((int) md.length() + chunkSize - 1) / chunkSize).sum();
        SetOnce<Exception> sendFilesError = new SetOnce<>();
        CountDownLatch sendFilesLatch = new CountDownLatch(1);
        handler.sendFiles(
            store,
            files.toArray(new StoreFileMetadata[0]),
            () -> 0,
            new LatchedActionListener<>(ActionListener.wrap(r -> sendFilesError.set(null), e -> sendFilesError.set(e)), sendFilesLatch)
        );
        assertBusy(() -> assertThat(sentChunks.get(), equalTo(Math.min(totalChunks, maxConcurrentChunks))));
        List<FileChunkResponse> failedChunks = randomSubsetOf(between(1, unrepliedChunks.size()), unrepliedChunks);
        CountDownLatch replyLatch = new CountDownLatch(failedChunks.size());
        failedChunks.forEach(c -> {
            c.listener.onFailure(new IllegalStateException("test chunk exception"));
            replyLatch.countDown();
        });
        replyLatch.await();
        unrepliedChunks.removeAll(failedChunks);
        unrepliedChunks.forEach(c -> {
            if (randomBoolean()) {
                c.listener.onFailure(new RuntimeException("test"));
            } else {
                c.listener.onResponse(null);
            }
        });
        sendFilesLatch.await();
        assertThat(sendFilesError.get(), instanceOf(IllegalStateException.class));
        assertThat(sendFilesError.get().getMessage(), containsString("test chunk exception"));
        assertThat("no more chunks should be sent", sentChunks.get(), equalTo(Math.min(totalChunks, maxConcurrentChunks)));
        store.close();
    }

    public void testCancelRecoveryDuringPhase1() throws Exception {
        Store store = newStore(createTempDir("source"), false);
        IndexShard shard = mock(IndexShard.class);
        when(shard.store()).thenReturn(store);
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();
        AtomicBoolean wasCancelled = new AtomicBoolean();
        SetOnce<Runnable> cancelRecovery = new SetOnce<>();
        final TestRecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            @Override
            public void receiveFileInfo(
                List<String> phase1FileNames,
                List<Long> phase1FileSizes,
                List<String> phase1ExistingFileNames,
                List<Long> phase1ExistingFileSizes,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                recoveryExecutor.execute(() -> listener.onResponse(null));
                if (randomBoolean()) {
                    wasCancelled.set(true);
                    cancelRecovery.get().run();
                }
            }

            @Override
            public void writeFileChunk(
                StoreFileMetadata md,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                recoveryExecutor.execute(() -> listener.onResponse(null));
                if (rarely()) {
                    wasCancelled.set(true);
                    cancelRecovery.get().run();
                }
            }

            @Override
            public void cleanFiles(
                int totalTranslogOps,
                long globalCheckpoint,
                Store.MetadataSnapshot sourceMetadata,
                ActionListener<Void> listener
            ) {
                recoveryExecutor.execute(() -> listener.onResponse(null));
                if (randomBoolean()) {
                    wasCancelled.set(true);
                    cancelRecovery.get().run();
                }
            }
        };
        final StartRecoveryRequest startRecoveryRequest = getStartRecoveryRequest();
        final RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            recoveryTarget,
            threadPool,
            startRecoveryRequest,
            between(1, 16),
            between(1, 4),
            between(1, 4)
        ) {
            @Override
            void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                final String leaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(startRecoveryRequest.targetNode().getId());
                listener.onResponse(
                    new RetentionLease(
                        leaseId,
                        startingSeqNo,
                        threadPool.absoluteTimeInMillis(),
                        ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
                    )
                );
            }
        };
        cancelRecovery.set(() -> handler.cancel("test"));
        final StepListener<RecoverySourceHandler.SendFileResult> phase1Listener = new StepListener<>();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            handler.phase1(DirectoryReader.listCommits(dir).get(0), 0, () -> 0, new LatchedActionListener<>(phase1Listener, latch), false);
            latch.await();
            phase1Listener.result();
        } catch (Exception e) {
            assertTrue(wasCancelled.get());
            assertNotNull(ExceptionsHelper.unwrap(e, CancellableThreads.ExecutionCancelledException.class));
        }
        store.close();
    }

    public void testVerifySeqNoStatsWhenRecoverWithSyncId() throws Exception {
        IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        RecoverySourceHandler handler = new LocalStorePeerRecoverySourceHandler(
            shard,
            new TestRecoveryTargetHandler(),
            threadPool,
            getStartRecoveryRequest(),
            between(1, 16),
            between(1, 4),
            between(1, 4)
        );

        String syncId = UUIDs.randomBase64UUID();
        int numDocs = between(0, 1000);
        long localCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        long maxSeqNo = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        assertTrue(
            handler.canSkipPhase1(
                newMetadataSnapshot(syncId, Long.toString(localCheckpoint), Long.toString(maxSeqNo), numDocs),
                newMetadataSnapshot(syncId, Long.toString(localCheckpoint), Long.toString(maxSeqNo), numDocs)
            )
        );

        AssertionError error = expectThrows(AssertionError.class, () -> {
            long localCheckpointOnTarget = randomValueOtherThan(
                localCheckpoint,
                () -> randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE)
            );
            long maxSeqNoOnTarget = randomValueOtherThan(
                maxSeqNo,
                () -> randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE)
            );
            handler.canSkipPhase1(
                newMetadataSnapshot(syncId, Long.toString(localCheckpoint), Long.toString(maxSeqNo), numDocs),
                newMetadataSnapshot(syncId, Long.toString(localCheckpointOnTarget), Long.toString(maxSeqNoOnTarget), numDocs)
            );
        });
        assertThat(error.getMessage(), containsString("try to recover [index][1] with sync id but seq_no stats are mismatched:"));
    }

    private Store.MetadataSnapshot newMetadataSnapshot(String syncId, String localCheckpoint, String maxSeqNo, int numDocs) {
        Map<String, String> userData = new HashMap<>();
        userData.put(Engine.SYNC_COMMIT_ID, syncId);
        if (localCheckpoint != null) {
            userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpoint);
        }
        if (maxSeqNo != null) {
            userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, maxSeqNo);
        }
        return new Store.MetadataSnapshot(Collections.emptyMap(), userData, numDocs);
    }

    private Store newStore(Path path) throws IOException {
        return newStore(path, true);
    }

    private Store newStore(Path path, boolean checkIndex) throws IOException {
        BaseDirectoryWrapper baseDirectoryWrapper = LocalStorePeerRecoverySourceHandlerTests.newFSDirectory(path);
        if (checkIndex == false) {
            baseDirectoryWrapper.setCheckIndexOnClose(false); // don't run checkindex we might corrupt the index in these tests
        }
        return new Store(shardId, INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId));
    }

    static final class FileChunkResponse {
        final long chunkNumber;
        final ActionListener<Void> listener;

        FileChunkResponse(long chunkNumber, ActionListener<Void> listener) {
            this.chunkNumber = chunkNumber;
            this.listener = listener;
        }
    }

    private List<StoreFileMetadata> generateFiles(Store store, int numFiles, IntSupplier fileSizeSupplier) throws IOException {
        List<StoreFileMetadata> files = new ArrayList<>();
        for (int i = 0; i < numFiles; i++) {
            byte[] buffer = randomByteArrayOfLength(fileSizeSupplier.getAsInt());
            CRC32 digest = new CRC32();
            digest.update(buffer, 0, buffer.length);
            StoreFileMetadata md = new StoreFileMetadata(
                "test-" + i,
                buffer.length + 8,
                Store.digestToString(digest.getValue()),
                org.apache.lucene.util.Version.LATEST
            );
            try (OutputStream out = new IndexOutputOutputStream(store.createVerifyingOutput(md.name(), md, IOContext.DEFAULT))) {
                out.write(buffer);
                out.write(Numbers.longToBytes(digest.getValue()));
            }
            store.directory().sync(Collections.singleton(md.name()));
            files.add(md);
        }
        return files;
    }

    class TestRecoveryTargetHandler implements RecoveryTargetHandler {
        @Override
        public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {}

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {}

        @Override
        public void forceSegmentFileSync() {}

        @Override
        public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext) {}

        @Override
        public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long timestamp,
            final long msu,
            final RetentionLeases retentionLeases,
            final long mappingVersion,
            final ActionListener<Long> listener
        ) {}

        @Override
        public void receiveFileInfo(
            List<String> phase1FileNames,
            List<Long> phase1FileSizes,
            List<String> phase1ExistingFileNames,
            List<Long> phase1ExistingFileSizes,
            int totalTranslogOps,
            ActionListener<Void> listener
        ) {

        }

        @Override
        public void cleanFiles(
            int totalTranslogOps,
            long globalCheckpoint,
            Store.MetadataSnapshot sourceMetadata,
            ActionListener<Void> listener
        ) {}

        @Override
        public void writeFileChunk(
            StoreFileMetadata fileMetadata,
            long position,
            BytesReference content,
            boolean lastChunk,
            int totalTranslogOps,
            ActionListener<Void> listener
        ) {}
    }

    private Translog.Snapshot newTranslogSnapshot(List<Translog.Operation> operations, List<Translog.Operation> operationsToSkip) {
        Iterator<Translog.Operation> iterator = operations.iterator();
        return new Translog.Snapshot() {
            int skippedCount = 0;

            @Override
            public int totalOperations() {
                return operations.size();
            }

            @Override
            public int skippedOperations() {
                return skippedCount;
            }

            @Override
            public Translog.Operation next() {
                while (iterator.hasNext()) {
                    Translog.Operation op = iterator.next();
                    if (operationsToSkip.contains(op)) {
                        skippedCount++;
                    } else {
                        return op;
                    }
                }
                return null;
            }

            @Override
            public void close() {

            }
        };
    }

    private static List<Translog.Operation> generateOperations(int numOps) {
        final List<Translog.Operation> operations = new ArrayList<>(numOps);
        final byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
        final Set<Long> seqNos = new HashSet<>();
        for (int i = 0; i < numOps; i++) {
            final long seqNo = randomValueOtherThanMany(n -> seqNos.add(n) == false, OpenSearchTestCase::randomNonNegativeLong);
            final Translog.Operation op;
            if (randomBoolean()) {
                op = new Translog.Index("id", seqNo, randomNonNegativeLong(), randomNonNegativeLong(), source, null, -1);
            } else if (randomBoolean()) {
                op = new Translog.Delete("id", seqNo, randomNonNegativeLong(), randomNonNegativeLong());
            } else {
                op = new Translog.NoOp(seqNo, randomNonNegativeLong(), "test");
            }
            operations.add(op);
        }
        return operations;
    }
}

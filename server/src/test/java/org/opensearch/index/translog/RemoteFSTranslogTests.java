/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.MissingHistoryOperationsException;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.LocalCheckpointTrackerTests;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
import static org.opensearch.index.translog.RemoteFsTranslog.TRANSLOG;
import static org.opensearch.index.translog.SnapshotMatchers.containsOperationsInAnyOrder;
import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")

public class RemoteFSTranslogTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    protected RemoteFsTranslog translog;
    private AtomicLong globalCheckpoint;
    protected Path translogDir;
    // A default primary term is used by translog instances created in this test.
    private final AtomicLong primaryTerm = new AtomicLong();
    private final AtomicBoolean primaryMode = new AtomicBoolean(true);
    private final AtomicReference<LongConsumer> persistedSeqNoConsumer = new AtomicReference<>();
    private ThreadPool threadPool;
    private final static String METADATA_DIR = "metadata";
    private final static String DATA_DIR = "data";
    BlobStoreRepository repository;

    BlobStoreTransferService blobStoreTransferService;

    TestTranslog.FailSwitch fail;

    private LongConsumer getPersistedSeqNoConsumer() {
        return seqNo -> {
            final LongConsumer consumer = persistedSeqNoConsumer.get();
            if (consumer != null) {
                consumer.accept(seqNo);
            }
        };
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Integer.MAX_VALUE));
        // if a previous test failed we clean up things here
        translogDir = createTempDir();
        translog = create(translogDir);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            translog.getDeletionPolicy().assertNoOpenTranslogRefs();
            translog.close();
        } finally {
            super.tearDown();
            terminate(threadPool);
        }
    }

    private RemoteFsTranslog create(Path path) throws IOException {
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        return create(path, createRepository(), translogUUID);
    }

    private RemoteFsTranslog create(Path path, BlobStoreRepository repository, String translogUUID) throws IOException {
        this.repository = repository;
        globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final TranslogConfig translogConfig = getTranslogConfig(path);
        final TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(translogConfig.getIndexSettings());
        threadPool = new TestThreadPool(getClass().getName());
        blobStoreTransferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        return new RemoteFsTranslog(
            translogConfig,
            translogUUID,
            deletionPolicy,
            () -> globalCheckpoint.get(),
            primaryTerm::get,
            getPersistedSeqNoConsumer(),
            repository,
            threadPool,
            primaryMode::get
        );

    }

    private TranslogConfig getTranslogConfig(final Path path) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            // only randomize between nog age retention and a long one, so failures will have a chance of reproducing
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomBoolean() ? "-1ms" : "1h")
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), randomIntBetween(-1, 2048) + "b")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .build();
        return getTranslogConfig(path, settings);
    }

    private TranslogConfig getTranslogConfig(final Path path, final Settings settings) {
        final ByteSizeValue bufferSize = randomFrom(
            TranslogConfig.DEFAULT_BUFFER_SIZE,
            new ByteSizeValue(8, ByteSizeUnit.KB),
            new ByteSizeValue(10 + randomInt(128 * 1024), ByteSizeUnit.BYTES)
        );

        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);
        return new TranslogConfig(shardId, path, indexSettings, NON_RECYCLING_INSTANCE, bufferSize);
    }

    private BlobStoreRepository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        fail = new TestTranslog.FailSwitch();
        fail.failNever();
        final FsRepository repository = new ThrowingBlobRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            fail
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }

    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                .build()
        );
    }

    private Translog.Location addToTranslogAndList(Translog translog, List<Translog.Operation> list, Translog.Operation op)
        throws IOException {
        Translog.Location loc = translog.add(op);
        Random random = random();
        if (random.nextBoolean()) {
            translog.ensureSynced(loc);
        }
        list.add(op);
        return loc;
    }

    private Translog.Location addToTranslogAndListAndUpload(Translog translog, List<Translog.Operation> list, Translog.Operation op)
        throws IOException {
        Translog.Location loc = translog.add(op);
        translog.ensureSynced(loc);
        list.add(op);
        return loc;
    }

    public void testUploadWithPrimaryModeFalse() {
        // Test setup
        primaryMode.set(false);

        // Validate
        assertTrue(translog.syncNeeded());
        assertFalse(primaryMode.get());
        try {
            translog.sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertTrue(translog.syncNeeded());
    }

    public void testUploadWithPrimaryModeTrue() {
        // Validate
        assertTrue(translog.syncNeeded());
        assertTrue(primaryMode.get());
        try {
            translog.sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertFalse(translog.syncNeeded());
    }

    public void testSimpleOperations() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        addToTranslogAndList(translog, ops, new Translog.Delete("2", 1, primaryTerm.get()));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
            assertThat(snapshot, containsOperationsInAnyOrder(ops));
        }

        final long seqNo = randomLongBetween(0, Integer.MAX_VALUE);
        final String reason = randomAlphaOfLength(16);
        final long noopTerm = randomLongBetween(1, primaryTerm.get());
        addToTranslogAndList(translog, ops, new Translog.NoOp(seqNo, noopTerm, reason));

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, containsOperationsInAnyOrder(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        try (Translog.Snapshot snapshot = translog.newSnapshot(seqNo + 1, randomLongBetween(seqNo + 1, Long.MAX_VALUE))) {
            assertThat(snapshot, SnapshotMatchers.size(0));
            assertThat(snapshot.totalOperations(), equalTo(0));
        }

    }

    public void testReadLocation() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        ArrayList<Translog.Location> locs = new ArrayList<>();
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 })));
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[] { 1 })));
        locs.add(addToTranslogAndList(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[] { 1 })));
        translog.sync();
        int i = 0;
        for (Translog.Operation op : ops) {
            assertEquals(op, translog.readOperation(locs.get(i++)));
        }
        assertNull(translog.readOperation(new Translog.Location(100, 0, 0)));
    }

    public void testReadLocationDownload() throws IOException {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        ArrayList<Translog.Location> locs = new ArrayList<>();
        locs.add(addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 })));
        locs.add(addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[] { 1 })));
        locs.add(addToTranslogAndListAndUpload(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[] { 1 })));
        translog.sync();
        int i = 0;
        for (Translog.Operation op : ops) {
            assertEquals(op, translog.readOperation(locs.get(i++)));
        }

        String translogUUID = translog.translogUUID;
        try {
            translog.getDeletionPolicy().assertNoOpenTranslogRefs();
            translog.close();
        } finally {
            terminate(threadPool);
        }

        // Delete translog files to test download flow
        for (Path file : FileSystemUtils.files(translogDir)) {
            Files.delete(file);
        }

        // Creating RemoteFsTranslog with the same location
        Translog newTranslog = create(translogDir, repository, translogUUID);
        i = 0;
        for (Translog.Operation op : ops) {
            assertEquals(op, newTranslog.readOperation(locs.get(i++)));
        }
        try {
            newTranslog.close();
        } catch (Exception e) {
            // Ignoring this exception for now. Once the download flow populates FileTracker,
            // we can remove this try-catch block
        }
    }

    public void testSnapshotWithNewTranslog() throws IOException {
        List<Closeable> toClose = new ArrayList<>();
        try {
            ArrayList<Translog.Operation> ops = new ArrayList<>();
            Translog.Snapshot snapshot = translog.newSnapshot();
            toClose.add(snapshot);
            assertThat(snapshot, SnapshotMatchers.size(0));

            addToTranslogAndList(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
            Translog.Snapshot snapshot1 = translog.newSnapshot();
            toClose.add(snapshot1);

            addToTranslogAndList(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[] { 2 }));

            assertThat(snapshot1, SnapshotMatchers.equalsTo(ops.get(0)));

            translog.rollGeneration();
            addToTranslogAndList(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[] { 3 }));

            Translog.Snapshot snapshot2 = translog.newSnapshot();
            toClose.add(snapshot2);
            translog.getDeletionPolicy().setLocalCheckpointOfSafeCommit(2);
            assertThat(snapshot2, containsOperationsInAnyOrder(ops));
            assertThat(snapshot2.totalOperations(), equalTo(ops.size()));
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public void testSnapshotOnClosedTranslog() throws IOException {
        assertTrue(Files.exists(translogDir.resolve(Translog.getFilename(1))));
        translog.add(new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        translog.close();
        AlreadyClosedException ex = expectThrows(AlreadyClosedException.class, () -> translog.newSnapshot());
        assertEquals(ex.getMessage(), "translog is already closed");
    }

    public void testRangeSnapshot() throws Exception {
        long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final int generations = between(2, 20);
        Map<Long, List<Translog.Operation>> operationsByGen = new HashMap<>();
        for (int gen = 0; gen < generations; gen++) {
            Set<Long> seqNos = new HashSet<>();
            int numOps = randomIntBetween(1, 100);
            for (int i = 0; i < numOps; i++) {
                final long seqNo = randomValueOtherThanMany(seqNos::contains, () -> randomLongBetween(0, 1000));
                minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
                seqNos.add(seqNo);
            }
            List<Translog.Operation> ops = new ArrayList<>(seqNos.size());
            for (long seqNo : seqNos) {
                Translog.Index op = new Translog.Index(randomAlphaOfLength(10), seqNo, primaryTerm.get(), new byte[] { randomByte() });
                translog.add(op);
                ops.add(op);
            }
            operationsByGen.put(translog.currentFileGeneration(), ops);
            translog.rollGeneration();
            if (rarely()) {
                translog.rollGeneration(); // empty generation
            }
        }

        if (minSeqNo > 0) {
            long fromSeqNo = randomLongBetween(0, minSeqNo - 1);
            long toSeqNo = randomLongBetween(fromSeqNo, minSeqNo - 1);
            try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
                assertThat(snapshot.totalOperations(), equalTo(0));
                assertNull(snapshot.next());
            }
        }

        long fromSeqNo = randomLongBetween(maxSeqNo + 1, Long.MAX_VALUE);
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
            assertThat(snapshot.totalOperations(), equalTo(0));
            assertNull(snapshot.next());
        }

        fromSeqNo = randomLongBetween(0, 2000);
        toSeqNo = randomLongBetween(fromSeqNo, 2000);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo)) {
            Set<Long> seenSeqNos = new HashSet<>();
            List<Translog.Operation> expectedOps = new ArrayList<>();
            for (long gen = translog.currentFileGeneration(); gen > 0; gen--) {
                for (Translog.Operation op : operationsByGen.getOrDefault(gen, Collections.emptyList())) {
                    if (fromSeqNo <= op.seqNo() && op.seqNo() <= toSeqNo && seenSeqNos.add(op.seqNo())) {
                        expectedOps.add(op);
                    }
                }
            }
            assertThat(TestTranslog.drainSnapshot(snapshot, false), equalTo(expectedOps));
        }
    }

    public void testSimpleOperationsUpload() throws Exception {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertThat(snapshot.totalOperations(), equalTo(ops.size()));
        }

        assertEquals(4, translog.allUploaded().size());

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(6, translog.allUploaded().size());

        translog.rollGeneration();
        assertEquals(6, translog.allUploaded().size());

        Set<String> mdFiles = blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR));
        assertEquals(2, mdFiles.size());
        logger.info("All md files {}", mdFiles);

        Set<String> tlogFiles = blobStoreTransferService.listAll(
            getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))
        );
        logger.info("All data files {}", tlogFiles);

        // assert content of ckp and tlog files
        BlobPath path = getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()));
        for (TranslogReader reader : translog.readers) {
            final long readerGeneration = reader.getGeneration();
            logger.error("Asserting content of {}", readerGeneration);
            Path translogPath = reader.path();
            try (
                InputStream stream = new CheckedInputStream(Files.newInputStream(translogPath), new CRC32());
                InputStream tlogStream = blobStoreTransferService.downloadBlob(path, Translog.getFilename(readerGeneration));
            ) {
                byte[] content = stream.readAllBytes();
                byte[] tlog = tlogStream.readAllBytes();
                assertArrayEquals(tlog, content);
            }

            Path checkpointPath = translog.location().resolve(Translog.getCommitCheckpointFileName(readerGeneration));
            try (
                CheckedInputStream stream = new CheckedInputStream(Files.newInputStream(checkpointPath), new CRC32());
                InputStream ckpStream = blobStoreTransferService.downloadBlob(path, Translog.getCommitCheckpointFileName(readerGeneration))
            ) {
                byte[] content = stream.readAllBytes();
                byte[] ckp = ckpStream.readAllBytes();
                assertArrayEquals(ckp, content);
            }
        }

        // expose the new checkpoint (simulating a commit), before we trim the translog
        // simulating the remote segment upload .
        translog.setMinSeqNoToKeep(0);
        // This should not trim anything from local
        translog.trimUnreferencedReaders();
        assertEquals(2, translog.readers.size());
        assertBusy(() -> {
            assertEquals(4, translog.allUploaded().size());
            assertEquals(
                4,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        // This should trim tlog-2 from local
        // This should not trim tlog-2.* files from remote as we not uploading any more translog to remote
        translog.setMinSeqNoToKeep(1);
        translog.trimUnreferencedReaders();
        assertEquals(1, translog.readers.size());
        assertBusy(() -> {
            assertEquals(4, translog.allUploaded().size());
            assertEquals(
                4,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        // this should now trim as tlog-2 files from remote, but not tlog-3 and tlog-4
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 2, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(2, translog.stats().estimatedNumberOfOperations());

        translog.setMinSeqNoToKeep(2);

        translog.trimUnreferencedReaders();
        assertEquals(1, translog.readers.size());
        assertEquals(1, translog.stats().estimatedNumberOfOperations());
        assertBusy(() -> assertEquals(4, translog.allUploaded().size()));
    }

    public void testMetadataFileDeletion() throws Exception {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        // Test deletion of metadata files
        int numDocs = randomIntBetween(6, 10);
        for (int i = 0; i < numDocs; i++) {
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(i), i, primaryTerm.get(), new byte[] { 1 }));
            translog.setMinSeqNoToKeep(i);
            translog.trimUnreferencedReaders();
            assertEquals(1, translog.readers.size());
        }
        assertBusy(() -> assertEquals(4, translog.allUploaded().size()));
        assertBusy(() -> assertEquals(1, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size()));
        int moreDocs = randomIntBetween(3, 10);
        logger.info("numDocs={} moreDocs={}", numDocs, moreDocs);
        for (int i = numDocs; i < numDocs + moreDocs; i++) {
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(i), i, primaryTerm.get(), new byte[] { 1 }));
        }
        translog.trimUnreferencedReaders();
        assertEquals(1 + moreDocs, translog.readers.size());
        assertBusy(() -> assertEquals(2 + 2L * moreDocs, translog.allUploaded().size()));
        assertBusy(() -> assertEquals(1, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size()));

        int totalDocs = numDocs + moreDocs;
        translog.setMinSeqNoToKeep(totalDocs - 1);
        translog.trimUnreferencedReaders();

        addToTranslogAndListAndUpload(
            translog,
            ops,
            new Translog.Index(String.valueOf(totalDocs), totalDocs, primaryTerm.get(), new byte[] { 1 })
        );
        translog.setMinSeqNoToKeep(totalDocs);
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertEquals(1, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size()));

        // Change primary term and test the deletion of older primaries
        String translogUUID = translog.translogUUID;
        try {
            translog.getDeletionPolicy().assertNoOpenTranslogRefs();
            translog.close();
        } finally {
            terminate(threadPool);
        }

        // Increase primary term
        long oldPrimaryTerm = primaryTerm.get();
        long newPrimaryTerm = primaryTerm.incrementAndGet();

        // Creating RemoteFsTranslog with the same location
        Translog newTranslog = create(translogDir, repository, translogUUID);
        int newPrimaryTermDocs = randomIntBetween(5, 10);
        for (int i = totalDocs + 1; i <= totalDocs + newPrimaryTermDocs; i++) {
            addToTranslogAndListAndUpload(newTranslog, ops, new Translog.Index(String.valueOf(i), i, primaryTerm.get(), new byte[] { 1 }));
            // newTranslog.deletionPolicy.setLocalCheckpointOfSafeCommit(i - 1);
            newTranslog.setMinSeqNoToKeep(i);
            newTranslog.trimUnreferencedReaders();
        }

        try {
            newTranslog.close();
        } catch (Exception e) {
            // Ignoring this exception for now. Once the download flow populates FileTracker,
            // we can remove this try-catch block
        }
    }

    private BlobPath getTranslogDirectory() {
        return repository.basePath().add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())).add(TRANSLOG);
    }

    private Long populateTranslogOps(boolean withMissingOps) throws IOException {
        long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final int generations = between(2, 20);
        long currentSeqNo = 0L;
        List<Translog.Operation> firstGenOps = null;
        Map<Long, List<Translog.Operation>> operationsByGen = new HashMap<>();
        for (int gen = 0; gen < generations; gen++) {
            List<Long> seqNos = new ArrayList<>();
            int numOps = randomIntBetween(4, 10);
            for (int i = 0; i < numOps; i++, currentSeqNo++) {
                minSeqNo = SequenceNumbers.min(minSeqNo, currentSeqNo);
                maxSeqNo = SequenceNumbers.max(maxSeqNo, currentSeqNo);
                seqNos.add(currentSeqNo);
            }
            Collections.shuffle(seqNos, new Random(100));
            List<Translog.Operation> ops = new ArrayList<>(seqNos.size());
            for (long seqNo : seqNos) {
                Translog.Index op = new Translog.Index(randomAlphaOfLength(10), seqNo, primaryTerm.get(), new byte[] { randomByte() });
                boolean shouldAdd = !withMissingOps || seqNo % 4 != 0;
                if (shouldAdd) {
                    translog.add(op);
                    ops.add(op);
                }
            }
            operationsByGen.put(translog.currentFileGeneration(), ops);
            if (firstGenOps == null) {
                firstGenOps = ops;
            }
            translog.rollGeneration();
            if (rarely()) {
                translog.rollGeneration(); // empty generation
            }
        }
        return currentSeqNo;
    }

    public void testFullRangeSnapshot() throws Exception {
        // Successful snapshot
        long nextSeqNo = populateTranslogOps(false);
        long fromSeqNo = 0L;
        long toSeqNo = Math.min(nextSeqNo - 1, fromSeqNo + 15);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo, true)) {
            int totOps = 0;
            for (Translog.Operation op = snapshot.next(); op != null; op = snapshot.next()) {
                totOps++;
            }
            assertEquals(totOps, toSeqNo - fromSeqNo + 1);
        }
    }

    public void testFullRangeSnapshotWithFailures() throws Exception {
        long nextSeqNo = populateTranslogOps(true);
        long fromSeqNo = 0L;
        long toSeqNo = Math.min(nextSeqNo - 1, fromSeqNo + 15);
        try (Translog.Snapshot snapshot = translog.newSnapshot(fromSeqNo, toSeqNo, true)) {
            int totOps = 0;
            for (Translog.Operation op = snapshot.next(); op != null; op = snapshot.next()) {
                totOps++;
            }
            fail("Should throw exception for missing operations");
        } catch (MissingHistoryOperationsException e) {
            assertTrue(e.getMessage().contains("Not all operations between from_seqno"));
        }
    }

    public void testConcurrentWritesWithVaryingSize() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<TestTranslog.LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Exception[] threadExceptions = new Exception[threadCount];
        final AtomicLong seqNoGenerator = new AtomicLong();
        final CountDownLatch downLatch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new TranslogThread(
                translog,
                downLatch,
                opsPerThread,
                threadId,
                writtenOperations,
                seqNoGenerator,
                threadExceptions
            );
            threads[i].setDaemon(true);
            threads[i].start();
        }

        downLatch.countDown();

        for (int i = 0; i < threadCount; i++) {
            if (threadExceptions[i] != null) {
                throw threadExceptions[i];
            }
            threads[i].join(60 * 1000);
        }

        List<TestTranslog.LocationOperation> collect = new ArrayList<>(writtenOperations);
        collect.sort(Comparator.comparing(op -> op.operation.seqNo()));

        List<Translog.Operation> opsList = new ArrayList<>(threadCount * opsPerThread);
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            for (Translog.Operation op = snapshot.next(); op != null; op = snapshot.next()) {
                opsList.add(op);
            }
        }
        opsList.sort(Comparator.comparing(op -> op.seqNo()));

        for (int i = 0; i < threadCount * opsPerThread; i++) {
            assertEquals(opsList.get(i), collect.get(i).operation);
        }
    }

    /**
     * Tests that concurrent readers and writes maintain view and snapshot semantics
     */
    public void testConcurrentWriteViewsAndSnapshot() throws Throwable {
        final Thread[] writers = new Thread[randomIntBetween(1, 3)];
        final Thread[] readers = new Thread[randomIntBetween(1, 3)];
        final int flushEveryOps = randomIntBetween(5, 10);
        final int maxOps = randomIntBetween(20, 100);
        final Object signalReaderSomeDataWasIndexed = new Object();
        final AtomicLong idGenerator = new AtomicLong();
        final CyclicBarrier barrier = new CyclicBarrier(writers.length + readers.length + 1);

        // a map of all written ops and their returned location.
        final Map<Translog.Operation, Translog.Location> writtenOps = ConcurrentCollections.newConcurrentMap();

        // a signal for all threads to stop
        final AtomicBoolean run = new AtomicBoolean(true);

        final Object flushMutex = new Object();
        final AtomicLong lastCommittedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final LocalCheckpointTracker tracker = LocalCheckpointTrackerTests.createEmptyTracker();
        final TranslogDeletionPolicy deletionPolicy = translog.getDeletionPolicy();
        // any errors on threads
        final List<Exception> errors = new CopyOnWriteArrayList<>();
        logger.info("using [{}] readers. [{}] writers. flushing every ~[{}] ops.", readers.length, writers.length, flushEveryOps);
        for (int i = 0; i < writers.length; i++) {
            final String threadName = "writer_" + i;
            final int threadId = i;
            writers[i] = new Thread(new AbstractRunnable() {
                @Override
                public void doRun() throws BrokenBarrierException, InterruptedException, IOException {
                    barrier.await();
                    int counter = 0;
                    while (run.get() && idGenerator.get() < maxOps) {
                        long id = idGenerator.getAndIncrement();
                        final Translog.Operation op;
                        final Translog.Operation.Type type = Translog.Operation.Type.values()[((int) (id % Translog.Operation.Type
                            .values().length))];
                        switch (type) {
                            case CREATE:
                            case INDEX:
                                op = new Translog.Index("" + id, id, primaryTerm.get(), new byte[] { (byte) id });
                                break;
                            case DELETE:
                                op = new Translog.Delete(Long.toString(id), id, primaryTerm.get());
                                break;
                            case NO_OP:
                                op = new Translog.NoOp(id, 1, Long.toString(id));
                                break;
                            default:
                                throw new AssertionError("unsupported operation type [" + type + "]");
                        }
                        Translog.Location location = translog.add(op);
                        tracker.markSeqNoAsProcessed(id);
                        Translog.Location existing = writtenOps.put(op, location);
                        if (existing != null) {
                            fail("duplicate op [" + op + "], old entry at " + location);
                        }
                        if (id % writers.length == threadId) {
                            translog.ensureSynced(location);
                        }
                        if (id % flushEveryOps == 0) {
                            synchronized (flushMutex) {
                                // we need not do this concurrently as we need to make sure that the generation
                                // we're committing - is still present when we're committing
                                long localCheckpoint = tracker.getProcessedCheckpoint();
                                translog.rollGeneration();
                                // expose the new checkpoint (simulating a commit), before we trim the translog
                                lastCommittedLocalCheckpoint.set(localCheckpoint);
                                // deletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpoint);
                                translog.setMinSeqNoToKeep(localCheckpoint + 1);
                                translog.trimUnreferencedReaders();
                            }
                        }
                        if (id % 7 == 0) {
                            synchronized (signalReaderSomeDataWasIndexed) {
                                signalReaderSomeDataWasIndexed.notifyAll();
                            }
                        }
                        counter++;
                    }
                    logger.info("--> [{}] done. wrote [{}] ops.", threadName, counter);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("--> writer [{}] had an error", threadName), e);
                    errors.add(e);
                }
            }, threadName);
            writers[i].start();
        }

        for (int i = 0; i < readers.length; i++) {
            final String threadId = "reader_" + i;
            readers[i] = new Thread(new AbstractRunnable() {
                Closeable retentionLock = null;
                long committedLocalCheckpointAtView;

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> new ParameterizedMessage("--> reader [{}] had an error", threadId), e);
                    errors.add(e);
                    try {
                        closeRetentionLock();
                    } catch (IOException inner) {
                        inner.addSuppressed(e);
                        logger.error("unexpected error while closing view, after failure", inner);
                    }
                }

                void closeRetentionLock() throws IOException {
                    if (retentionLock != null) {
                        retentionLock.close();
                    }
                }

                void acquireRetentionLock() throws IOException {
                    closeRetentionLock();
                    retentionLock = translog.acquireRetentionLock();
                    // captures the last committed checkpoint, while holding the view, simulating
                    // recovery logic which captures a view and gets a lucene commit
                    committedLocalCheckpointAtView = lastCommittedLocalCheckpoint.get();
                    logger.info("--> [{}] min gen after acquiring lock [{}]", threadId, translog.getMinFileGeneration());
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    int iter = 0;
                    while (idGenerator.get() < maxOps) {
                        if (iter++ % 10 == 0) {
                            acquireRetentionLock();
                        }

                        // captures al views that are written since the view was created (with a small caveat see bellow)
                        // these are what we expect the snapshot to return (and potentially some more).
                        Set<Translog.Operation> expectedOps = new HashSet<>(writtenOps.keySet());
                        expectedOps.removeIf(op -> op.seqNo() <= committedLocalCheckpointAtView);
                        try (Translog.Snapshot snapshot = translog.newSnapshot(committedLocalCheckpointAtView + 1L, Long.MAX_VALUE)) {
                            Translog.Operation op;
                            while ((op = snapshot.next()) != null) {
                                expectedOps.remove(op);
                            }
                        }
                        if (expectedOps.isEmpty() == false) {
                            StringBuilder missed = new StringBuilder("missed ").append(expectedOps.size())
                                .append(" operations from [")
                                .append(committedLocalCheckpointAtView + 1L)
                                .append("]");
                            boolean failed = false;
                            for (Translog.Operation expectedOp : expectedOps) {
                                final Translog.Location loc = writtenOps.get(expectedOp);
                                failed = true;
                                missed.append("\n --> [").append(expectedOp).append("] written at ").append(loc);
                            }
                            if (failed) {
                                fail(missed.toString());
                            }
                        }
                        // slow down things a bit and spread out testing..
                        synchronized (signalReaderSomeDataWasIndexed) {
                            if (idGenerator.get() < maxOps) {
                                signalReaderSomeDataWasIndexed.wait();
                            }
                        }
                    }
                    closeRetentionLock();
                    logger.info("--> [{}] done. tested [{}] snapshots", threadId, iter);
                }
            }, threadId);
            readers[i].start();
        }

        barrier.await();
        logger.debug("--> waiting for threads to stop");
        for (Thread thread : writers) {
            thread.join();
        }
        logger.debug("--> waiting for readers to stop");
        // force stopping, if all writers crashed
        synchronized (signalReaderSomeDataWasIndexed) {
            idGenerator.set(Long.MAX_VALUE);
            signalReaderSomeDataWasIndexed.notifyAll();
        }
        for (Thread thread : readers) {
            thread.join();
        }
        if (errors.size() > 0) {
            Throwable e = errors.get(0);
            for (Throwable suppress : errors.subList(1, errors.size())) {
                e.addSuppressed(suppress);
            }
            throw e;
        }
        logger.info("--> test done. total ops written [{}]", writtenOps.size());
    }

    public void testSyncUpTo() throws IOException {
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            int seqNo = ++count;
            final Translog.Location location = translog.add(
                new Translog.Index("" + op, seqNo, primaryTerm.get(), Integer.toString(seqNo).getBytes(Charset.forName("UTF-8")))
            );
            if (randomBoolean()) {
                assertTrue("at least one operation pending", translog.syncNeeded());
                assertTrue("this operation has not been synced", translog.ensureSynced(location));
                // we are the last location so everything should be synced
                assertFalse("the last call to ensureSycned synced all previous ops", translog.syncNeeded());
                seqNo = ++count;
                translog.add(
                    new Translog.Index("" + op, seqNo, primaryTerm.get(), Integer.toString(seqNo).getBytes(Charset.forName("UTF-8")))
                );
                assertTrue("one pending operation", translog.syncNeeded());
                assertFalse("this op has been synced before", translog.ensureSynced(location)); // not syncing now
                assertTrue("we only synced a previous operation yet", translog.syncNeeded());
            }
            if (rarely()) {
                translog.rollGeneration();
                assertFalse("location is from a previous translog - already synced", translog.ensureSynced(location)); // not syncing now
                assertFalse("no sync needed since no operations in current translog", translog.syncNeeded());
            }

            if (randomBoolean()) {
                translog.sync();
                assertFalse("translog has been synced already", translog.ensureSynced(location));
            }
        }
    }

    public void testSyncUpFailure() throws IOException {
        int translogOperations = randomIntBetween(1, 20);
        int count = 0;
        fail.failAlways();
        ArrayList<Translog.Location> locations = new ArrayList<>();
        for (int op = 0; op < translogOperations; op++) {
            int seqNo = ++count;
            final Translog.Location location = translog.add(
                new Translog.Index("" + op, seqNo, primaryTerm.get(), Integer.toString(seqNo).getBytes(Charset.forName("UTF-8")))
            );
            if (randomBoolean()) {
                fail.failAlways();
                try {
                    translog.ensureSynced(location);
                    fail("io exception expected");
                } catch (IOException e) {
                    assertTrue("at least one operation pending", translog.syncNeeded());
                }
            } else {
                fail.failNever();
                translog.ensureSynced(location);
                assertFalse("no sync needed since no operations in current translog", translog.syncNeeded());
            }
            locations.add(location);

        }
        // clean up
        fail.failNever();

        // writes should get synced up now
        translog.sync();
        assertFalse(translog.syncNeeded());
        for (Translog.Location location : locations) {
            assertFalse("all of the locations should be synced: " + location, translog.ensureSynced(location));
        }

    }

    public void testSyncUpToStream() throws IOException {
        int iters = randomIntBetween(5, 10);
        for (int i = 0; i < iters; i++) {
            int translogOperations = randomIntBetween(10, 100);
            int count = 0;
            ArrayList<Translog.Location> locations = new ArrayList<>();
            for (int op = 0; op < translogOperations; op++) {
                if (rarely()) {
                    translog.rollGeneration();
                }
                final Translog.Location location = translog.add(
                    new Translog.Index("" + op, op, primaryTerm.get(), Integer.toString(++count).getBytes(Charset.forName("UTF-8")))
                );
                locations.add(location);
            }
            Collections.shuffle(locations, random());
            if (randomBoolean()) {
                assertTrue("at least one operation pending", translog.syncNeeded());
                assertTrue("this operation has not been synced", translog.ensureSynced(locations.stream()));
                // we are the last location so everything should be synced
                assertFalse("the last call to ensureSycned synced all previous ops", translog.syncNeeded());
            } else if (rarely()) {
                translog.rollGeneration();
                // not syncing now
                assertFalse("location is from a previous translog - already synced", translog.ensureSynced(locations.stream()));
                assertFalse("no sync needed since no operations in current translog", translog.syncNeeded());
            } else {
                translog.sync();
                assertFalse("translog has been synced already", translog.ensureSynced(locations.stream()));
            }
            for (Translog.Location location : locations) {
                assertFalse("all of the locations should be synced: " + location, translog.ensureSynced(location));
            }
        }
    }

    public void testLocationComparison() throws IOException {
        List<Translog.Location> locations = new ArrayList<>();
        int translogOperations = randomIntBetween(10, 100);
        int count = 0;
        for (int op = 0; op < translogOperations; op++) {
            locations.add(
                translog.add(
                    new Translog.Index("" + op, op, primaryTerm.get(), Integer.toString(++count).getBytes(Charset.forName("UTF-8")))
                )
            );
            if (randomBoolean()) {
                translog.ensureSynced(locations.get(op));
            }
            if (rarely() && translogOperations > op + 1) {
                translog.rollGeneration();
            }
        }
        Collections.shuffle(locations, random());
        Translog.Location max = locations.get(0);
        for (Translog.Location location : locations) {
            max = max(max, location);
        }

        try (Translog.Snapshot snap = new TestTranslog.SortedSnapshot(translog.newSnapshot())) {
            Translog.Operation next;
            Translog.Operation maxOp = null;
            while ((next = snap.next()) != null) {
                maxOp = next;
            }
            assertNotNull(maxOp);
            assertEquals(maxOp.getSource().source.utf8ToString(), Integer.toString(count));
        }
    }

    public static Translog.Location max(Translog.Location a, Translog.Location b) {
        if (a.compareTo(b) > 0) {
            return a;
        }
        return b;
    }

    public void testTranslogWriter() throws IOException {
        final TranslogWriter writer = translog.createWriter(translog.currentFileGeneration() + 1);
        final Set<Long> persistedSeqNos = new HashSet<>();
        persistedSeqNoConsumer.set(persistedSeqNos::add);
        final int numOps = scaledRandomIntBetween(8, 250000);
        final Set<Long> seenSeqNos = new HashSet<>();
        boolean opsHaveValidSequenceNumbers = randomBoolean();
        for (int i = 0; i < numOps; i++) {
            byte[] bytes = new byte[4];
            DataOutput out = EndiannessReverserUtil.wrapDataOutput(new ByteArrayDataOutput(bytes));
            out.writeInt(i);
            long seqNo;
            do {
                seqNo = opsHaveValidSequenceNumbers ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO;
                opsHaveValidSequenceNumbers = opsHaveValidSequenceNumbers || !rarely();
            } while (seenSeqNos.contains(seqNo));
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seenSeqNos.add(seqNo);
            }
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), seqNo);
        }
        assertThat(persistedSeqNos, empty());
        writer.sync();
        persistedSeqNos.remove(SequenceNumbers.UNASSIGNED_SEQ_NO);
        assertEquals(seenSeqNos, persistedSeqNos);

        final BaseTranslogReader reader = randomBoolean()
            ? writer
            : translog.openReader(writer.path(), Checkpoint.read(translog.location().resolve(Translog.CHECKPOINT_FILE_NAME)));
        for (int i = 0; i < numOps; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
            buffer.flip();
            final int value = buffer.getInt();
            assertEquals(i, value);
        }
        final long minSeqNo = seenSeqNos.stream().min(Long::compareTo).orElse(SequenceNumbers.NO_OPS_PERFORMED);
        final long maxSeqNo = seenSeqNos.stream().max(Long::compareTo).orElse(SequenceNumbers.NO_OPS_PERFORMED);
        assertThat(reader.getCheckpoint().minSeqNo, equalTo(minSeqNo));
        assertThat(reader.getCheckpoint().maxSeqNo, equalTo(maxSeqNo));

        byte[] bytes = new byte[4];
        DataOutput out = EndiannessReverserUtil.wrapDataOutput(new ByteArrayDataOutput(bytes));
        out.writeInt(2048);
        writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), randomNonNegativeLong());

        if (reader instanceof TranslogReader) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            try {
                reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * numOps);
                fail("read past EOF?");
            } catch (EOFException ex) {
                // expected
            }
            ((TranslogReader) reader).close();
        } else {
            // live reader!
            ByteBuffer buffer = ByteBuffer.allocate(4);
            final long pos = reader.getFirstOperationOffset() + 4 * numOps;
            reader.readBytes(buffer, pos);
            buffer.flip();
            final int value = buffer.getInt();
            assertEquals(2048, value);
        }
        IOUtils.close(writer);
    }

    public void testTranslogWriterCanFlushInAddOrReadCall() throws IOException {
        Path tempDir = createTempDir();
        final TranslogConfig temp = getTranslogConfig(tempDir);
        final TranslogConfig config = new TranslogConfig(
            temp.getShardId(),
            temp.getTranslogPath(),
            temp.getIndexSettings(),
            temp.getBigArrays(),
            new ByteSizeValue(1, ByteSizeUnit.KB)
        );

        final Set<Long> persistedSeqNos = new HashSet<>();
        final AtomicInteger writeCalls = new AtomicInteger();

        final ChannelFactory channelFactory = (file, openOption) -> {
            FileChannel delegate = FileChannel.open(file, openOption);
            boolean success = false;
            try {
                // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp");

                final FileChannel channel;
                if (isCkpFile) {
                    channel = delegate;
                } else {
                    channel = new FilterFileChannel(delegate) {

                        @Override
                        public int write(ByteBuffer src) throws IOException {
                            writeCalls.incrementAndGet();
                            return super.write(src);
                        }
                    };
                }
                success = true;
                return channel;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(delegate);
                }
            }
        };

        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            channelFactory,
            primaryTerm.get()
        );

        try (
            Translog translog = new RemoteFsTranslog(
                config,
                translogUUID,
                new DefaultTranslogDeletionPolicy(-1, -1, 0),
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                primaryTerm::get,
                persistedSeqNos::add,
                repository,
                threadPool,
                () -> Boolean.TRUE
            ) {
                @Override
                ChannelFactory getChannelFactory() {
                    return channelFactory;
                }
            }
        ) {
            TranslogWriter writer = translog.getCurrent();
            int initialWriteCalls = writeCalls.get();
            byte[] bytes = new byte[256];
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 1);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 2);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 3);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 4);
            assertThat(persistedSeqNos, empty());
            assertEquals(initialWriteCalls, writeCalls.get());

            if (randomBoolean()) {
                // Since the buffer is full, this will flush before performing the add.
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 5);
                assertThat(persistedSeqNos, empty());
                assertThat(writeCalls.get(), greaterThan(initialWriteCalls));
            } else {
                // Will flush on read
                writer.readBytes(ByteBuffer.allocate(256), 0);
                assertThat(persistedSeqNos, empty());
                assertThat(writeCalls.get(), greaterThan(initialWriteCalls));

                // Add after we the read flushed the buffer
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 5);
            }

            writer.sync();

            // Sequence numbers are marked as persisted after sync
            assertThat(persistedSeqNos, contains(1L, 2L, 3L, 4L, 5L));
        }
    }

    public void testTranslogWriterFsyncDisabledInRemoteFsTranslog() throws IOException {
        Path tempDir = createTempDir();
        final TranslogConfig temp = getTranslogConfig(tempDir);
        final TranslogConfig config = new TranslogConfig(
            temp.getShardId(),
            temp.getTranslogPath(),
            temp.getIndexSettings(),
            temp.getBigArrays(),
            new ByteSizeValue(1, ByteSizeUnit.KB)
        );

        final Set<Long> persistedSeqNos = new HashSet<>();
        final AtomicInteger translogFsyncCalls = new AtomicInteger();
        final AtomicInteger checkpointFsyncCalls = new AtomicInteger();

        final ChannelFactory channelFactory = (file, openOption) -> {
            FileChannel delegate = FileChannel.open(file, openOption);
            boolean success = false;
            try {
                // don't do partial writes for checkpoints we rely on the fact that the bytes are written as an atomic operation
                final boolean isCkpFile = file.getFileName().toString().endsWith(".ckp");

                final FileChannel channel;
                if (isCkpFile) {
                    channel = new FilterFileChannel(delegate) {
                        @Override
                        public void force(boolean metaData) throws IOException {
                            checkpointFsyncCalls.incrementAndGet();
                        }
                    };
                } else {
                    channel = new FilterFileChannel(delegate) {

                        @Override
                        public void force(boolean metaData) throws IOException {
                            translogFsyncCalls.incrementAndGet();
                        }
                    };
                }
                success = true;
                return channel;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(delegate);
                }
            }
        };

        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            channelFactory,
            primaryTerm.get()
        );

        try (
            Translog translog = new RemoteFsTranslog(
                config,
                translogUUID,
                new DefaultTranslogDeletionPolicy(-1, -1, 0),
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                primaryTerm::get,
                persistedSeqNos::add,
                repository,
                threadPool,
                () -> Boolean.TRUE
            ) {
                @Override
                ChannelFactory getChannelFactory() {
                    return channelFactory;
                }
            }
        ) {
            TranslogWriter writer = translog.getCurrent();
            byte[] bytes = new byte[256];
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 1);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 2);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 3);
            writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), 4);
            writer.sync();
            // Fsync is still enabled during empty translog creation.
            assertEquals(2, checkpointFsyncCalls.get());
            assertEquals(1, translogFsyncCalls.get());
            // Sequence numbers are marked as persisted after sync
            assertThat(persistedSeqNos, contains(1L, 2L, 3L, 4L));
        }
    }

    public void testCloseIntoReader() throws IOException {
        try (TranslogWriter writer = translog.createWriter(translog.currentFileGeneration() + 1)) {
            final int numOps = randomIntBetween(8, 128);
            for (int i = 0; i < numOps; i++) {
                final byte[] bytes = new byte[4];
                final DataOutput out = EndiannessReverserUtil.wrapDataOutput(new ByteArrayDataOutput(bytes));
                out.writeInt(i);
                writer.add(ReleasableBytesReference.wrap(new BytesArray(bytes)), randomNonNegativeLong());
            }
            writer.sync();
            final Checkpoint writerCheckpoint = writer.getCheckpoint();
            TranslogReader reader = writer.closeIntoReader();
            try {
                if (randomBoolean()) {
                    reader.close();
                    reader = translog.openReader(reader.path(), writerCheckpoint);
                }
                for (int i = 0; i < numOps; i++) {
                    final ByteBuffer buffer = ByteBuffer.allocate(4);
                    reader.readBytes(buffer, reader.getFirstOperationOffset() + 4 * i);
                    buffer.flip();
                    final int value = buffer.getInt();
                    assertEquals(i, value);
                }
                final Checkpoint readerCheckpoint = reader.getCheckpoint();
                assertThat(readerCheckpoint, equalTo(writerCheckpoint));
            } finally {
                IOUtils.close(reader);
            }
        }
    }

    public class ThrowingBlobRepository extends FsRepository {
        private final Environment environment;

        private TestTranslog.FailSwitch fail;

        public ThrowingBlobRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings,
            TestTranslog.FailSwitch fail
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
            this.environment = environment;
            this.fail = fail;
        }

        protected BlobStore createBlobStore() throws Exception {
            final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
            final Path locationFile = environment.resolveRepoFile(location);
            return new ThrowingBlobStore(bufferSize, locationFile, isReadOnly(), fail);
        }
    }

    private class ThrowingBlobStore extends FsBlobStore {

        private TestTranslog.FailSwitch fail;

        public ThrowingBlobStore(int bufferSizeInBytes, Path path, boolean readonly, TestTranslog.FailSwitch fail) throws IOException {
            super(bufferSizeInBytes, path, readonly);
            this.fail = fail;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            try {
                return new ThrowingBlobContainer(this, path, buildAndCreate(path), fail);
            } catch (IOException ex) {
                throw new OpenSearchException("failed to create blob container", ex);
            }
        }
    }

    private class ThrowingBlobContainer extends FsBlobContainer {

        private TestTranslog.FailSwitch fail;

        public ThrowingBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, TestTranslog.FailSwitch fail) {
            super(blobStore, blobPath, path);
            this.fail = fail;
        }

        public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            if (fail.fail()) {
                throw new IOException("blob container throwing error");
            }
            super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    class TranslogThread extends Thread {
        private final CountDownLatch downLatch;
        private final int opsPerThread;
        private final int threadId;
        private final Collection<TestTranslog.LocationOperation> writtenOperations;
        private final Exception[] threadExceptions;
        private final Translog translog;
        private final AtomicLong seqNoGenerator;

        TranslogThread(
            Translog translog,
            CountDownLatch downLatch,
            int opsPerThread,
            int threadId,
            Collection<TestTranslog.LocationOperation> writtenOperations,
            AtomicLong seqNoGenerator,
            Exception[] threadExceptions
        ) {
            this.translog = translog;
            this.downLatch = downLatch;
            this.opsPerThread = opsPerThread;
            this.threadId = threadId;
            this.writtenOperations = writtenOperations;
            this.seqNoGenerator = seqNoGenerator;
            this.threadExceptions = threadExceptions;
        }

        @Override
        public void run() {
            try {
                downLatch.await();
                for (int opCount = 0; opCount < opsPerThread; opCount++) {
                    Translog.Operation op;
                    final Translog.Operation.Type type = randomFrom(Translog.Operation.Type.values());
                    switch (type) {
                        case CREATE:
                        case INDEX:
                            op = new Translog.Index(
                                threadId + "_" + opCount,
                                seqNoGenerator.getAndIncrement(),
                                primaryTerm.get(),
                                randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8")
                            );
                            break;
                        case DELETE:
                            op = new Translog.Delete(
                                threadId + "_" + opCount,
                                seqNoGenerator.getAndIncrement(),
                                primaryTerm.get(),
                                1 + randomInt(100000)
                            );
                            break;
                        case NO_OP:
                            op = new Translog.NoOp(seqNoGenerator.getAndIncrement(), primaryTerm.get(), randomAlphaOfLength(16));
                            break;
                        default:
                            throw new AssertionError("unsupported operation type [" + type + "]");
                    }

                    Translog.Location loc = add(op);
                    writtenOperations.add(new TestTranslog.LocationOperation(op, loc));
                    if (rarely()) { // lets verify we can concurrently read this
                        assertEquals(op, translog.readOperation(loc));
                    }
                    afterAdd();
                }
            } catch (Exception t) {
                threadExceptions[threadId] = t;
            }
        }

        protected Translog.Location add(Translog.Operation op) throws IOException {
            Translog.Location location = translog.add(op);
            if (randomBoolean()) {
                translog.ensureSynced(location);
            }
            return location;
        }

        protected void afterAdd() {}
    }

}

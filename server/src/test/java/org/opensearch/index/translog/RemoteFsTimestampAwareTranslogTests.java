/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.index.translog.transfer.TranslogUploadFailedException;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.mockito.Mockito;

import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class RemoteFsTimestampAwareTranslogTests extends RemoteFsTranslogTests {

    Runnable updatePinnedTimstampTask;
    BlobContainer blobContainer;
    RemoteStorePinnedTimestampService remoteStorePinnedTimestampServiceSpy;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(
            Settings.builder().put(CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        Supplier<RepositoriesService> repositoriesServiceSupplier = mock(Supplier.class);
        Settings settings = Settings.builder()
            .put(Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "remote-repo")
            .build();
        RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        when(repositoriesService.repository("remote-repo")).thenReturn(blobStoreRepository);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.schedule(any(), any(), any())).then(invocationOnMock -> {
            Runnable updateTask = invocationOnMock.getArgument(0);
            updatePinnedTimstampTask = () -> {
                long currentTime = System.currentTimeMillis();
                while (RemoteStorePinnedTimestampService.getPinnedTimestamps().v1() < currentTime) {
                    updateTask.run();
                }
            };
            return null;
        }).then(subsequentInvocationsOnMock -> null);

        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = new RemoteStorePinnedTimestampService(
            repositoriesServiceSupplier,
            settings,
            threadPool,
            clusterService
        );
        remoteStorePinnedTimestampServiceSpy = Mockito.spy(remoteStorePinnedTimestampService);

        BlobStore blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        when(blobStoreRepository.basePath()).thenReturn(new BlobPath());
        blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);

        when(blobContainer.listBlobs()).thenReturn(new HashMap<>());

        remoteStorePinnedTimestampServiceSpy.start();
    }

    @Override
    protected RemoteFsTranslog createTranslogInstance(
        TranslogConfig translogConfig,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy
    ) throws IOException {
        return new RemoteFsTimestampAwareTranslog(
            translogConfig,
            translogUUID,
            deletionPolicy,
            () -> globalCheckpoint.get(),
            primaryTerm::get,
            getPersistedSeqNoConsumer(),
            repository,
            threadPool,
            primaryMode::get,
            new RemoteTranslogTransferTracker(shardId, 10),
            DefaultRemoteStoreSettings.INSTANCE
        );
    }

    @Override
    public void testSyncUpAlwaysFailure() throws IOException {
        int translogOperations = randomIntBetween(1, 20);
        int count = 0;
        fail.failAlways();
        for (int op = 0; op < translogOperations; op++) {
            translog.add(
                new Translog.Index(String.valueOf(op), count, primaryTerm.get(), Integer.toString(count).getBytes(StandardCharsets.UTF_8))
            );
            try {
                translog.sync();
                fail("io exception expected");
            } catch (TranslogUploadFailedException e) {
                assertTrue("at least one operation pending", translog.syncNeeded());
            }
        }
        assertTrue(translog.isOpen());
        fail.failNever();
        translog.sync();
    }

    public void testGetMinMaxTranslogGenerationFromFilename() throws Exception {
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        ArrayList<Translog.Operation> ops = new ArrayList<>();

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("0", 0, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 2, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("3", 3, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("4", 4, primaryTerm.get(), new byte[] { 1 }));

        CountDownLatch latch = new CountDownLatch(1);
        blobStoreTransferService.listAllInSortedOrder(
            getTranslogDirectory().add(METADATA_DIR),
            "metadata",
            Integer.MAX_VALUE,
            new LatchedActionListener<>(new ActionListener<List<BlobMetadata>>() {
                @Override
                public void onResponse(List<BlobMetadata> blobMetadataList) {
                    Long minGen = 1L;
                    Long maxGen = 6L;
                    for (BlobMetadata blobMetadata : blobMetadataList) {
                        Tuple<Long, Long> minMaxGen = TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(blobMetadata.name());
                        assertEquals(minGen, minMaxGen.v1());
                        assertEquals(maxGen, minMaxGen.v2());
                        maxGen -= 1;
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // This means test failure
                    fail();
                }
            }, latch)
        );
        latch.await();

        // Old format metadata file
        String oldFormatMdFilename = "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__1";
        assertNull(TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(oldFormatMdFilename));

        // Node id containing separator
        String nodeIdWithSeparator =
            "metadata__9223372036438563903__9223372036854774799__9223370311919910393__node__1__9223372036438563958__2__1";
        Tuple<Long, Long> minMaxGen = TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(nodeIdWithSeparator);
        Long minGen = Long.MAX_VALUE - 9223372036438563958L;
        assertEquals(minGen, minMaxGen.v1());

        // Malformed md filename
        String malformedMdFileName = "metadata__9223372036438563903__9223372036854774799__9223370311919910393__node1__xyz__3__1";
        assertNull(TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(malformedMdFileName));
    }

    public void testGetMinMaxPrimaryTermFromFilename() throws Exception {
        // New format metadata file
        String newFormatMetadataFile =
            "metadata__9223372036854775800__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1";
        Tuple<Long, Long> minMaxPrimaryterm = TranslogTransferMetadata.getMinMaxPrimaryTermFromFilename(newFormatMetadataFile);
        Long minPrimaryTerm = 2L;
        Long maxPrimaryTerm = 7L;
        assertEquals(minPrimaryTerm, minMaxPrimaryterm.v1());
        assertEquals(maxPrimaryTerm, minMaxPrimaryterm.v2());

        // Old format metadata file
        String oldFormatMdFilename = "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__1";
        assertNull(TranslogTransferMetadata.getMinMaxPrimaryTermFromFilename(oldFormatMdFilename));

        // Node id containing separator
        String nodeIdWithSeparator =
            "metadata__9223372036854775800__9223372036854774799__9223370311919910393__node__1__9223372036438563958__2__1";
        minMaxPrimaryterm = TranslogTransferMetadata.getMinMaxPrimaryTermFromFilename(nodeIdWithSeparator);
        minPrimaryTerm = 2L;
        maxPrimaryTerm = 7L;
        assertEquals(minPrimaryTerm, minMaxPrimaryterm.v1());
        assertEquals(maxPrimaryTerm, minMaxPrimaryterm.v2());

        // Malformed md filename
        String malformedMdFileName = "metadata__9223372036854775800__9223372036854774799__9223370311919910393__node1__xyz__3qwe__1";
        assertNull(TranslogTransferMetadata.getMinMaxPrimaryTermFromFilename(malformedMdFileName));
    }

    public void testIndexDeletionWithNoPinnedTimestampNoRecentMdFiles() throws Exception {
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        ArrayList<Translog.Operation> ops = new ArrayList<>();

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("0", 0, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 2, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("3", 3, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("4", 4, primaryTerm.get(), new byte[] { 1 }));

        assertBusy(() -> {
            assertEquals(5, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
            assertEquals(
                12,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        updatePinnedTimstampTask.run();
        ((RemoteFsTimestampAwareTranslog) translog).trimUnreferencedReaders(true, false);

        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        assertBusy(() -> {
            assertEquals(0, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
            assertEquals(
                0,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });
    }

    public void testIndexDeletionWithNoPinnedTimestampButRecentFiles() throws Exception {
        ArrayList<Translog.Operation> ops = new ArrayList<>();

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("0", 0, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 2, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("3", 3, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("4", 4, primaryTerm.get(), new byte[] { 1 }));

        updatePinnedTimstampTask.run();
        ((RemoteFsTimestampAwareTranslog) translog).trimUnreferencedReaders(true, false);

        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertBusy(() -> {
            assertEquals(5, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
            assertEquals(
                12,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });
    }

    @Override
    public void testSimpleOperationsUpload() throws Exception {
        ArrayList<Translog.Operation> ops = new ArrayList<>();

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("0", 0, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));

        // First reader is created at the init of translog
        assertEquals(3, translog.readers.size());
        assertEquals(2, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
        assertBusy(() -> {
            assertEquals(6, translog.allUploaded().size());
            assertEquals(
                6,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 2, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("3", 3, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("4", 4, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("5", 5, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("6", 6, primaryTerm.get(), new byte[] { 1 }));

        assertBusy(() -> {
            assertEquals(
                16,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        // Fetch pinned timestamps so that it won't be stale
        updatePinnedTimstampTask.run();

        translog.setMinSeqNoToKeep(6);
        translog.trimUnreferencedReaders();
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("7", 7, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("8", 8, primaryTerm.get(), new byte[] { 1 }));
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        // Fetch pinned timestamps so that it won't be stale
        updatePinnedTimstampTask.run();
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        assertEquals(3, translog.readers.size());
        assertBusy(() -> {
            assertEquals(2, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
            assertEquals(6, translog.allUploaded().size());
            assertEquals(
                6,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        }, 60, TimeUnit.SECONDS);
    }

    @Override
    public void testMetadataFileDeletion() throws Exception {
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        // Test deletion of metadata files
        int numDocs = randomIntBetween(6, 10);
        for (int i = 0; i < numDocs; i++) {
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(i), i, primaryTerm.get(), new byte[] { 1 }));
            translog.setMinSeqNoToKeep(i);
            // Fetch pinned timestamps so that it won't be stale
            updatePinnedTimstampTask.run();
            translog.trimUnreferencedReaders();
            assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
            assertEquals(1, translog.readers.size());
        }
        assertBusy(() -> assertEquals(2, translog.allUploaded().size()));
        addToTranslogAndListAndUpload(
            translog,
            ops,
            new Translog.Index(String.valueOf(numDocs), numDocs, primaryTerm.get(), new byte[] { 1 })
        );
        addToTranslogAndListAndUpload(
            translog,
            ops,
            new Translog.Index(String.valueOf(numDocs + 1), numDocs + 1, primaryTerm.get(), new byte[] { 1 })
        );
        updatePinnedTimstampTask.run();
        translog.trimUnreferencedReaders();
        assertBusy(() -> { assertEquals(2, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size()); });
    }

    public void testMetadataFileDeletionWithPinnedTimestamps() throws Exception {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        // Test deletion of metadata files
        int numDocs = randomIntBetween(16, 20);
        for (int i = 0; i < numDocs; i++) {
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(i), i, primaryTerm.get(), new byte[] { 1 }));
            translog.setMinSeqNoToKeep(i);
            assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
            translog.trimUnreferencedReaders();
            // This is just to make sure that each metadata is at least 1ms apart
            Thread.sleep(1);
        }

        CountDownLatch latch = new CountDownLatch(1);
        blobStoreTransferService.listAllInSortedOrder(
            getTranslogDirectory().add(METADATA_DIR),
            "metadata",
            Integer.MAX_VALUE,
            new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(List<BlobMetadata> blobMetadataList) {
                    List<String> pinnedTimestampMatchingMetadataFiles = new ArrayList<>();
                    List<Long> pinnedTimestamps = new ArrayList<>();
                    for (BlobMetadata blobMetadata : blobMetadataList) {
                        String metadataFilename = blobMetadata.name();
                        if (randomBoolean()) {
                            long timestamp = RemoteStoreUtils.invertLong(metadataFilename.split(METADATA_SEPARATOR)[3]);
                            pinnedTimestamps.add(timestamp);
                            pinnedTimestampMatchingMetadataFiles.add(metadataFilename);
                        }
                    }

                    Map<String, BlobMetadata> pinnedTimestampsMap = new HashMap<>();
                    pinnedTimestamps.forEach(ts -> pinnedTimestampsMap.put(randomInt(1000) + "__" + ts, new PlainBlobMetadata("x", 100)));

                    try {

                        when(blobContainer.listBlobs()).thenReturn(pinnedTimestampsMap);

                        Set<String> dataFilesBeforeTrim = blobStoreTransferService.listAll(
                            getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))
                        );

                        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
                        updatePinnedTimstampTask.run();
                        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
                        translog.trimUnreferencedReaders();
                        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

                        Set<String> metadataFilesAfterTrim = blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR));
                        Set<String> dataFilesAfterTrim = blobStoreTransferService.listAll(
                            getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))
                        );

                        // If non pinned generations are within, minRemoteGenReferenced - 1 - indexSettings().getRemoteTranslogExtraKeep()
                        // we will not delete them
                        if (dataFilesAfterTrim.equals(dataFilesBeforeTrim) == false) {
                            // We check for number of pinned timestamp or +1 due to latest metadata.
                            assertTrue(
                                metadataFilesAfterTrim.size() == pinnedTimestamps.size()
                                    || metadataFilesAfterTrim.size() == pinnedTimestamps.size() + 1
                            );
                        }

                        for (String md : pinnedTimestampMatchingMetadataFiles) {
                            assertTrue(metadataFilesAfterTrim.contains(md));
                            Tuple<Long, Long> minMaXGen = TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(md);
                            for (long i = minMaXGen.v1(); i <= minMaXGen.v2(); i++) {
                                assertTrue(dataFilesAfterTrim.contains(Translog.getFilename(i)));
                            }
                        }
                    } catch (Exception e) {
                        fail();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            }, latch)
        );

        latch.await();
    }

    @Override
    public void testDrainSync() throws Exception {
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        // This test checks following scenarios -
        // 1. During ongoing uploads, the available permits are 0.
        // 2. During an upload, if drainSync is called, it will wait for it to acquire and available permits are 0.
        // 3. After drainSync, if trimUnreferencedReaders is attempted, we do not delete from remote store.
        // 4. After drainSync, if an upload is an attempted, we do not upload to remote store.
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        assertEquals(0, translog.allUploaded().size());
        assertEquals(1, translog.readers.size());

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(0), 0, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(4, translog.allUploaded().size());
        assertEquals(2, translog.readers.size());
        assertBusy(() -> assertEquals(1, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size()));

        translog.setMinSeqNoToKeep(0);
        // Fetch pinned timestamps so that it won't be stale
        updatePinnedTimstampTask.run();
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(1, translog.readers.size());

        // Case 1 - During ongoing uploads, the available permits are 0.
        slowDown.setSleepSeconds(2);
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread1 = new Thread(() -> {
            try {
                addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(1), 1, primaryTerm.get(), new byte[] { 1 }));
                assertEquals(2, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
                latch.countDown();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        thread1.start();
        assertBusy(() -> assertEquals(0, translog.availablePermits()));
        // Case 2 - During an upload, if drainSync is called, it will wait for it to acquire and available permits are 0.
        Releasable releasable = translog.drainSync();
        assertBusy(() -> assertEquals(0, latch.getCount()));
        assertEquals(0, translog.availablePermits());
        slowDown.setSleepSeconds(0);
        assertEquals(4, translog.allUploaded().size());
        assertEquals(2, translog.readers.size());
        Set<String> mdFiles = blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR));

        // Case 3 - After drainSync, if trimUnreferencedReaders is attempted, we do not delete from remote store.
        translog.setMinSeqNoToKeep(1);
        // Fetch pinned timestamps so that it won't be stale
        updatePinnedTimstampTask.run();
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(1, translog.readers.size());
        assertEquals(2, translog.allUploaded().size());
        assertEquals(mdFiles, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)));

        // Case 4 - After drainSync, if an upload is an attempted, we do not upload to remote store.
        Translog.Location loc = addToTranslogAndListAndUpload(
            translog,
            ops,
            new Translog.Index(String.valueOf(2), 2, primaryTerm.get(), new byte[] { 1 })
        );
        assertEquals(1, translog.readers.size());
        assertEquals(2, translog.allUploaded().size());
        assertEquals(mdFiles, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)));

        // Refill the permits back
        Releasables.close(releasable);
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index(String.valueOf(3), 3, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(2, translog.readers.size());
        assertEquals(4, translog.allUploaded().size());
        assertEquals(3, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());

        translog.setMinSeqNoToKeep(3);
        // Fetch pinned timestamps so that it won't be stale
        updatePinnedTimstampTask.run();
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(1, translog.readers.size());
        assertBusy(() -> assertEquals(2, translog.allUploaded().size()));
        assertBusy(() -> assertEquals(1, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size()));
    }

    @Override
    public void testExtraGenToKeep() throws Exception {
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);

        TranslogConfig config = getConfig(1);
        ChannelFactory channelFactory = getChannelFactory();
        final Set<Long> persistedSeqNos = new HashSet<>();
        String translogUUID = Translog.createEmptyTranslog(
            config.getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            channelFactory,
            primaryTerm.get()
        );
        TranslogDeletionPolicy deletionPolicy = createTranslogDeletionPolicy(config.getIndexSettings());
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (
            RemoteFsTranslog translog = new RemoteFsTranslog(
                config,
                translogUUID,
                deletionPolicy,
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                primaryTerm::get,
                persistedSeqNos::add,
                repository,
                threadPool,
                () -> Boolean.TRUE,
                new RemoteTranslogTransferTracker(shardId, 10),
                DefaultRemoteStoreSettings.INSTANCE
            ) {
                @Override
                ChannelFactory getChannelFactory() {
                    return channelFactory;
                }
            }
        ) {
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 1, primaryTerm.get(), new byte[] { 1 }));
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index("3", 2, primaryTerm.get(), new byte[] { 1 }));

            // expose the new checkpoint (simulating a commit), before we trim the translog
            translog.setMinSeqNoToKeep(2);

            // Trims from local
            // Fetch pinned timestamps so that it won't be stale
            updatePinnedTimstampTask.run();
            translog.trimUnreferencedReaders();
            assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

            addToTranslogAndListAndUpload(translog, ops, new Translog.Index("4", 3, primaryTerm.get(), new byte[] { 1 }));
            addToTranslogAndListAndUpload(translog, ops, new Translog.Index("5", 4, primaryTerm.get(), new byte[] { 1 }));
            // Trims from remote now
            // Fetch pinned timestamps so that it won't be stale
            updatePinnedTimstampTask.run();
            translog.trimUnreferencedReaders();
            assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
            assertEquals(
                8,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        }
    }

    public void testGetGenerationsToBeDeletedEmptyMetadataFilesNotToBeDeleted() throws IOException {
        List<String> metadataFilesNotToBeDeleted = new ArrayList<>();
        List<String> metadataFilesToBeDeleted = List.of(
            // 4 to 7
            "metadata__9223372036854775806__9223372036854775800__9223370311919910398__31__9223372036854775803__1__1",
            // 17 to 37
            "metadata__9223372036854775806__9223372036854775770__9223370311919910398__31__9223372036854775790__1__1",
            // 27 to 42
            "metadata__9223372036854775806__9223372036854775765__9223370311919910403__31__9223372036854775780__1__1"
        );
        Set<Long> generations = ((RemoteFsTimestampAwareTranslog) translog).getGenerationsToBeDeleted(
            metadataFilesNotToBeDeleted,
            metadataFilesToBeDeleted,
            Long.MAX_VALUE
        );
        Set<Long> md1Generations = LongStream.rangeClosed(4, 7).boxed().collect(Collectors.toSet());
        Set<Long> md2Generations = LongStream.rangeClosed(17, 37).boxed().collect(Collectors.toSet());
        Set<Long> md3Generations = LongStream.rangeClosed(27, 42).boxed().collect(Collectors.toSet());

        assertTrue(generations.containsAll(md1Generations));
        assertTrue(generations.containsAll(md2Generations));
        assertTrue(generations.containsAll(md3Generations));

        generations.removeAll(md1Generations);
        generations.removeAll(md2Generations);
        generations.removeAll(md3Generations);
        assertTrue(generations.isEmpty());
    }

    public void testGetGenerationsToBeDeleted() throws IOException {
        List<String> metadataFilesNotToBeDeleted = List.of(
            // 1 to 4
            "metadata__9223372036854775806__9223372036854775803__9223370311919910398__31__9223372036854775806__1__1",
            // 26 to 30
            "metadata__9223372036854775806__9223372036854775777__9223370311919910398__31__9223372036854775781__1__1",
            // 42 to 100
            "metadata__9223372036854775806__9223372036854775707__9223370311919910403__31__9223372036854775765__1__1"
        );
        List<String> metadataFilesToBeDeleted = List.of(
            // 4 to 7
            "metadata__9223372036854775806__9223372036854775800__9223370311919910398__31__9223372036854775803__1__1",
            // 17 to 37
            "metadata__9223372036854775806__9223372036854775770__9223370311919910398__31__9223372036854775790__1__1",
            // 27 to 42
            "metadata__9223372036854775806__9223372036854775765__9223370311919910403__31__9223372036854775780__1__1"
        );
        Set<Long> generations = ((RemoteFsTimestampAwareTranslog) translog).getGenerationsToBeDeleted(
            metadataFilesNotToBeDeleted,
            metadataFilesToBeDeleted,
            Long.MAX_VALUE
        );
        Set<Long> md1Generations = LongStream.rangeClosed(5, 7).boxed().collect(Collectors.toSet());
        Set<Long> md2Generations = LongStream.rangeClosed(17, 25).boxed().collect(Collectors.toSet());
        Set<Long> md3Generations = LongStream.rangeClosed(31, 41).boxed().collect(Collectors.toSet());

        assertTrue(generations.containsAll(md1Generations));
        assertTrue(generations.containsAll(md2Generations));
        assertTrue(generations.containsAll(md3Generations));

        generations.removeAll(md1Generations);
        generations.removeAll(md2Generations);
        generations.removeAll(md3Generations);
        assertTrue(generations.isEmpty());
    }

    public void testGetMetadataFilesToBeDeletedNoExclusion() {
        updatePinnedTimstampTask.run();

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__9223370311919910403__31__9223372036854775701__1"
        );

        assertEquals(
            metadataFiles,
            RemoteFsTimestampAwareTranslog.getMetadataFilesToBeDeleted(
                metadataFiles,
                new HashMap<>(),
                Long.MAX_VALUE,
                Map.of(),
                false,
                logger
            )
        );
    }

    public void testGetMetadataFilesToBeDeletedExclusionBasedOnAgeOnly() {
        updatePinnedTimstampTask.run();
        long currentTimeInMillis = System.currentTimeMillis();
        String md1Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 200000);
        String md2Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis + 30000);
        String md3Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis + 60000);

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__" + md1Timestamp + "__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775800__" + md2Timestamp + "__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__" + md3Timestamp + "__31__9223372036854775701__1"
        );

        List<String> metadataFilesToBeDeleted = RemoteFsTimestampAwareTranslog.getMetadataFilesToBeDeleted(
            metadataFiles,
            new HashMap<>(),
            Long.MAX_VALUE,
            Map.of(),
            false,
            logger
        );
        assertEquals(1, metadataFilesToBeDeleted.size());
        assertEquals(metadataFiles.get(0), metadataFilesToBeDeleted.get(0));
    }

    public void testGetMetadataFilesToBeDeletedExclusionBasedOnPinningOnly() throws IOException {
        long currentTimeInMillis = System.currentTimeMillis();
        String md1Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 200000);
        String md2Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 300000);
        String md3Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 600000);

        long pinnedTimestamp = RemoteStoreUtils.invertLong(md2Timestamp) + 10000;
        when(blobContainer.listBlobs()).thenReturn(Map.of(randomInt(100) + "__" + pinnedTimestamp, new PlainBlobMetadata("xyz", 100)));

        updatePinnedTimstampTask.run();

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__" + md1Timestamp + "__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775600__" + md2Timestamp + "__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__" + md3Timestamp + "__31__9223372036854775701__1"
        );

        List<String> metadataFilesToBeDeleted = RemoteFsTimestampAwareTranslog.getMetadataFilesToBeDeleted(
            metadataFiles,
            new HashMap<>(),
            Long.MAX_VALUE,
            Map.of(),
            false,
            logger
        );
        assertEquals(2, metadataFilesToBeDeleted.size());
        assertEquals(metadataFiles.get(0), metadataFilesToBeDeleted.get(0));
        assertEquals(metadataFiles.get(2), metadataFilesToBeDeleted.get(1));
    }

    public void testGetMetadataFilesToBeDeletedExclusionBasedOnAgeAndPinning() throws IOException {
        long currentTimeInMillis = System.currentTimeMillis();
        String md1Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis + 100000);
        String md2Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 300000);
        String md3Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 600000);

        long pinnedTimestamp = RemoteStoreUtils.invertLong(md2Timestamp) + 10000;
        when(blobContainer.listBlobs()).thenReturn(Map.of(randomInt(100) + "__" + pinnedTimestamp, new PlainBlobMetadata("xyz", 100)));

        updatePinnedTimstampTask.run();

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__" + md1Timestamp + "__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775600__" + md2Timestamp + "__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__" + md3Timestamp + "__31__9223372036854775701__1"
        );

        List<String> metadataFilesToBeDeleted = RemoteFsTimestampAwareTranslog.getMetadataFilesToBeDeleted(
            metadataFiles,
            new HashMap<>(),
            Long.MAX_VALUE,
            Map.of(),
            false,
            logger
        );
        assertEquals(1, metadataFilesToBeDeleted.size());
        assertEquals(metadataFiles.get(2), metadataFilesToBeDeleted.get(0));
    }

    public void testIsGenerationPinned() {
        TreeSet<Tuple<Long, Long>> pinnedGenerations = new TreeSet<>(new TreeSet<>((o1, o2) -> {
            if (Objects.equals(o1.v1(), o2.v1()) == false) {
                return o1.v1().compareTo(o2.v1());
            } else {
                return o1.v2().compareTo(o2.v2());
            }
        }));

        pinnedGenerations.add(new Tuple<>(12L, 34L));
        pinnedGenerations.add(new Tuple<>(121L, 140L));
        pinnedGenerations.add(new Tuple<>(142L, 160L));
        pinnedGenerations.add(new Tuple<>(12L, 120L));
        pinnedGenerations.add(new Tuple<>(12L, 78L));
        pinnedGenerations.add(new Tuple<>(142L, 170L));
        pinnedGenerations.add(new Tuple<>(1L, 1L));
        pinnedGenerations.add(new Tuple<>(12L, 56L));
        pinnedGenerations.add(new Tuple<>(142L, 180L));
        pinnedGenerations.add(new Tuple<>(4L, 9L));

        RemoteFsTimestampAwareTranslog translog = (RemoteFsTimestampAwareTranslog) this.translog;

        assertFalse(translog.isGenerationPinned(3, pinnedGenerations));
        assertFalse(translog.isGenerationPinned(10, pinnedGenerations));
        assertFalse(translog.isGenerationPinned(141, pinnedGenerations));
        assertFalse(translog.isGenerationPinned(181, pinnedGenerations));
        assertFalse(translog.isGenerationPinned(5000, pinnedGenerations));
        assertFalse(translog.isGenerationPinned(0, pinnedGenerations));

        assertTrue(translog.isGenerationPinned(1, pinnedGenerations));
        assertTrue(translog.isGenerationPinned(120, pinnedGenerations));
        assertTrue(translog.isGenerationPinned(121, pinnedGenerations));
        assertTrue(translog.isGenerationPinned(156, pinnedGenerations));
        assertTrue(translog.isGenerationPinned(12, pinnedGenerations));
    }

    public void testGetMinMaxTranslogGenerationFromMetadataFile() throws IOException {
        TranslogTransferManager translogTransferManager = mock(TranslogTransferManager.class);

        RemoteFsTimestampAwareTranslog translog = (RemoteFsTimestampAwareTranslog) this.translog;

        // Fetch generations directly from the filename
        assertEquals(
            new Tuple<>(701L, 1008L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__9223372036854775106__1__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(4L, 7L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__9223372036854775803__2__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(106L, 106L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775701__9223370311919910403__31__9223372036854775701__3__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(4573L, 99964L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854675843__9223370311919910408__31__9223372036854771234__4__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(1L, 4L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775803__9223370311919910413__31__9223372036854775806__5__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(2474L, 3462L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854772345__9223370311919910429__31__9223372036854773333__6__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(5807L, 7917L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854767890__9223370311919910434__31__9223372036854770000__7__1",
                translogTransferManager
            )
        );

        // For older md filenames, it needs to read the content
        TranslogTransferMetadata md1 = mock(TranslogTransferMetadata.class);
        when(md1.getMinTranslogGeneration()).thenReturn(701L);
        when(md1.getGeneration()).thenReturn(1008L);
        when(translogTransferManager.readMetadata("metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__1"))
            .thenReturn(md1);
        assertEquals(
            new Tuple<>(701L, 1008L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__1",
                translogTransferManager
            )
        );
        TranslogTransferMetadata md2 = mock(TranslogTransferMetadata.class);
        when(md2.getMinTranslogGeneration()).thenReturn(4L);
        when(md2.getGeneration()).thenReturn(7L);
        when(translogTransferManager.readMetadata("metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__1"))
            .thenReturn(md2);
        assertEquals(
            new Tuple<>(4L, 7L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__1",
                translogTransferManager
            )
        );

        verify(translogTransferManager).readMetadata("metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__1");
        verify(translogTransferManager).readMetadata("metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__1");
    }

    public void testDeleteStaleRemotePrimaryTerms() throws IOException {
        TranslogTransferManager translogTransferManager = mock(TranslogTransferManager.class);

        List<String> metadataFiles = List.of(
            // PT 4 to 9
            "metadata__9223372036854775798__9223372036854774799__9223370311919910393__node1__9223372036438563958__4__1",
            // PT 2 to 7
            "metadata__9223372036854775800__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1",
            // PT 2 to 6
            "metadata__9223372036854775801__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1"
        );

        Logger staticLogger = LogManager.getLogger(RemoteFsTimestampAwareTranslogTests.class);
        when(translogTransferManager.listPrimaryTermsInRemote()).thenReturn(Set.of(1L, 2L, 3L, 4L));
        AtomicLong minPrimaryTermInRemote = new AtomicLong(Long.MAX_VALUE);
        RemoteFsTimestampAwareTranslog.deleteStaleRemotePrimaryTerms(
            metadataFiles,
            translogTransferManager,
            new HashMap<>(),
            minPrimaryTermInRemote,
            staticLogger
        );
        verify(translogTransferManager).deletePrimaryTermsAsync(2L);
        assertEquals(2, minPrimaryTermInRemote.get());

        RemoteFsTimestampAwareTranslog.deleteStaleRemotePrimaryTerms(
            metadataFiles,
            translogTransferManager,
            new HashMap<>(),
            minPrimaryTermInRemote,
            staticLogger
        );
        // This means there are no new invocations of deletePrimaryTermAsync
        verify(translogTransferManager, times(1)).deletePrimaryTermsAsync(anyLong());
    }

    public void testDeleteStaleRemotePrimaryTermsNoPrimaryTermInRemote() throws IOException {
        TranslogTransferManager translogTransferManager = mock(TranslogTransferManager.class);

        List<String> metadataFiles = List.of(
            // PT 4 to 9
            "metadata__9223372036854775798__9223372036854774799__9223370311919910393__node1__9223372036438563958__4__1",
            // PT 2 to 7
            "metadata__9223372036854775800__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1",
            // PT 2 to 6
            "metadata__9223372036854775801__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1"
        );

        Logger staticLogger = LogManager.getLogger(RemoteFsTimestampAwareTranslogTests.class);
        when(translogTransferManager.listPrimaryTermsInRemote()).thenReturn(Set.of());
        AtomicLong minPrimaryTermInRemote = new AtomicLong(Long.MAX_VALUE);
        RemoteFsTimestampAwareTranslog.deleteStaleRemotePrimaryTerms(
            metadataFiles,
            translogTransferManager,
            new HashMap<>(),
            minPrimaryTermInRemote,
            staticLogger
        );
        verify(translogTransferManager, times(0)).deletePrimaryTermsAsync(anyLong());
        assertEquals(Long.MAX_VALUE, minPrimaryTermInRemote.get());
    }

    public void testDeleteStaleRemotePrimaryTermsPrimaryTermInRemoteIsBigger() throws IOException {
        TranslogTransferManager translogTransferManager = mock(TranslogTransferManager.class);

        List<String> metadataFiles = List.of(
            // PT 4 to 9
            "metadata__9223372036854775798__9223372036854774799__9223370311919910393__node1__9223372036438563958__4__1",
            // PT 2 to 7
            "metadata__9223372036854775800__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1",
            // PT 2 to 6
            "metadata__9223372036854775801__9223372036854774799__9223370311919910393__node1__9223372036438563958__2__1"
        );

        Logger staticLogger = LogManager.getLogger(RemoteFsTimestampAwareTranslogTests.class);
        when(translogTransferManager.listPrimaryTermsInRemote()).thenReturn(Set.of(2L, 3L, 4L));
        AtomicLong minPrimaryTermInRemote = new AtomicLong(Long.MAX_VALUE);
        RemoteFsTimestampAwareTranslog.deleteStaleRemotePrimaryTerms(
            metadataFiles,
            translogTransferManager,
            new HashMap<>(),
            minPrimaryTermInRemote,
            staticLogger
        );
        verify(translogTransferManager, times(0)).deletePrimaryTermsAsync(anyLong());
        assertEquals(2, minPrimaryTermInRemote.get());
    }

}

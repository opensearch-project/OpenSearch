/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.model.RemoteStorePinnedTimestampsBlobStore;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.mockito.Mockito;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class RemoteFsTranslogWithPinnedTimestampTests extends RemoteFsTranslogTests {

    Runnable updatePinnedTimstampTask;
    BlobStoreTransferService pinnedTimestampBlobStoreTransferService;
    RemoteStorePinnedTimestampsBlobStore remoteStorePinnedTimestampsBlobStore;
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
            updatePinnedTimstampTask = invocationOnMock.getArgument(0);
            updatePinnedTimstampTask.run();
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

        remoteStorePinnedTimestampsBlobStore = mock(RemoteStorePinnedTimestampsBlobStore.class);
        pinnedTimestampBlobStoreTransferService = mock(BlobStoreTransferService.class);
        when(remoteStorePinnedTimestampServiceSpy.pinnedTimestampsBlobStore()).thenReturn(remoteStorePinnedTimestampsBlobStore);
        when(remoteStorePinnedTimestampServiceSpy.blobStoreTransferService()).thenReturn(pinnedTimestampBlobStoreTransferService);

        doAnswer(invocationOnMock -> {
            ActionListener<List<BlobMetadata>> actionListener = invocationOnMock.getArgument(3);
            actionListener.onResponse(new ArrayList<>());
            return null;
        }).when(pinnedTimestampBlobStoreTransferService).listAllInSortedOrder(any(), any(), eq(1), any());

        remoteStorePinnedTimestampServiceSpy.start();
    }

    @Override
    public void testSimpleOperationsUpload() throws Exception {
        ArrayList<Translog.Operation> ops = new ArrayList<>();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 0, primaryTerm.get(), new byte[] { 1 }));
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertThat(snapshot, SnapshotMatchers.equalsTo(ops));
            assertEquals(ops.size(), snapshot.totalOperations());
        }

        assertEquals(2, translog.allUploaded().size());

        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("1", 1, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(4, translog.allUploaded().size());

        translog.rollGeneration();
        assertEquals(4, translog.allUploaded().size());

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
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(2, translog.readers.size());
        assertBusy(() -> {
            assertEquals(4, translog.allUploaded().size());
            assertEquals(
                6,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        // This should trim tlog-2 from local
        // This should not trim tlog-2.* files from remote as we not uploading any more translog to remote
        translog.setMinSeqNoToKeep(1);
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(1, translog.readers.size());
        assertBusy(() -> {
            assertEquals(2, translog.allUploaded().size());
            assertEquals(
                6,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

        // this should now trim as tlog-2 files from remote, but not tlog-3 and tlog-4
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("2", 2, primaryTerm.get(), new byte[] { 1 }));
        assertEquals(2, translog.stats().estimatedNumberOfOperations());
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        translog.setMinSeqNoToKeep(2);
        // this should now trim as tlog-2 files from remote, but not tlog-3 and tlog-4
        translog.trimUnreferencedReaders();
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(1, translog.readers.size());
        assertEquals(1, translog.stats().estimatedNumberOfOperations());
        assertBusy(() -> {
            assertEquals(2, translog.allUploaded().size());
            assertEquals(
                8,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });

    }
}

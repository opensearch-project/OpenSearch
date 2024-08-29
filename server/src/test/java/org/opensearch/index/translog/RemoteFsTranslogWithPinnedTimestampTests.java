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
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.model.RemotePinnedTimestamps;
import org.opensearch.gateway.remote.model.RemoteStorePinnedTimestampsBlobStore;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.mockito.Mockito;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        updatePinnedTimstampTask.run();

        translog.setMinSeqNoToKeep(4);
        translog.trimUnreferencedReaders();
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("7", 7, primaryTerm.get(), new byte[] { 1 }));
        addToTranslogAndListAndUpload(translog, ops, new Translog.Index("8", 8, primaryTerm.get(), new byte[] { 1 }));
        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));

        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.ZERO);
        updatePinnedTimstampTask.run();
        translog.trimUnreferencedReaders();

        assertBusy(() -> assertTrue(translog.isRemoteGenerationDeletionPermitsAvailable()));
        assertEquals(5, translog.readers.size());
        assertBusy(() -> {
            assertEquals(1, blobStoreTransferService.listAll(getTranslogDirectory().add(METADATA_DIR)).size());
            assertEquals(10, translog.allUploaded().size());
            assertEquals(
                10,
                blobStoreTransferService.listAll(getTranslogDirectory().add(DATA_DIR).add(String.valueOf(primaryTerm.get()))).size()
            );
        });
    }

    // getGenerationsToBeDeleted
    public void testGetGenerationsToBeDeleted() {
        // translog.readAndCacheGenerationForPinnedTimestamp
        // translog.getGenerationsToBeDeleted
    }

    public void testGetMetadataFilesToBeDeletedNoExclusion() {
        updatePinnedTimstampTask.run();

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__9223370311919910403__31__9223372036854775701__1"
        );

        assertEquals(metadataFiles, translog.getMetadataFilesToBeDeleted(metadataFiles));
    }

    public void testGetMetadataFilesToBeDeletedExclusionBasedOnAgeOnly() {
        updatePinnedTimstampTask.run();
        long currentTimeInMillis = System.currentTimeMillis();
        String md1Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 100000);
        String md2Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis + 30000);
        String md3Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis + 60000);

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__" + md1Timestamp + "__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775800__" + md2Timestamp + "__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__" + md3Timestamp + "__31__9223372036854775701__1"
        );

        List<String> metadataFilesToBeDeleted = translog.getMetadataFilesToBeDeleted(metadataFiles);
        assertEquals(1, metadataFilesToBeDeleted.size());
        assertEquals(metadataFiles.get(0), metadataFilesToBeDeleted.get(0));
    }

    public void testGetMetadataFilesToBeDeletedExclusionBasedOnPinningOnly() throws IOException {
        long currentTimeInMillis = System.currentTimeMillis();
        String md1Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 100000);
        String md2Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 300000);
        String md3Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 600000);

        doAnswer(invocationOnMock -> {
            ActionListener<List<BlobMetadata>> actionListener = invocationOnMock.getArgument(3);
            actionListener.onResponse(List.of(new PlainBlobMetadata("pinned_timestamp_123", 1000)));
            return null;
        }).when(pinnedTimestampBlobStoreTransferService).listAllInSortedOrder(any(), any(), eq(1), any());

        long pinnedTimestamp = RemoteStoreUtils.invertLong(md2Timestamp) + 10000;
        when(remoteStorePinnedTimestampsBlobStore.read(any())).thenReturn(
            new RemotePinnedTimestamps.PinnedTimestamps(Map.of(pinnedTimestamp, List.of("xyz")))
        );
        when(remoteStorePinnedTimestampsBlobStore.getBlobPathForUpload(any())).thenReturn(new BlobPath());

        updatePinnedTimstampTask.run();

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__" + md1Timestamp + "__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775600__" + md2Timestamp + "__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__" + md3Timestamp + "__31__9223372036854775701__1"
        );

        List<String> metadataFilesToBeDeleted = translog.getMetadataFilesToBeDeleted(metadataFiles);
        assertEquals(2, metadataFilesToBeDeleted.size());
        assertEquals(metadataFiles.get(0), metadataFilesToBeDeleted.get(0));
        assertEquals(metadataFiles.get(2), metadataFilesToBeDeleted.get(1));
    }

    public void testGetMetadataFilesToBeDeletedExclusionBasedOnAgeAndPinning() throws IOException {
        long currentTimeInMillis = System.currentTimeMillis();
        String md1Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis + 100000);
        String md2Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 300000);
        String md3Timestamp = RemoteStoreUtils.invertLong(currentTimeInMillis - 600000);

        doAnswer(invocationOnMock -> {
            ActionListener<List<BlobMetadata>> actionListener = invocationOnMock.getArgument(3);
            actionListener.onResponse(List.of(new PlainBlobMetadata("pinned_timestamp_123", 1000)));
            return null;
        }).when(pinnedTimestampBlobStoreTransferService).listAllInSortedOrder(any(), any(), eq(1), any());

        long pinnedTimestamp = RemoteStoreUtils.invertLong(md2Timestamp) + 10000;
        when(remoteStorePinnedTimestampsBlobStore.read(any())).thenReturn(
            new RemotePinnedTimestamps.PinnedTimestamps(Map.of(pinnedTimestamp, List.of("xyz")))
        );
        when(remoteStorePinnedTimestampsBlobStore.getBlobPathForUpload(any())).thenReturn(new BlobPath());

        updatePinnedTimstampTask.run();

        List<String> metadataFiles = List.of(
            "metadata__9223372036438563903__9223372036854774799__" + md1Timestamp + "__31__9223372036854775106__1",
            "metadata__9223372036438563903__9223372036854775600__" + md2Timestamp + "__31__9223372036854775803__1",
            "metadata__9223372036438563903__9223372036854775701__" + md3Timestamp + "__31__9223372036854775701__1"
        );

        List<String> metadataFilesToBeDeleted = translog.getMetadataFilesToBeDeleted(metadataFiles);
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

        // Fetch generations directly from the filename
        assertEquals(
            new Tuple<>(701L, 1008L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854774799__9223370311919910393__31__9223372036854775106__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(4L, 7L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775800__9223370311919910398__31__9223372036854775803__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(106L, 106L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775701__9223370311919910403__31__9223372036854775701__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(4573L, 99964L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854675843__9223370311919910408__31__9223372036854771234__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(1L, 4L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854775803__9223370311919910413__31__9223372036854775806__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(2474L, 3462L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854772345__9223370311919910429__31__9223372036854773333__1",
                translogTransferManager
            )
        );
        assertEquals(
            new Tuple<>(5807L, 7917L),
            translog.getMinMaxTranslogGenerationFromMetadataFile(
                "metadata__9223372036438563903__9223372036854767890__9223370311919910434__31__9223372036854770000__1",
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
}

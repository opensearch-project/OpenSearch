/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.index.translog.transfer.TranslogTransferManager.CHECKPOINT_FILE_DATA_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobStoreTransferServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    private BlobStoreRepository repository;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        repository = createRepository();
        threadPool = new TestThreadPool(getClass().getName());
    }

    public void testUploadBlob() throws IOException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            testFile,
            randomNonNegativeLong(),
            null
        );
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        transferService.uploadBlob(transferFileSnapshot, repository.basePath(), WritePriority.HIGH);
    }

    public void testUploadBlobFromByteArray() throws IOException {
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            "dummy_name",
            randomByteArrayOfLength(128),
            1
        );
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        transferService.uploadBlob(transferFileSnapshot, repository.basePath(), WritePriority.NORMAL);
    }

    public void testUploadBlobAsync() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        AtomicBoolean succeeded = new AtomicBoolean(false);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            testFile,
            randomNonNegativeLong(),
            null
        );
        CountDownLatch latch = new CountDownLatch(1);
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        transferService.uploadBlob(
            ThreadPool.Names.TRANSLOG_TRANSFER,
            transferFileSnapshot,
            repository.basePath(),
            new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(FileSnapshot.TransferFileSnapshot fileSnapshot) {
                    assert succeeded.compareAndSet(false, true);
                    assertEquals(transferFileSnapshot.getPrimaryTerm(), fileSnapshot.getPrimaryTerm());
                    assertEquals(transferFileSnapshot.getName(), fileSnapshot.getName());
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Failed to perform uploadBlobAsync", e);
                }
            }, latch),
            WritePriority.HIGH
        );
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        assertTrue(succeeded.get());
    }

    public void testUploadBlobFromInputStreamSyncFSRepo() throws IOException, InterruptedException {
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        uploadBlobFromInputStream(transferService);
    }

    public void testUploadBlobFromInputStreamAsyncFSRepo() throws IOException, InterruptedException {
        BlobStore blobStore = createTestBlobStore();
        MockAsyncFsContainer mockAsyncFsContainer = new MockAsyncFsContainer((FsBlobStore) blobStore, BlobPath.cleanPath(), null);
        FsBlobStore fsBlobStore = mock(FsBlobStore.class);
        when(fsBlobStore.blobContainer(any())).thenReturn(mockAsyncFsContainer);

        TransferService transferService = new BlobStoreTransferService(fsBlobStore, threadPool);
        uploadBlobFromInputStream(transferService);
    }

    private IndexMetadata getIndexMetadata() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return new IndexMetadata.Builder(index.getName()).settings(idxSettings).version(5L).numberOfShards(1).numberOfReplicas(0).build();
    }

    private void uploadBlobFromInputStream(TransferService transferService) throws IOException, InterruptedException {
        TestClass testObject = new TestClass("field1", "value1");
        AtomicBoolean succeeded = new AtomicBoolean(false);
        ChecksumBlobStoreFormat<IndexMetadata> blobStoreFormat = new ChecksumBlobStoreFormat<>(
            "coordination",
            "%s",
            IndexMetadata::fromXContent
        );
        IndexMetadata indexMetadata = getIndexMetadata();
        try (
            InputStream inputStream = blobStoreFormat.serialize(
                indexMetadata,
                "index-metadata",
                new NoneCompressor(),
                new ToXContent.MapParams(Map.of())
            ).streamInput()
        ) {
            CountDownLatch latch = new CountDownLatch(1);
            ActionListener<TestClass> listener = new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(TestClass testObject) {
                    assert succeeded.compareAndSet(false, true);
                    assert testObject.name.equals("field1");
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Failed to perform uploadBlobAsync", e);
                }
            }, latch);
            ActionListener<Void> completionListener = ActionListener.wrap(
                resp -> listener.onResponse(testObject),
                ex -> listener.onFailure(ex)
            );
            transferService.uploadBlob(inputStream, repository.basePath(), "test-object", WritePriority.URGENT, completionListener);
            assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
            assertTrue(succeeded.get());
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        repository.stop();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /** Create a {@link Repository} with a random name **/
    private BlobStoreRepository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
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

    private BlobStore createTestBlobStore() throws IOException {
        return new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
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

    public void testBuildTransferFileMetadata_EmptyInputStream() throws IOException {
        InputStream emptyInputStream = new ByteArrayInputStream(new byte[0]);
        Map<String, String> metadata = BlobStoreTransferService.buildTransferFileMetadata(emptyInputStream);
        assertTrue(metadata.containsKey(CHECKPOINT_FILE_DATA_KEY));
        assertEquals("", metadata.get(CHECKPOINT_FILE_DATA_KEY));
    }

    public void testBuildTransferFileMetadata_NonEmptyInputStream() throws IOException {
        String inputData = "This is a test input stream.";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
        Map<String, String> metadata = BlobStoreTransferService.buildTransferFileMetadata(inputStream);
        assertTrue(metadata.containsKey(CHECKPOINT_FILE_DATA_KEY));
        String expectedBase64String = Base64.getEncoder().encodeToString(inputData.getBytes(StandardCharsets.UTF_8));
        assertEquals(expectedBase64String, metadata.get(CHECKPOINT_FILE_DATA_KEY));
    }

    public void testBuildTransferFileMetadata_InputStreamExceedsLimit() {
        byte[] largeData = new byte[1025]; // 1025 bytes, exceeding the 1KB limit
        InputStream largeInputStream = new ByteArrayInputStream(largeData);
        IOException exception = assertThrows(IOException.class, () -> BlobStoreTransferService.buildTransferFileMetadata(largeInputStream));
        assertEquals(exception.getMessage(), "Input stream exceeds 1KB limit");
    }

    public void testBuildTransferFileMetadata_SmallInputStreamOptimization() throws IOException {
        String inputData = "Small input";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
        Map<String, String> metadata = BlobStoreTransferService.buildTransferFileMetadata(inputStream);
        assertTrue(metadata.containsKey(CHECKPOINT_FILE_DATA_KEY));
        String expectedBase64String = Base64.getEncoder().encodeToString(inputData.getBytes(StandardCharsets.UTF_8));
        assertEquals(expectedBase64String, metadata.get(CHECKPOINT_FILE_DATA_KEY));
    }

    private static class TestClass implements Serializable {
        private TestClass(String name, String value) {
            this.name = name;
            this.value = value;
        }

        private final String name;
        private final String value;

        @Override
        public String toString() {
            return "TestClass{ name: " + name + ", value: " + value + " }";
        }
    }

    private static class MockAsyncFsContainer extends FsBlobContainer implements AsyncMultiStreamBlobContainer {

        private BlobContainer delegate;

        public MockAsyncFsContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
            super(blobStore, blobPath, path);
            delegate = blobStore.blobContainer(BlobPath.cleanPath());
        }

        @Override
        public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
            InputStream inputStream = writeContext.getStreamProvider(Integer.MAX_VALUE).provideStream(0).getInputStream();
            delegate.writeBlob(writeContext.getFileName(), inputStream, writeContext.getFileSize(), true);
            completionListener.onResponse(null);
        }

        @Override
        public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
            throw new RuntimeException("read not supported");
        }

        @Override
        public boolean remoteIntegrityCheckSupported() {
            return false;
        }

        public BlobContainer getDelegate() {
            return delegate;
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.mockito.stubbing.Answer;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.Stream;
import org.opensearch.crypto.CryptoClient;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        transferService.uploadBlob(transferFileSnapshot, repository.basePath());
    }

    public void testUploadBlobFromByteArray() throws IOException {
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            "dummy_name",
            randomByteArrayOfLength(128),
            1
        );
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        transferService.uploadBlob(transferFileSnapshot, repository.basePath());
    }

    public void testUploadBlobAsync() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        AtomicBoolean succeeded = new AtomicBoolean(false);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());
        CountDownLatch latch = new CountDownLatch(1);
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        transferService.uploadBlobAsync(
            ThreadPool.Names.TRANSLOG_TRANSFER,
            transferFileSnapshot,
            null,
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
            TransferContentType.DATA
        );
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        assertTrue(succeeded.get());
    }

    public void testUploadBlobAsyncWithEncryptionEnabled() throws IOException, InterruptedException {
        BlobStoreRepository repository = createRepository(true);
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        AtomicBoolean succeeded = new AtomicBoolean(false);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());
        CountDownLatch latch = new CountDownLatch(1);
        TransferService transferService = new BlobStoreTransferService(repository.blobStore(), threadPool);
        CryptoClient cryptoClient = mock(CryptoClient.class);
        when(cryptoClient.initCryptoContext()).thenReturn(mock(Object.class));
        doAnswer((Answer<Stream>) invocation -> (Stream) invocation.getArgument(1)).when(cryptoClient)
            .createEncryptingStream(any(), any(Stream.class));
        transferService.uploadBlobAsync(
            ThreadPool.Names.TRANSLOG_TRANSFER,
            transferFileSnapshot,
            cryptoClient,
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
            TransferContentType.DATA
        );
        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        assertTrue(succeeded.get());
        verify(cryptoClient, times(1)).createEncryptingStream(any(), any(Stream.class));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        repository.stop();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    private BlobStoreRepository createRepository() {
        return createRepository(null);
    }

    /** Create a {@link Repository} with a random name **/
    private BlobStoreRepository createRepository(Boolean encrypted) {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings, encrypted);
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
}

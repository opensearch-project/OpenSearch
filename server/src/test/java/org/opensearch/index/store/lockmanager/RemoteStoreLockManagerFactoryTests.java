/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteStoreLockManagerFactoryTests extends OpenSearchTestCase {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private RemoteStoreLockManagerFactory remoteStoreLockManagerFactory;

    @Before
    public void setup() throws IOException {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        remoteStoreLockManagerFactory = new RemoteStoreLockManagerFactory(repositoriesServiceSupplier);
    }

    public void testNewLockManager() throws IOException {

        String testRepository = "testRepository";
        String testIndexUUID = "testIndexUUID";
        String testShardId = "testShardId";
        RemoteStorePathStrategy pathStrategy = new RemoteStorePathStrategy(PathType.FIXED);

        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repository.basePath()).thenReturn(new BlobPath().add("base_path"));
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        when(repositoriesService.repository(testRepository)).thenReturn(repository);

        RemoteStoreLockManager lockManager = remoteStoreLockManagerFactory.newLockManager(
            testRepository,
            testIndexUUID,
            testShardId,
            pathStrategy
        );

        assertTrue(lockManager != null);
        ArgumentCaptor<BlobPath> blobPathCaptor = ArgumentCaptor.forClass(BlobPath.class);
        verify(blobStore, times(1)).blobContainer(blobPathCaptor.capture());
        List<BlobPath> blobPaths = blobPathCaptor.getAllValues();
        assertEquals("base_path/" + testIndexUUID + "/" + testShardId + "/segments/lock_files/", blobPaths.get(0).buildAsString());

        verify(repositoriesService).repository(testRepository);
    }

}

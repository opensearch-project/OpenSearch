/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class RemoteStoreMDLockManagerFactoryTests extends OpenSearchTestCase {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private RemoteStoreMDLockManagerFactory remoteStoreMDLockManagerFactory;
    @Before
    public void setup() throws IOException {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        remoteStoreMDLockManagerFactory = new RemoteStoreMDLockManagerFactory(repositoriesServiceSupplier);
    }

    public void testNewLockManagerShard() throws IOException {

        String testRepository = "testRepository";
        String testIndexUUID = "testIndexUUID";
        String testShardId = "testShardId";

        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repository.basePath()).thenReturn(new BlobPath().add("base_path"));
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        when(repositoriesService.repository(testRepository)).thenReturn(repository);

        RemoteStoreMDShardLockManager lockManager = remoteStoreMDLockManagerFactory.newLockManager(testRepository,
            testIndexUUID, testShardId);

        assertTrue(lockManager instanceof FileBasedMDShardLockManager);
        ArgumentCaptor<BlobPath> blobPathCaptor = ArgumentCaptor.forClass(BlobPath.class);
        verify(blobStore, times(1)).blobContainer(blobPathCaptor.capture());
        List<BlobPath> blobPaths = blobPathCaptor.getAllValues();
        assertEquals("base_path/" + testIndexUUID + "/" + testShardId + "/segments/lock_files/",
            blobPaths.get(0).buildAsString());

        verify(repositoriesService).repository(testRepository);
    }

    public void testNewLockManagerIndex() throws IOException {

        String testRepository = "testRepository";
        String testIndexUUID = "testIndexUUID";

        BlobStoreRepository repository = mock(BlobStoreRepository.class);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repository.basePath()).thenReturn(new BlobPath().add("base_path"));
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        when(blobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        when(repositoriesService.repository(testRepository)).thenReturn(repository);

        RemoteStoreMDIndexLockManager lockManager = remoteStoreMDLockManagerFactory.newLockManager(testRepository,
            testIndexUUID);

        assertTrue(lockManager instanceof FileBasedMDIndexLockManager);
        ArgumentCaptor<BlobPath> blobPathCaptor = ArgumentCaptor.forClass(BlobPath.class);
        verify(blobStore, times(1)).blobContainer(blobPathCaptor.capture());
        List<BlobPath> blobPaths = blobPathCaptor.getAllValues();
        assertEquals("base_path/" + testIndexUUID + "/resource_lock_files/", blobPaths.get(0).buildAsString());

        verify(repositoriesService).repository(testRepository);
    }


}

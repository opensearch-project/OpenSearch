/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link BlobStoreProvider}.
 */
public class BlobStoreProviderTests extends OpenSearchTestCase {
    @Mock
    private BlobStoreRepository mockRepository;

    @Mock
    private RepositoryMetadata mockMetadata;

    @Mock
    private Lifecycle mockLifecycle;

    @Mock
    private BlobStore mockBlobStore;

    @Mock
    private BlobStore mockServerSideEncryptionBlobStore;

    private Object lock;
    private BlobStoreProvider provider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        lock = new Object();
        when(mockMetadata.name()).thenReturn("test-repository");
        provider = new BlobStoreProvider(mockRepository, mockMetadata, mockLifecycle, lock);
    }

    public void testGetBlobStore() throws Exception {
        // Setup: Mock the serverSideEncryptedBlobStore to return a value
        // Note: Since SetOnce is used internally, we need to first call blobStore() to initialize it
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockBlobStore);

        // Initialize the server-side encrypted blob store
        provider.blobStore(false);

        // Test
        BlobStore result = provider.getBlobStore(false);

        // Verify
        assertEquals(mockBlobStore, result);
    }

    public void testGetBlobStoreWithServerSideEncryption() throws Exception {
        // Setup: Mock the serverSideEncryptedBlobStore to return a value
        // Note: Since SetOnce is used internally, we need to first call blobStore() to initialize it
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockServerSideEncryptionBlobStore);
        provider.blobStore(true);

        BlobStore result = provider.getBlobStore(true);

        // Verify
        assertEquals(mockServerSideEncryptionBlobStore, result);
    }

    public void testBlobStoreWithClientSideEncryptionFirstTime() throws Exception {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockBlobStore);

        // Test
        BlobStore result = provider.blobStore(false);

        // Verify
        assertEquals(mockBlobStore, result);
        verify(mockRepository).createBlobStore();
    }

    public void testBlobStoreWithClientSideEncryptionEnabledSubsequentCalls() throws Exception {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockBlobStore);

        // First call
        BlobStore firstResult = provider.blobStore(false);

        // Second call
        BlobStore secondResult = provider.blobStore(false);

        // Verify
        assertEquals(mockBlobStore, firstResult);
        assertEquals(mockBlobStore, secondResult);
        assertSame(firstResult, secondResult);
        // Verify createServerSideEncryptedBlobStore is called only once
        verify(mockRepository, times(1)).createBlobStore();
    }

    public void testInitBlobStoreWhenLifecycleNotStarted() {
        // Setup
        when(mockLifecycle.started()).thenReturn(false);
        when(mockLifecycle.state()).thenReturn(Lifecycle.State.STOPPED);

        // Test - should throw RepositoryException
        expectThrows(RepositoryException.class, () -> provider.initBlobStore());
    }
}

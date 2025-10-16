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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerSideEncryptionEnabledBlobStoreProviderTests extends OpenSearchTestCase {

    @Mock
    private BlobStoreRepository mockRepository;

    @Mock
    private RepositoryMetadata mockMetadata;

    @Mock
    private Lifecycle mockLifecycle;

    @Mock
    private BlobStore mockServerSideEncryptedBlobStore;

    @Mock
    private BlobStore mockClientSideEncryptedBlobStore;

    @Mock
    private BlobStore mockRegularBlobStore;

    private Object lock;
    private ServerSideEncryptionEnabledBlobStoreProvider provider;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        lock = new Object();
        when(mockMetadata.name()).thenReturn("test-repository");
        provider = new ServerSideEncryptionEnabledBlobStoreProvider(mockRepository, mockMetadata, mockLifecycle, lock);
    }

    public void testConstructor() {
        assertNotNull(provider);
        // Verify that the provider extends BlobStoreProvider
        assertTrue(provider instanceof BlobStoreProvider);
    }

    public void testGetBlobStoreWithServerSideEncryptionEnabled() {
        // Setup: Mock the serverSideEncryptedBlobStore to return a value
        // Note: Since SetOnce is used internally, we need to first call blobStore() to initialize it
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);

        // Initialize the server-side encrypted blob store
        provider.blobStore(true);

        // Test
        BlobStore result = provider.getBlobStore(true);

        // Verify
        assertEquals(mockServerSideEncryptedBlobStore, result);
    }

    public void testGetBlobStoreWithServerSideEncryptionDisabled() {
        // Setup: Mock the regular blobStore
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createClientSideEncryptedBlobStore()).thenReturn(mockRegularBlobStore);

        // Initialize the regular blob store
        provider.blobStore(false);

        // Test
        BlobStore result = provider.getBlobStore(false);

        // Verify
        assertEquals(mockRegularBlobStore, result);
    }

    public void testBlobStoreWithServerSideEncryptionEnabledFirstTime() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);

        // Test
        BlobStore result = provider.blobStore(true);

        // Verify
        assertEquals(mockServerSideEncryptedBlobStore, result);
        verify(mockRepository).createServerSideEncryptedBlobStore();
    }

    public void testBlobStoreWithServerSideEncryptionEnabledSubsequentCalls() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);

        // First call
        BlobStore firstResult = provider.blobStore(true);

        // Second call
        BlobStore secondResult = provider.blobStore(true);

        // Verify
        assertEquals(mockServerSideEncryptedBlobStore, firstResult);
        assertEquals(mockServerSideEncryptedBlobStore, secondResult);
        assertSame(firstResult, secondResult);
        // Verify createServerSideEncryptedBlobStore is called only once
        verify(mockRepository, times(1)).createServerSideEncryptedBlobStore();
    }

    public void testBlobStoreWithServerSideEncryptionDisabled() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createClientSideEncryptedBlobStore()).thenReturn(mockClientSideEncryptedBlobStore);

        // Test
        BlobStore result = provider.blobStore(false);

        // Verify
        assertEquals(mockClientSideEncryptedBlobStore, result);
    }

    public void testInitBlobStoreWithServerSideEncryptionEnabled() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);

        // Test
        BlobStore result = provider.initBlobStore(true);

        // Verify
        assertEquals(mockServerSideEncryptedBlobStore, result);
        verify(mockRepository).createServerSideEncryptedBlobStore();
    }

    public void testInitBlobStoreWithServerSideEncryptionDisabled() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createClientSideEncryptedBlobStore()).thenReturn(mockClientSideEncryptedBlobStore);

        // Test
        BlobStore result = provider.initBlobStore(false);

        // Verify
        assertEquals(mockClientSideEncryptedBlobStore, result);
        verify(mockRepository).createClientSideEncryptedBlobStore();
    }

    public void testInitBlobStoreWhenLifecycleNotStarted() {
        // Setup
        when(mockLifecycle.started()).thenReturn(false);
        when(mockLifecycle.state()).thenReturn(Lifecycle.State.STOPPED);

        // Test - should throw RepositoryException
        expectThrows(RepositoryException.class, () -> provider.initBlobStore(true));
    }

    public void testInitBlobStoreWhenRepositoryThrowsRepositoryException() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        RepositoryException repositoryException = new RepositoryException("test-repo", "test error");
        when(mockRepository.createServerSideEncryptedBlobStore()).thenThrow(repositoryException);

        // Test - should re-throw RepositoryException
        expectThrows(RepositoryException.class, () -> provider.initBlobStore(true));
    }

    public void testInitBlobStoreWhenRepositoryThrowsGenericException() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        RuntimeException genericException = new RuntimeException("generic error");
        when(mockRepository.createServerSideEncryptedBlobStore()).thenThrow(genericException);

        // Test - should wrap in RepositoryException
        try {
            expectThrows(RepositoryException.class, () -> provider.initBlobStore(true));
        } catch (RepositoryException e) {
            assertEquals("test-repository", e.repository());
            assertEquals("[test-repository] cannot create blob store", e.getMessage());
            assertEquals(genericException, e.getCause());
        }
    }

    public void testCloseWithServerSideEncryptedBlobStore() throws Exception {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);

        // Initialize the server-side encrypted blob store
        provider.blobStore(true);

        // Test
        provider.close();

        // Verify
        verify(mockServerSideEncryptedBlobStore).close();
    }

    public void testCloseWithoutServerSideEncryptedBlobStore() throws Exception {
        // Test - close without initializing server-side encrypted blob store
        provider.close();

        // Verify - no exception should be thrown and no close() called on null store
        // This test passes if no exception is thrown
    }

    public void testCloseWhenServerSideEncryptedBlobStoreThrowsException() throws Exception {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);
        doThrow(new RuntimeException("close error")).when(mockServerSideEncryptedBlobStore).close();

        // Initialize the server-side encrypted blob store
        provider.blobStore(true);

        // Test - should not throw exception even if blob store close() fails
        provider.close();

        // Verify
        verify(mockServerSideEncryptedBlobStore).close();
        // Test passes if no exception is propagated
    }

    public void testMixedUsageServerSideAndClientSide() {
        // Setup
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createServerSideEncryptedBlobStore()).thenReturn(mockServerSideEncryptedBlobStore);
        when(mockRepository.createClientSideEncryptedBlobStore()).thenReturn(mockClientSideEncryptedBlobStore);

        // Test - use both server-side and client-side encryption
        BlobStore serverSideResult = provider.blobStore(true);
        BlobStore clientSideResult = provider.blobStore(false);

        // Verify
        assertEquals(mockServerSideEncryptedBlobStore, serverSideResult);
        assertEquals(mockClientSideEncryptedBlobStore, clientSideResult);
        assertNotSame(serverSideResult, clientSideResult);

        verify(mockRepository).createServerSideEncryptedBlobStore();
        verify(mockRepository).createClientSideEncryptedBlobStore();
    }

    public void testGetBlobStoreReturnsNullWhenNotInitialized() {
        // Test - getBlobStore when server-side encrypted store is not initialized
        BlobStore result = provider.getBlobStore(true);

        // Verify - should return null since SetOnce.get() returns null when not set
        assertNull(result);
    }
}

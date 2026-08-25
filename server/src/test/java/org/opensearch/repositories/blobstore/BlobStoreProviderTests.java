/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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

    public void testInitBlobStoreLogsTimeTaken() throws Exception {
        when(mockLifecycle.started()).thenReturn(true);
        when(mockMetadata.type()).thenReturn("test-type");
        when(mockRepository.createBlobStore()).thenReturn(mockBlobStore);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(BlobStoreProvider.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "blob store init timing",
                    BlobStoreProvider.class.getCanonicalName(),
                    Level.INFO,
                    "initialized blob store for repository [test-type][test-repository] in [*]"
                )
            );
            provider.blobStore(false);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testInitBlobStoreLogsTimeTakenOnFailure() throws Exception {
        when(mockLifecycle.started()).thenReturn(true);
        when(mockMetadata.type()).thenReturn("test-type");
        when(mockRepository.createBlobStore()).thenThrow(new RuntimeException("blob store creation failed"));
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(BlobStoreProvider.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "blob store init timing",
                    BlobStoreProvider.class.getCanonicalName(),
                    Level.INFO,
                    "initialized blob store for repository [test-type][test-repository] in [*]"
                )
            );
            expectThrows(RepositoryException.class, () -> provider.blobStore(false));
            appender.assertAllExpectationsMatched();
        }
    }

    public void testInitBlobStoreWhenLifecycleNotStarted() {
        // Setup
        when(mockLifecycle.started()).thenReturn(false);
        when(mockLifecycle.state()).thenReturn(Lifecycle.State.STOPPED);

        // Test - should throw RepositoryException
        expectThrows(RepositoryException.class, () -> provider.initBlobStore());
    }

    /**
     * Verifies that {@link BlobStoreProvider#reloadBlobStore(RepositoryMetadata)} reloads BOTH the
     * non-SSE and SSE blob stores when both have been initialized. This is the regression test for
     * the bug where SSE-enabled repositories did not pick up updated metadata because only the
     * non-SSE store was reloaded.
     */
    public void testReloadBlobStoreReloadsBothStoresWhenBothInitialized() throws Exception {
        // Setup: initialize both non-SSE and SSE blob stores
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockBlobStore).thenReturn(mockServerSideEncryptionBlobStore);

        provider.blobStore(false); // initializes non-SSE store
        provider.blobStore(true);  // initializes SSE store

        RepositoryMetadata newMetadata = mock(RepositoryMetadata.class);
        when(newMetadata.name()).thenReturn("test-repository");

        // Test
        provider.reloadBlobStore(newMetadata);

        // Verify both stores were reloaded with the new metadata
        verify(mockBlobStore, times(1)).reload(newMetadata);
        verify(mockServerSideEncryptionBlobStore, times(1)).reload(newMetadata);
    }

    /**
     * Verifies that only the non-SSE blob store is reloaded when only it has been initialized.
     * The SSE blob store should not be reloaded because it has never been created.
     */
    public void testReloadBlobStoreOnlyReloadsNonSseStoreWhenOnlyNonSseInitialized() throws Exception {
        // Setup: initialize only the non-SSE blob store
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockBlobStore);

        provider.blobStore(false);

        RepositoryMetadata newMetadata = mock(RepositoryMetadata.class);
        when(newMetadata.name()).thenReturn("test-repository");

        // Test
        provider.reloadBlobStore(newMetadata);

        // Verify: only the non-SSE store was reloaded; SSE store was never touched
        verify(mockBlobStore, times(1)).reload(newMetadata);
        verify(mockServerSideEncryptionBlobStore, never()).reload(any());
    }

    /**
     * Verifies that only the SSE blob store is reloaded when only it has been initialized.
     * The non-SSE blob store should not be reloaded because it has never been created.
     */
    public void testReloadBlobStoreOnlyReloadsSseStoreWhenOnlySseInitialized() throws Exception {
        // Setup: initialize only the SSE blob store
        when(mockLifecycle.started()).thenReturn(true);
        when(mockRepository.createBlobStore()).thenReturn(mockServerSideEncryptionBlobStore);

        provider.blobStore(true);

        RepositoryMetadata newMetadata = mock(RepositoryMetadata.class);
        when(newMetadata.name()).thenReturn("test-repository");

        // Test
        provider.reloadBlobStore(newMetadata);

        // Verify: only the SSE store was reloaded; non-SSE store was never touched
        verify(mockServerSideEncryptionBlobStore, times(1)).reload(newMetadata);
        verify(mockBlobStore, never()).reload(any());
    }

    /**
     * Verifies that {@link BlobStoreProvider#reloadBlobStore(RepositoryMetadata)} is a safe no-op
     * when neither blob store has been initialized yet. This guards against NPEs when reload is
     * triggered before any blob store has been lazily created.
     */
    public void testReloadBlobStoreNoOpWhenNeitherStoreInitialized() {
        RepositoryMetadata newMetadata = mock(RepositoryMetadata.class);

        // Test - should not throw
        provider.reloadBlobStore(newMetadata);

        // Verify: neither store had reload invoked on it
        verify(mockBlobStore, never()).reload(any());
        verify(mockServerSideEncryptionBlobStore, never()).reload(any());
    }
}

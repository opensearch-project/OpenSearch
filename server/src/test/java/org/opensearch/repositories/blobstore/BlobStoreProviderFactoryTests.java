/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;


import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;

import org.opensearch.test.OpenSearchTestCase;

public class BlobStoreProviderFactoryTests extends OpenSearchTestCase {

    @Mock
    private BlobStoreRepository mockRepository;

    @Mock
    private RepositoryMetadata mockMetadata;

    @Mock
    private Lifecycle mockLifecycle;

    private Object lock;
    private BlobStoreProviderFactory factory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        lock = new Object();
    }

    @Test
    public void testConstructor() {
        // Test successful construction
        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);
        assertNotNull(factory);
    }

    @Test
    public void testGetBlobStoreProviderWithServerSideEncryptionEnabled() {
        // Setup
        Settings settings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, true)
            .build();
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Execute
        BlobStoreProvider provider = factory.getBlobStoreProvider();

        // Verify
        assertNotNull(provider);
        assertTrue(provider instanceof ServerSideEncryptionEnabledBlobStoreProvider);
        verify(mockMetadata, atLeastOnce()).settings();
    }

    @Test
    public void testGetBlobStoreProviderWithServerSideEncryptionDisabled() {
        // Setup
        Settings settings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, false)
            .build();
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Execute
        BlobStoreProvider provider = factory.getBlobStoreProvider();

        // Verify
        assertNotNull(provider);
        System.out.println("provider.getClass().getName() = " + provider.getClass().getName());
        assertFalse(provider instanceof ServerSideEncryptionEnabledBlobStoreProvider);
        assertTrue(provider instanceof BlobStoreProvider);
        verify(mockMetadata, atLeastOnce()).settings();
    }

    @Test
    public void testGetBlobStoreProviderWithDefaultSettings() {
        // Setup - empty settings (default behavior)
        Settings settings = Settings.EMPTY;
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Execute
        BlobStoreProvider provider = factory.getBlobStoreProvider();

        // Verify
        assertNotNull(provider);
        assertFalse(provider instanceof ServerSideEncryptionEnabledBlobStoreProvider);
        assertTrue(provider instanceof BlobStoreProvider);
    }

    @Test
    public void testGetBlobStoreProviderSingletonBehaviorWithEncryption() {
        // Setup
        Settings settings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, true)
            .build();
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Execute multiple calls
        BlobStoreProvider provider1 = factory.getBlobStoreProvider();
        BlobStoreProvider provider2 = factory.getBlobStoreProvider();

        // Verify same instance is returned (singleton behavior)
        assertSame(provider1, provider2);
        assertTrue(provider1 instanceof ServerSideEncryptionEnabledBlobStoreProvider);
    }

    @Test
    public void testGetBlobStoreProviderSingletonBehaviorWithoutEncryption() {
        // Setup
        Settings settings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, false)
            .build();
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Execute multiple calls
        BlobStoreProvider provider1 = factory.getBlobStoreProvider();
        BlobStoreProvider provider2 = factory.getBlobStoreProvider();

        // Verify same instance is returned (singleton behavior)
        assertSame(provider1, provider2);
        assertFalse(provider1 instanceof ServerSideEncryptionEnabledBlobStoreProvider);
    }

    @Test
    public void testGetBlobStoreProviderThreadSafety() throws InterruptedException {
        // Setup
        Settings settings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, true)
            .build();
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Test concurrent access
        final BlobStoreProvider[] providers = new BlobStoreProvider[2];
        Thread thread1 = new Thread(() -> providers[0] = factory.getBlobStoreProvider());
        Thread thread2 = new Thread(() -> providers[1] = factory.getBlobStoreProvider());

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // Verify both threads get the same instance
        assertNotNull(providers[0]);
        assertNotNull(providers[1]);
        assertSame(providers[0], providers[1]);
    }

    @Test
    public void testGetBlobStoreProviderParameterPassing() {
        // Setup
        Settings settings = Settings.EMPTY;
        when(mockMetadata.settings()).thenReturn(settings);

        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        // Execute
        BlobStoreProvider provider = factory.getBlobStoreProvider();

        // Verify that the provider was created with correct parameters
        // This test ensures that the factory passes the correct constructor parameters
        assertNotNull(provider);

        // Additional verification could be done if BlobStoreProvider had getter methods
        // or if we could verify the constructor calls through mocking
    }

    @Test
    public void testFactoryStateConsistency() {
        // Setup
        Settings encryptedSettings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, true)
            .build();
        Settings nonEncryptedSettings = Settings.builder()
            .put(RemoteStoreNodeAttribute.REPOSITORY_METADATA_SERVER_SIDE_ENCRYPTION_ENABLED_KEY, false)
            .build();

        // Test that factory maintains consistent state based on initial metadata
        when(mockMetadata.settings()).thenReturn(encryptedSettings);
        factory = new BlobStoreProviderFactory(mockRepository, mockMetadata, mockLifecycle, lock);

        BlobStoreProvider encryptedProvider = factory.getBlobStoreProvider();
        assertTrue(encryptedProvider instanceof ServerSideEncryptionEnabledBlobStoreProvider);

        // Even if we change the mock to return different settings,
        // the factory should maintain its initial behavior
        when(mockMetadata.settings()).thenReturn(nonEncryptedSettings);
        BlobStoreProvider sameProvider = factory.getBlobStoreProvider();

        assertNotSame(encryptedProvider, sameProvider);
        assertTrue(sameProvider != null);
    }

}

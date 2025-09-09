/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.NodeRoleSettings;

import static org.junit.Assert.*;

public class CompositeRemoteRepositoryTest {

    private CompositeRemoteRepository compositeRepo;
    private RepositoryMetadata mockMetadata;

    @Before
    public void setUp() {
        compositeRepo = new CompositeRemoteRepository();
        mockMetadata = new RepositoryMetadata("test-repo", "fs", Settings.EMPTY);
    }

    @Test
    public void testRegisterCompositeRepository() {
        compositeRepo.registerCompositeRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER,
            mockMetadata
        );

        RepositoryMetadata result = compositeRepo.getRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER
        );

        assertNotNull(result);
        assertEquals(mockMetadata, result);
    }

    @Test
    public void testIsServerSideEncryptionEnabled() {
        compositeRepo.registerCompositeRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.CLIENT,
            mockMetadata
        );

        // When no SERVER encryption type is registered
        assertFalse(compositeRepo.isServerSideEncryptionEnabled());

        // When SERVER encryption type is registered
        compositeRepo.registerCompositeRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER,
            mockMetadata
        );

        assertTrue(compositeRepo.isServerSideEncryptionEnabled());
    }

    @Test
    public void testMultipleRepositoryTypes() {
        compositeRepo.registerCompositeRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.CLIENT,
            mockMetadata
        );

        compositeRepo.registerCompositeRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.TRANSLOG,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER,
            mockMetadata
        );

        assertNotNull(compositeRepo.getRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.CLIENT
        ));

        assertNotNull(compositeRepo.getRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.TRANSLOG,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER
        ));
    }

    @Test
    public void testToString() {
        compositeRepo.registerCompositeRepository(
            CompositeRemoteRepository.RemoteStoreRepositoryType.SEGMENT,
            CompositeRemoteRepository.CompositeRepositoryEncryptionType.SERVER,
            mockMetadata
        );

        String result = compositeRepo.toString();
        assertTrue(result.contains("CompositeRemoteRepository"));
        assertTrue(result.contains("repositoryEncryptionTypeMap"));
    }
}

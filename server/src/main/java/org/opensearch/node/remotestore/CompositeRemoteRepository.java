/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.opensearch.cluster.metadata.RepositoryMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Composite Repository for the ServerSideEncryption support.
 */
public class CompositeRemoteRepository {

    private final Map<RemoteStoreRepositoryType, Map<CompositeRepositoryEncryptionType, RepositoryMetadata>> repositoryEncryptionTypeMap;

    public CompositeRemoteRepository() {
        repositoryEncryptionTypeMap = new HashMap<>();
    }

    public void registerCompositeRepository(
        final RemoteStoreRepositoryType repositoryType,
        final CompositeRepositoryEncryptionType type,
        final RepositoryMetadata metadata
    ) {
        Map<CompositeRepositoryEncryptionType, RepositoryMetadata> encryptionTypeMap = repositoryEncryptionTypeMap.get(repositoryType);
        if (encryptionTypeMap == null) {
            encryptionTypeMap = new HashMap<>();
        }
        encryptionTypeMap.put(type, metadata);

        repositoryEncryptionTypeMap.put(repositoryType, encryptionTypeMap);
    }

    public RepositoryMetadata getRepository(RemoteStoreRepositoryType repositoryType, CompositeRepositoryEncryptionType encryptionType) {
        return repositoryEncryptionTypeMap.get(repositoryType).get(encryptionType);
    }

    @Override
    public String toString() {
        return "CompositeRemoteRepository{" + "repositoryEncryptionTypeMap=" + repositoryEncryptionTypeMap + '}';
    }

    public boolean isServerSideEncryptionEnabled() {
        return repositoryEncryptionTypeMap.get(RemoteStoreRepositoryType.SEGMENT).containsKey(CompositeRepositoryEncryptionType.SERVER);
    }

    /**
     * Enum for Remote store repo types
     */
    public enum RemoteStoreRepositoryType {
        SEGMENT,
        TRANSLOG
    }

    /**
     * Enum for composite repo types
     */
    public enum CompositeRepositoryEncryptionType {
        CLIENT,
        SERVER
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.util.function.Supplier;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreTranslogRepo;

/**
 * Determines the {@link RemoteStorePathStrategy} at the time of index metadata creation.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteStoreCustomMetadataResolver {

    private final RemoteStoreSettings remoteStoreSettings;
    private final Supplier<Version> minNodeVersionSupplier;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final Settings settings;

    public RemoteStoreCustomMetadataResolver(
        RemoteStoreSettings remoteStoreSettings,
        Supplier<Version> minNodeVersionSupplier,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Settings settings
    ) {
        this.remoteStoreSettings = remoteStoreSettings;
        this.minNodeVersionSupplier = minNodeVersionSupplier;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.settings = settings;
    }

    public RemoteStorePathStrategy getPathStrategy() {
        PathType pathType;
        PathHashAlgorithm pathHashAlgorithm;
        // Min node version check ensures that we are enabling the new prefix type only when all the nodes understand it.
        pathType = Version.V_2_14_0.compareTo(minNodeVersionSupplier.get()) <= 0 ? remoteStoreSettings.getPathType() : PathType.FIXED;
        // If the path type is fixed, hash algorithm is not applicable.
        pathHashAlgorithm = pathType == PathType.FIXED ? null : remoteStoreSettings.getPathHashAlgorithm();
        return new RemoteStorePathStrategy(pathType, pathHashAlgorithm);
    }

    public boolean isTranslogMetadataEnabled() {
        Repository repository;
        try {
            repository = repositoriesServiceSupplier.get().repository(getRemoteStoreTranslogRepo(settings));
        } catch (RepositoryMissingException ex) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_store enabled setting", ex);
        }
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        return Version.V_2_15_0.compareTo(minNodeVersionSupplier.get()) <= 0
            && remoteStoreSettings.isTranslogMetadataEnabled()
            && blobStoreRepository.blobStore().isBlobMetadataEnabled();
    }

}

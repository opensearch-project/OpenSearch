/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;

/**
 * Extension of {@link FsRepository} that can be reloaded inplace
 *
 * @opensearch.internal
 */
public class ReloadableFsRepository extends FsRepository {
    /**
     * Constructs a shared file system repository that is reloadable in-place.
     */
    public ReloadableFsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
    }

    @Override
    public boolean isReloadable() {
        return true;
    }

    @Override
    public void reload(RepositoryMetadata repositoryMetadata) {
        super.reload(repositoryMetadata);
        validateLocation();
        readMetadata();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.translogmetadata.mocks;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

import java.util.Collections;
import java.util.Map;

public class MockFsMetadataSupportedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    public static final String TYPE_MD = "fs_metadata_supported_repository";

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            "fs_metadata_supported_repository",
            metadata -> new MockFsMetadataSupportedRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );
    }
}

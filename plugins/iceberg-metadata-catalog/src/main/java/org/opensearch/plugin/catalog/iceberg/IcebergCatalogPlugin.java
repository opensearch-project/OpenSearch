/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.catalog.CatalogMetadataClient;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.CatalogPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Plugin entry point for the Iceberg metadata catalog.
 * <p>
 * Registers the {@code iceberg_s3tables} repository type as an internal repository via
 * {@link RepositoryPlugin#getInternalRepositories} and an {@link IcebergMetadataClient} via
 * {@link CatalogPlugin}. The repository is internal because it is provisioned by core from
 * node settings rather than by the public {@code _snapshot} REST API.
 */
public class IcebergCatalogPlugin extends Plugin implements RepositoryPlugin, CatalogPlugin {

    /** Creates a new instance. */
    public IcebergCatalogPlugin() {}

    @Override
    public Map<String, Repository.Factory> getInternalRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(IcebergCatalogRepository.TYPE, IcebergCatalogRepository::new);
    }

    @Override
    public CatalogMetadataClient createMetadataClient(RepositoryMetadata repositoryMetadata, Environment environment) {
        if (!IcebergCatalogRepository.TYPE.equals(repositoryMetadata.type())) {
            throw new IllegalArgumentException(
                "expected repository type [" + IcebergCatalogRepository.TYPE + "] but got ["
                    + repositoryMetadata.type() + "]"
            );
        }
        return new IcebergMetadataClient(repositoryMetadata, environment);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            IcebergCatalogRepository.BUCKET_ARN_SETTING,
            IcebergCatalogRepository.REGION_SETTING,
            IcebergCatalogRepository.CATALOG_ENDPOINT_SETTING
        );
    }
}

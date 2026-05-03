/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.catalog.CatalogMetadataClient;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.env.Environment;

/**
 * Extension point for plugins providing an external metadata catalog (Iceberg, Glue, etc.).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogPlugin {

    /**
     * Creates the client for the catalog configured on this node. Called once at startup.
     *
     * @param repositoryMetadata  name, type, and settings under {@code catalog.repository.settings.*}
     * @param environment         node environment, for keystore secrets and config paths
     */
    CatalogMetadataClient createMetadataClient(RepositoryMetadata repositoryMetadata, Environment environment);
}

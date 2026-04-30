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
 * Extension point for plugins that provide an external metadata catalog
 * (e.g., Apache Iceberg, AWS Glue). Separate from {@link RepositoryPlugin} —
 * a plugin may implement both.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogPlugin {

    /**
     * Creates the {@link CatalogMetadataClient} for the catalog configured on this node.
     * Called once during node startup after core has resolved the catalog's repository
     * metadata from node settings. The returned client is owned by core and closed on
     * shutdown.
     *
     * @param repositoryMetadata  metadata describing the catalog repository — name, type,
     *                            and the user-supplied settings under {@code catalog.repository.settings.*}
     * @param environment         node environment, for resolving keystore-backed secrets
     *                            and relative paths against the OpenSearch config directory
     * @return a newly created catalog metadata client
     */
    CatalogMetadataClient createMetadataClient(RepositoryMetadata repositoryMetadata, Environment environment);
}

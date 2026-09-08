/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.catalog.CatalogRepository;
import org.opensearch.catalog.MetadataClient;
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
     * Creates the {@link MetadataClient} for the given catalog repository. Called once
     * during node startup after core has constructed the repository from node settings.
     * The returned client is owned by core and closed on shutdown.
     *
     * @param repository  the catalog repository registered at startup
     * @param environment node environment (for config dir, keystore secure settings)
     * @return a newly created metadata client bound to that repository
     */
    MetadataClient createMetadataClient(CatalogRepository repository, Environment environment);
}

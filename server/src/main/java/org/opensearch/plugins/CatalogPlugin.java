/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.catalog.MetadataClient;
import org.opensearch.cluster.service.ClusterService;
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
     * Creates the {@link MetadataClient} for this catalog. Called once during node startup;
     * the returned client is owned by core and closed on shutdown.
     *
     * @param env            node environment (settings, paths)
     * @param clusterService cluster service for reading cluster state
     * @return a newly created metadata client
     */
    MetadataClient createMetadataClient(Environment env, ClusterService clusterService);
}

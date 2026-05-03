/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.env.Environment;
import org.opensearch.plugins.CatalogPlugin;
import org.opensearch.plugins.Plugin;

/** Test-only {@link CatalogPlugin} that hands out a single {@link MockCatalogMetadataClient} per node. */
public class MockCatalogPlugin extends Plugin implements CatalogPlugin {

    /** Must match {@code catalog.repository.type} in test node settings. */
    public static final String TYPE = "mock_catalog";

    private final MockCatalogMetadataClient client = new MockCatalogMetadataClient();

    public MockCatalogMetadataClient getClient() {
        return client;
    }

    @Override
    public CatalogMetadataClient createMetadataClient(RepositoryMetadata repositoryMetadata, Environment environment) {
        return client;
    }
}

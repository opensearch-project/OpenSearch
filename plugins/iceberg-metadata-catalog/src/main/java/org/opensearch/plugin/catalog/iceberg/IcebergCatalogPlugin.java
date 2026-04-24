/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.plugins.Plugin;

/**
 * Plugin entry point for the Iceberg metadata catalog.
 * <p>
 * This plugin provides an Apache Iceberg backed metadata catalog for publishing
 * OpenSearch index data to S3 Tables. It copies S3 client code from repository-s3
 * for plugin isolation (plugins cannot depend on other plugins).
 */
public class IcebergCatalogPlugin extends Plugin {
    /** Creates a new instance. */
    public IcebergCatalogPlugin() {}
}

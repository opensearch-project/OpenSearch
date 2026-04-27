/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.catalog.CatalogRepository;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Setting;

/**
 * {@link CatalogRepository} backed by Apache Iceberg tables on S3 Tables.
 * <p>
 * Holds the connection settings for the warehouse bucket and REST catalog endpoint; all
 * read/write operations live on {@link IcebergMetadataClient}. Settings validation is the
 * only job of this class — hence the Repository type being {@code iceberg_s3tables}.
 *
 * @opensearch.experimental
 */
public class IcebergCatalogRepository extends CatalogRepository {

    /** Repository type identifier registered via {@code getRepositories()}. */
    public static final String TYPE = "iceberg_s3tables";

    /** ARN of the S3 Tables bucket that stores the warehouse data files. */
    public static final Setting<String> BUCKET_ARN_SETTING = Setting.simpleString("bucket_arn", Setting.Property.NodeScope);

    /** AWS region of the S3 Tables bucket and the REST catalog endpoint. */
    public static final Setting<String> REGION_SETTING = Setting.simpleString("region", Setting.Property.NodeScope);

    /** URL of the Iceberg REST catalog service. */
    public static final Setting<String> CATALOG_ENDPOINT_SETTING = Setting.simpleString("catalog_endpoint", Setting.Property.NodeScope);

    /**
     * Creates a new repository from the given metadata.
     *
     * @param metadata  repository metadata including name and user-supplied settings
     */
    public IcebergCatalogRepository(RepositoryMetadata metadata) {
        super(metadata);
        // Setting validation will move here in PR 4.
    }
}

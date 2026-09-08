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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;

/**
 * {@link CatalogRepository} backed by Apache Iceberg tables on AWS S3 Tables.
 * <p>
 * Holds the connection settings for the warehouse bucket and REST catalog endpoint; all
 * read/write operations live on {@link IcebergMetadataClient}. Settings are validated
 * in the constructor — any missing required value fails node startup.
 * <p>
 * Settings (under the {@code catalog.repository.settings.} node-setting prefix):
 * <ul>
 *   <li>{@code bucket_arn} — required, ARN of the S3 Tables bucket.</li>
 *   <li>{@code region} — required, AWS region of the bucket and REST catalog.</li>
 *   <li>{@code catalog_endpoint} — optional; defaults to
 *       {@code https://s3tables.<region>.amazonaws.com/iceberg}.</li>
 *   <li>{@code role_arn}, {@code role_session_name}, {@code identity_token_file} —
 *       optional IRSA / assume-role inputs; used by the credentials builder.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public class IcebergCatalogRepository extends CatalogRepository {

    /** Repository type identifier registered via {@code getInternalRepositories()}. */
    public static final String TYPE = "iceberg_s3tables";

    /** ARN of the S3 Tables bucket that stores the warehouse data files. */
    public static final Setting<String> BUCKET_ARN_SETTING = Setting.simpleString("bucket_arn", Setting.Property.NodeScope);

    /** AWS region of the S3 Tables bucket and the REST catalog endpoint. */
    public static final Setting<String> REGION_SETTING = Setting.simpleString("region", Setting.Property.NodeScope);

    /**
     * URL of the Iceberg REST catalog service. If empty, the default is derived from
     * {@link #REGION_SETTING} as {@code https://s3tables.<region>.amazonaws.com/iceberg}.
     */
    public static final Setting<String> CATALOG_ENDPOINT_SETTING = Setting.simpleString("catalog_endpoint", Setting.Property.NodeScope);

    /** ARN of the IAM role to assume when reaching S3 / the REST catalog. */
    public static final Setting<String> ROLE_ARN_SETTING = Setting.simpleString("role_arn", Setting.Property.NodeScope);

    /** STS session name used when assuming {@link #ROLE_ARN_SETTING}. */
    public static final Setting<String> ROLE_SESSION_NAME_SETTING = Setting.simpleString("role_session_name", Setting.Property.NodeScope);

    /**
     * Filesystem path to an OIDC web-identity token file. When set together with
     * {@link #ROLE_ARN_SETTING}, the credentials builder uses
     * {@code StsWebIdentityTokenFileCredentialsProvider} (IRSA). Relative paths are
     * resolved against {@code Environment.configDir()}.
     */
    public static final Setting<String> IDENTITY_TOKEN_FILE_SETTING = Setting.simpleString(
        "identity_token_file",
        Setting.Property.NodeScope
    );

    /**
     * Creates a new repository from the given metadata. Validates the settings — a
     * missing {@code bucket_arn} or {@code region} fails fast.
     *
     * @param metadata  repository metadata including name and user-supplied settings
     */
    public IcebergCatalogRepository(RepositoryMetadata metadata) {
        super(metadata);
        validateRequiredSettings(metadata);
    }

    /**
     * Validates required settings. Throws {@link IllegalArgumentException} if
     * {@code bucket_arn} or {@code region} are missing or empty.
     */
    private static void validateRequiredSettings(RepositoryMetadata metadata) {
        Settings settings = metadata.settings();
        if (Strings.isNullOrEmpty(BUCKET_ARN_SETTING.get(settings))) {
            throw new IllegalArgumentException(
                "repository [" + metadata.name() + "] is missing required setting [" + BUCKET_ARN_SETTING.getKey() + "]"
            );
        }
        if (Strings.isNullOrEmpty(REGION_SETTING.get(settings))) {
            throw new IllegalArgumentException(
                "repository [" + metadata.name() + "] is missing required setting [" + REGION_SETTING.getKey() + "]"
            );
        }
    }

    /** Returns the configured S3 Tables bucket ARN. Never null or empty. */
    public String getBucketArn() {
        return BUCKET_ARN_SETTING.get(getMetadata().settings());
    }

    /** Returns the configured AWS region. Never null or empty. */
    public String getRegion() {
        return REGION_SETTING.get(getMetadata().settings());
    }

    /**
     * Returns the REST catalog endpoint URL. If {@link #CATALOG_ENDPOINT_SETTING} is
     * not set, derives the default S3 Tables endpoint from the region.
     */
    public String getCatalogEndpoint() {
        String configured = CATALOG_ENDPOINT_SETTING.get(getMetadata().settings());
        if (!Strings.isNullOrEmpty(configured)) {
            return configured;
        }
        return "https://s3tables." + getRegion() + ".amazonaws.com/iceberg";
    }

    /** Returns the configured role ARN, or an empty string if unset. */
    public String getRoleArn() {
        return ROLE_ARN_SETTING.get(getMetadata().settings());
    }

    /** Returns the configured STS role session name, or an empty string if unset. */
    public String getRoleSessionName() {
        return ROLE_SESSION_NAME_SETTING.get(getMetadata().settings());
    }

    /** Returns the configured web-identity token file path, or an empty string if unset. */
    public String getIdentityTokenFile() {
        return IDENTITY_TOKEN_FILE_SETTING.get(getMetadata().settings());
    }
}

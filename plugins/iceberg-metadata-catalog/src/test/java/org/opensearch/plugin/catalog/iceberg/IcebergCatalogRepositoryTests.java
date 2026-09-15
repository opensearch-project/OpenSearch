/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class IcebergCatalogRepositoryTests extends OpenSearchTestCase {

    public void testMissingBucketArnRejected() {
        Settings settings = Settings.builder().put("region", "us-east-1").build();
        RepositoryMetadata metadata = new RepositoryMetadata("catalog", IcebergCatalogRepository.TYPE, settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IcebergCatalogRepository(metadata));
        assertTrue(e.getMessage().contains("bucket_arn"));
    }

    public void testMissingRegionRejected() {
        Settings settings = Settings.builder().put("bucket_arn", "arn:aws:s3tables:::bucket").build();
        RepositoryMetadata metadata = new RepositoryMetadata("catalog", IcebergCatalogRepository.TYPE, settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IcebergCatalogRepository(metadata));
        assertTrue(e.getMessage().contains("region"));
    }

    public void testDefaultCatalogEndpointDerivedFromRegion() {
        Settings settings = Settings.builder().put("bucket_arn", "arn:aws:s3tables:::bucket").put("region", "us-west-2").build();
        IcebergCatalogRepository repository = new IcebergCatalogRepository(
            new RepositoryMetadata("catalog", IcebergCatalogRepository.TYPE, settings)
        );
        assertEquals("https://s3tables.us-west-2.amazonaws.com/iceberg", repository.getCatalogEndpoint());
    }

    public void testCustomCatalogEndpointOverridesDefault() {
        Settings settings = Settings.builder()
            .put("bucket_arn", "arn:aws:s3tables:::bucket")
            .put("region", "us-west-2")
            .put("catalog_endpoint", "https://example.amazonaws.com/iceberg")
            .build();
        IcebergCatalogRepository repository = new IcebergCatalogRepository(
            new RepositoryMetadata("catalog", IcebergCatalogRepository.TYPE, settings)
        );
        assertEquals("https://example.amazonaws.com/iceberg", repository.getCatalogEndpoint());
    }

    public void testTypedAccessors() {
        Settings settings = Settings.builder()
            .put("bucket_arn", "arn:aws:s3tables:::bucket")
            .put("region", "us-west-2")
            .put("role_arn", "arn:aws:iam::123:role/OpenSearch")
            .put("role_session_name", "opensearch-node")
            .put("identity_token_file", "/var/run/secrets/token")
            .build();
        IcebergCatalogRepository repository = new IcebergCatalogRepository(
            new RepositoryMetadata("catalog", IcebergCatalogRepository.TYPE, settings)
        );
        assertEquals("arn:aws:s3tables:::bucket", repository.getBucketArn());
        assertEquals("us-west-2", repository.getRegion());
        assertEquals("arn:aws:iam::123:role/OpenSearch", repository.getRoleArn());
        assertEquals("opensearch-node", repository.getRoleSessionName());
        assertEquals("/var/run/secrets/token", repository.getIdentityTokenFile());
    }
}

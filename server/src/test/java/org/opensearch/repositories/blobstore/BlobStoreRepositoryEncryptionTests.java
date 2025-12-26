/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for BlobStoreRepository encryption functionality with CryptoMetadata.
 * These tests verify that crypto metadata is correctly extracted from index settings
 * and can be used for encrypted blob operations.
 */
public class BlobStoreRepositoryEncryptionTests extends OpenSearchTestCase {

    public void testResolveCryptoMetadataFromKmsIndexSettings() {
        // Create index settings with KMS encryption configuration
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.store.crypto.key_provider", "aws-kms-provider")
            .put("index.store.crypto.kms.key_arn", "arn:aws:kms:us-east-1:123456789:key/test-key")
            .put("index.store.crypto.kms.encryption_context", "tenant=test,env=prod")
            .build();

        // Extract crypto metadata using the static factory method
        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexSettings);

        // Verify it's a valid CryptoMetadata instance
        assertNotNull(cryptoMetadata);

        assertEquals("aws-kms-provider", cryptoMetadata.keyProviderName());
        assertEquals("aws-kms", cryptoMetadata.keyProviderType());

        // Verify KMS-specific settings through generic getters
        assertTrue(cryptoMetadata.getKeyArn().isPresent());
        assertEquals("arn:aws:kms:us-east-1:123456789:key/test-key", cryptoMetadata.getKeyArn().get());

        assertTrue(cryptoMetadata.getEncryptionContext().isPresent());
        assertEquals("tenant=test,env=prod", cryptoMetadata.getEncryptionContext().get());
    }

    public void testResolveCryptoMetadataReturnsNullForNonEncryptedIndex() {
        // Create index settings without any encryption configuration
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();

        // Extract crypto metadata - should be null for non-encrypted index
        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexSettings);

        assertNull(cryptoMetadata);
    }

    public void testResolveCryptoMetadataReturnsNullForMissingKeyProvider() {
        // Create index settings with some crypto-related settings but missing key provider
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.store.crypto.kms.key_arn", "arn:aws:kms:us-east-1:123456789:key/test-key")
            .build();

        // Extract crypto metadata - should be null without key provider
        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexSettings);

        assertNull(cryptoMetadata);
    }

    public void testCryptoMetadataFromCompleteIndexMetadata() {
        // Create full IndexMetadata with KMS encryption settings
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.store.type", "cryptofs")
            .put("index.store.crypto.key_provider", "kms-provider")
            .put("index.store.crypto.kms.key_arn", "arn:aws:kms:us-west-2:987654321:key/prod-key")
            .put("index.store.crypto.kms.encryption_context", "classification=secret")
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Use CryptoMetadata.fromIndexSettings
        CryptoMetadata cryptoMetadata = CryptoMetadata.fromIndexSettings(indexMetadata.getSettings());

        assertNotNull(cryptoMetadata);
        assertEquals("kms-provider", cryptoMetadata.keyProviderName());
        assertEquals("aws-kms", cryptoMetadata.keyProviderType());

        assertTrue(cryptoMetadata.getKeyArn().isPresent());
        assertEquals("arn:aws:kms:us-west-2:987654321:key/prod-key", cryptoMetadata.getKeyArn().get());

        assertTrue(cryptoMetadata.getEncryptionContext().isPresent());
        assertEquals("classification=secret", cryptoMetadata.getEncryptionContext().get());
    }

    public void testDifferentIndicesCanHaveDifferentKmsKeys() {
        // Index A with one KMS key
        Settings indexASettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.store.crypto.key_provider", "provider-a")
            .put("index.store.crypto.kms.key_arn", "arn:aws:kms:us-east-1:111:key/key-a")
            .put("index.store.crypto.kms.encryption_context", "tenant=acme")
            .build();

        // Index B with different KMS key
        Settings indexBSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.store.crypto.key_provider", "provider-b")
            .put("index.store.crypto.kms.key_arn", "arn:aws:kms:us-west-2:222:key/key-b")
            .put("index.store.crypto.kms.encryption_context", "tenant=globex")
            .build();

        CryptoMetadata cryptoA = CryptoMetadata.fromIndexSettings(indexASettings);
        CryptoMetadata cryptoB = CryptoMetadata.fromIndexSettings(indexBSettings);

        // Verify both are valid but different
        assertNotNull(cryptoA);
        assertNotNull(cryptoB);

        assertNotEquals(cryptoA.getKeyArn().get(), cryptoB.getKeyArn().get());
        assertNotEquals(cryptoA.getEncryptionContext().get(), cryptoB.getEncryptionContext().get());

        assertEquals("arn:aws:kms:us-east-1:111:key/key-a", cryptoA.getKeyArn().get());
        assertEquals("arn:aws:kms:us-west-2:222:key/key-b", cryptoB.getKeyArn().get());
    }
}

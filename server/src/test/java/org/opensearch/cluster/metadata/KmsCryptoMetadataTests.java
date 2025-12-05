/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class KmsCryptoMetadataTests extends OpenSearchTestCase {

    public void testKmsCryptoMetadataCreation() {
        Settings settings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-west-2:123456789012:key/test-key")
            .put("kms.encryption_context", "test-context")
            .build();

        KmsCryptoMetadata metadata = new KmsCryptoMetadata("test-provider", settings, true);

        assertEquals("test-provider", metadata.keyProviderName());
        assertEquals("aws-kms", metadata.keyProviderType());
        assertTrue(metadata.isIndexLevel());
        assertTrue(metadata.getKmsKeyArn().isPresent());
        assertEquals("arn:aws:kms:us-west-2:123456789012:key/test-key", metadata.getKmsKeyArn().get());
        assertTrue(metadata.getKmsEncryptionContext().isPresent());
        assertEquals("test-context", metadata.getKmsEncryptionContext().get());
    }

    public void testKmsCryptoMetadataSerialization() throws IOException {
        Settings settings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-west-2:123456789012:key/test-key")
            .put("kms.encryption_context", "test-context")
            .build();

        KmsCryptoMetadata original = new KmsCryptoMetadata("test-provider", settings, true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        KmsCryptoMetadata deserialized = new KmsCryptoMetadata(in);

        assertEquals(original.keyProviderName(), deserialized.keyProviderName());
        assertEquals(original.keyProviderType(), deserialized.keyProviderType());
        assertEquals(original.isIndexLevel(), deserialized.isIndexLevel());
        assertEquals(original.getKmsKeyArn(), deserialized.getKmsKeyArn());
        assertEquals(original.getKmsEncryptionContext(), deserialized.getKmsEncryptionContext());
    }

    public void testFromIndexSettings() {
        Settings indexSettings = Settings.builder()
            .put("index.store.crypto.key_provider", "test-provider")
            .put("index.store.crypto.kms.key_arn", "arn:aws:kms:us-west-2:123456789012:key/test-key")
            .build();

        KmsCryptoMetadata metadata = KmsCryptoMetadata.fromIndexSettings(indexSettings);

        assertNotNull(metadata);
        assertEquals("test-provider", metadata.keyProviderName());
        assertTrue(metadata.isIndexLevel());
        assertTrue(metadata.getKmsKeyArn().isPresent());
    }

    public void testFromIndexSettingsReturnsNullWhenNoProvider() {
        Settings indexSettings = Settings.builder().build();
        KmsCryptoMetadata metadata = KmsCryptoMetadata.fromIndexSettings(indexSettings);
        assertNull(metadata);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.utils.SseKmsUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class S3BlobContainerEncryptionTests extends OpenSearchTestCase {

    public void testEncryptionContextMerging() {
        String indexContext = "tenant=acme,classification=confidential";
        String repoContext = Base64.getEncoder().encodeToString("{\"repo\":\"test\",\"env\":\"prod\"}".getBytes(StandardCharsets.UTF_8));

        String merged = SseKmsUtil.mergeAndEncodeEncryptionContexts(indexContext, repoContext);
        assertNotNull(merged);

        String decoded = new String(Base64.getDecoder().decode(merged), StandardCharsets.UTF_8);
        assertTrue(decoded.contains("\"tenant\":\"acme\""));
        assertTrue(decoded.contains("\"classification\":\"confidential\""));
        assertTrue(decoded.contains("\"repo\":\"test\""));
        assertTrue(decoded.contains("\"env\":\"prod\""));
    }

    public void testCryptoMetadataExtraction() {
        Settings cryptoSettings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-east-1:123456789:key/index-key")
            .put("kms.encryption_context", "tenant=acme,env=staging")
            .build();
        CryptoMetadata cryptoMetadata = new CryptoMetadata("index-provider", "aws-kms", cryptoSettings);

        assertEquals("arn:aws:kms:us-east-1:123456789:key/index-key", cryptoMetadata.settings().get("kms.key_arn"));
        assertEquals("tenant=acme,env=staging", cryptoMetadata.settings().get("kms.encryption_context"));
    }
}

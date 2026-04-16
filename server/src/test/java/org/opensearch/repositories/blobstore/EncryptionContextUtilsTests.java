/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class EncryptionContextUtilsTests extends OpenSearchTestCase {

    public void testCryptofsToJson() {
        // Test basic conversion
        String cryptofs = "key1=value1,key2=value2";
        String json = EncryptionContextUtils.cryptofsToJson(cryptofs);
        assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", json);

        // Test with special characters in values
        cryptofs = "key=value with spaces,key2=value=with=equals";
        json = EncryptionContextUtils.cryptofsToJson(cryptofs);
        assertEquals("{\"key\":\"value with spaces\",\"key2\":\"value=with=equals\"}", json);

        // Test empty string
        assertEquals("{}", EncryptionContextUtils.cryptofsToJson(""));

        // Test null
        assertEquals("{}", EncryptionContextUtils.cryptofsToJson(null));

        // Test single key-value
        cryptofs = "singleKey=singleValue";
        json = EncryptionContextUtils.cryptofsToJson(cryptofs);
        assertEquals("{\"singleKey\":\"singleValue\"}", json);
    }

    public void testJsonToCryptofs() {
        // Test basic conversion
        String json = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        String cryptofs = EncryptionContextUtils.jsonToCryptofs(json);
        assertTrue(cryptofs.contains("key1=value1"));
        assertTrue(cryptofs.contains("key2=value2"));
        assertTrue(cryptofs.contains(","));

        // Test empty JSON
        assertEquals("", EncryptionContextUtils.jsonToCryptofs("{}"));

        // Test null
        assertEquals("", EncryptionContextUtils.jsonToCryptofs(null));

        // Test single key-value
        json = "{\"singleKey\":\"singleValue\"}";
        cryptofs = EncryptionContextUtils.jsonToCryptofs(json);
        assertEquals("singleKey=singleValue", cryptofs);
    }

    public void testMergeJson() {
        // Test basic merge - no conflicts
        String base = "{\"repo\":\"remote-segment\",\"type\":\"segments\"}";
        String override = "{\"tenant\":\"acme\",\"env\":\"staging\"}";
        String merged = EncryptionContextUtils.mergeJson(base, override);
        assertTrue(merged.contains("\"repo\":\"remote-segment\""));
        assertTrue(merged.contains("\"type\":\"segments\""));
        assertTrue(merged.contains("\"tenant\":\"acme\""));
        assertTrue(merged.contains("\"env\":\"staging\""));

        // Test merge with conflicts - override wins
        base = "{\"env\":\"prod\",\"region\":\"us-east-1\"}";
        override = "{\"env\":\"staging\",\"tenant\":\"acme\"}";
        merged = EncryptionContextUtils.mergeJson(base, override);
        assertTrue(merged.contains("\"env\":\"staging\"")); // Override value
        assertTrue(merged.contains("\"region\":\"us-east-1\""));
        assertTrue(merged.contains("\"tenant\":\"acme\""));

        // Test null handling
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.mergeJson(null, "{\"key\":\"value\"}"));
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.mergeJson("{\"key\":\"value\"}", null));
        assertEquals("{}", EncryptionContextUtils.mergeJson(null, null));

        // Test empty JSON
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.mergeJson("{}", "{\"key\":\"value\"}"));
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.mergeJson("{\"key\":\"value\"}", "{}"));
    }

    public void testMergeAndEncodeEncryptionContexts() {
        // Test with cryptofs format index context
        String indexContext = "tenant=acme,env=staging";
        String repoContext = Base64.getEncoder()
            .encodeToString("{\"repo\":\"remote-segment\",\"type\":\"segments\"}".getBytes(StandardCharsets.UTF_8));

        String result = EncryptionContextUtils.mergeAndEncodeEncryptionContexts(indexContext, repoContext);
        assertNotNull(result);

        // Decode and verify
        String decoded = new String(Base64.getDecoder().decode(result), StandardCharsets.UTF_8);
        assertTrue(decoded.contains("\"tenant\":\"acme\""));
        assertTrue(decoded.contains("\"env\":\"staging\""));
        assertTrue(decoded.contains("\"repo\":\"remote-segment\""));
        assertTrue(decoded.contains("\"type\":\"segments\""));

        // Test with JSON format index context
        indexContext = "{\"tenant\":\"acme\",\"env\":\"staging\"}";
        result = EncryptionContextUtils.mergeAndEncodeEncryptionContexts(indexContext, repoContext);
        decoded = new String(Base64.getDecoder().decode(result), StandardCharsets.UTF_8);
        assertTrue(decoded.contains("\"tenant\":\"acme\""));

        // Test null handling
        assertNull(EncryptionContextUtils.mergeAndEncodeEncryptionContexts(null, null));

        // Test only index context
        result = EncryptionContextUtils.mergeAndEncodeEncryptionContexts("key=value", null);
        decoded = new String(Base64.getDecoder().decode(result), StandardCharsets.UTF_8);
        assertEquals("{\"key\":\"value\"}", decoded);

        // Test only repo context
        result = EncryptionContextUtils.mergeAndEncodeEncryptionContexts(null, repoContext);
        assertEquals(repoContext, result);
    }

    public void testMergeCryptoMetadata() {
        // Create index metadata
        Settings indexSettings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-east-1:123456789:key/index-key")
            .put("kms.encryption_context", "tenant=acme,env=staging")
            .build();
        CryptoMetadata indexMetadata = new CryptoMetadata("index-provider", "aws-kms", indexSettings);

        // Create repository metadata
        Settings repoSettings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-east-1:123456789:key/repo-key")
            .put("kms.encryption_context", "repo=remote-segment,env=prod")
            .put("region", "us-east-1")
            .build();
        CryptoMetadata repoMetadata = new CryptoMetadata("repo-provider", "aws-kms", repoSettings);

        // Test merge
        CryptoMetadata merged = EncryptionContextUtils.mergeCryptoMetadata(indexMetadata, repoMetadata);

        // Verify key provider (index wins)
        assertEquals("index-provider", merged.keyProviderName());
        assertEquals("aws-kms", merged.keyProviderType());

        // Verify settings merge
        assertEquals("arn:aws:kms:us-east-1:123456789:key/index-key", merged.settings().get("kms.key_arn"));
        assertEquals("us-east-1", merged.settings().get("region")); // From repo

        // Verify encryption context merge
        String mergedContext = merged.settings().get("kms.encryption_context");
        assertTrue(mergedContext.contains("tenant=acme"));
        assertTrue(mergedContext.contains("env=staging")); // Index overrides repo
        assertTrue(mergedContext.contains("repo=remote-segment"));

        // Test with null repository metadata
        merged = EncryptionContextUtils.mergeCryptoMetadata(indexMetadata, null);
        assertEquals(indexMetadata.keyProviderName(), merged.keyProviderName());
        assertEquals(indexMetadata.settings(), merged.settings());

        // Test with null index metadata
        merged = EncryptionContextUtils.mergeCryptoMetadata(null, repoMetadata);
        assertEquals(repoMetadata.keyProviderName(), merged.keyProviderName());
        assertEquals(repoMetadata.settings(), merged.settings());

        // Test with both null
        assertNull(EncryptionContextUtils.mergeCryptoMetadata(null, null));
    }

    public void testParseSimpleJson() {
        // Test valid JSON
        String json = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
        var map = new java.util.HashMap<String, String>();
        EncryptionContextUtils.parseSimpleJson(json, map);
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));

        // Test empty JSON
        json = "{}";
        map = new java.util.HashMap<String, String>();
        EncryptionContextUtils.parseSimpleJson(json, map);
        assertTrue(map.isEmpty());

        // Test whitespace handling
        json = "{ \"key\" : \"value\" }";
        map = new java.util.HashMap<String, String>();
        EncryptionContextUtils.parseSimpleJson(json, map);
        assertEquals("value", map.get("key"));

        // Test special characters in values
        json = "{\"key\":\"value with spaces\",\"key2\":\"value\\\"with\\\"quotes\"}";
        map = new java.util.HashMap<String, String>();
        EncryptionContextUtils.parseSimpleJson(json, map);
        assertEquals("value with spaces", map.get("key"));
        assertEquals("value\"with\"quotes", map.get("key2"));
    }

    public void testCryptofsJsonRoundTrip() {
        // Test round trip conversion
        String originalCryptofs = "key1=value1,key2=value2,key3=value with spaces";
        String json = EncryptionContextUtils.cryptofsToJson(originalCryptofs);
        String backToCryptofs = EncryptionContextUtils.jsonToCryptofs(json);

        // Order might be different but all key-value pairs should be present
        assertTrue(backToCryptofs.contains("key1=value1"));
        assertTrue(backToCryptofs.contains("key2=value2"));
        assertTrue(backToCryptofs.contains("key3=value with spaces"));
    }

    public void testComplexMergeScenario() {
        // Complex scenario with multiple keys and conflicts
        String indexContext = "env=staging,tenant=acme,classification=confidential,region=us-west-2";
        String repoBase64 = Base64.getEncoder()
            .encodeToString(
                "{\"env\":\"prod\",\"region\":\"us-east-1\",\"repo\":\"remote-segment\",\"type\":\"segments\"}".getBytes(
                    StandardCharsets.UTF_8
                )
            );

        String result = EncryptionContextUtils.mergeAndEncodeEncryptionContexts(indexContext, repoBase64);
        String decoded = new String(Base64.getDecoder().decode(result), StandardCharsets.UTF_8);

        // Verify all keys are present with correct precedence
        assertTrue(decoded.contains("\"env\":\"staging\"")); // Index wins
        assertTrue(decoded.contains("\"tenant\":\"acme\""));
        assertTrue(decoded.contains("\"classification\":\"confidential\""));
        assertTrue(decoded.contains("\"region\":\"us-west-2\"")); // Index wins
        assertTrue(decoded.contains("\"repo\":\"remote-segment\"")); // From repo
        assertTrue(decoded.contains("\"type\":\"segments\"")); // From repo
    }

    public void testMergeAndEncodeWithNonBase64JsonRepoContext() {
        // Test with repo context that's already valid JSON (not Base64 encoded)
        // This triggers the IllegalArgumentException fallback path (line 67)
        String indexContext = "key=value";
        String rawJsonRepo = "{\"repo\":\"test\"}";  // Valid JSON but not Base64

        String result = EncryptionContextUtils.mergeAndEncodeEncryptionContexts(indexContext, rawJsonRepo);
        assertNotNull(result);
        String decoded = new String(Base64.getDecoder().decode(result), StandardCharsets.UTF_8);
        assertTrue(decoded.contains("\"key\":\"value\""));
        assertTrue(decoded.contains("\"repo\":\"test\""));
    }

    public void testMergeAndEncodeWithEmptyStrings() {
        // Test with empty strings (different from null)
        assertNull(EncryptionContextUtils.mergeAndEncodeEncryptionContexts("", ""));
        assertNull(EncryptionContextUtils.mergeAndEncodeEncryptionContexts("", null));
        assertNull(EncryptionContextUtils.mergeAndEncodeEncryptionContexts(null, ""));
    }

    public void testMergeCryptoMetadataWithOnlyRepoEncryptionContext() {
        // Index metadata with no encryption context
        Settings indexSettings = Settings.builder().put("kms.key_arn", "arn:aws:kms:us-east-1:123456789:key/index-key").build();
        CryptoMetadata indexMetadata = new CryptoMetadata("index-provider", "aws-kms", indexSettings);

        // Repo metadata with encryption context
        Settings repoSettings = Settings.builder().put("kms.encryption_context", "repo=segment").build();
        CryptoMetadata repoMetadata = new CryptoMetadata("repo-provider", "aws-kms", repoSettings);

        CryptoMetadata merged = EncryptionContextUtils.mergeCryptoMetadata(indexMetadata, repoMetadata);
        assertEquals("repo=segment", merged.settings().get("kms.encryption_context"));
    }

    public void testMergeCryptoMetadataWithJsonFormatContext() {
        // Index metadata with JSON format encryption context
        Settings indexSettings = Settings.builder().put("kms.encryption_context", "{\"tenant\":\"acme\"}").build();
        CryptoMetadata indexMetadata = new CryptoMetadata("index-provider", "aws-kms", indexSettings);

        // Repo metadata with JSON format encryption context
        Settings repoSettings = Settings.builder().put("kms.encryption_context", "{\"repo\":\"segment\"}").build();
        CryptoMetadata repoMetadata = new CryptoMetadata("repo-provider", "aws-kms", repoSettings);

        CryptoMetadata merged = EncryptionContextUtils.mergeCryptoMetadata(indexMetadata, repoMetadata);
        String mergedCtx = merged.settings().get("kms.encryption_context");
        assertTrue(mergedCtx.contains("tenant=acme"));
        assertTrue(mergedCtx.contains("repo=segment"));
    }

    public void testMergeCryptoMetadataWithNullKeyProviders() {
        // Index metadata with null key provider name
        Settings indexSettings = Settings.builder().put("some.setting", "value").build();
        CryptoMetadata indexMetadata = new CryptoMetadata(null, null, indexSettings);

        // Repo metadata with key provider
        Settings repoSettings = Settings.builder().build();
        CryptoMetadata repoMetadata = new CryptoMetadata("repo-provider", "aws-kms", repoSettings);

        CryptoMetadata merged = EncryptionContextUtils.mergeCryptoMetadata(indexMetadata, repoMetadata);
        assertEquals("repo-provider", merged.keyProviderName());
        assertEquals("aws-kms", merged.keyProviderType());
    }

    public void testCryptofsToJsonEdgeCases() {
        // Test trailing comma
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.cryptofsToJson("key=value,"));
        // Test leading comma
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.cryptofsToJson(",key=value"));
        // Test whitespace around equals
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.cryptofsToJson("key = value"));
        // Test malformed pair (no equals)
        assertEquals("{}", EncryptionContextUtils.cryptofsToJson("invalid"));
    }

    public void testJsonToCryptofsWithEmpty() {
        // Empty string input
        assertEquals("", EncryptionContextUtils.jsonToCryptofs(""));
        // Whitespace only
        assertEquals("", EncryptionContextUtils.jsonToCryptofs("   "));
    }

    public void testParseSimpleJsonWithNullAndEmpty() {
        var map = new java.util.HashMap<String, String>();
        // Null input shouldn't throw
        EncryptionContextUtils.parseSimpleJson(null, map);
        assertTrue(map.isEmpty());

        // Empty string input
        EncryptionContextUtils.parseSimpleJson("", map);
        assertTrue(map.isEmpty());

        // Whitespace only
        EncryptionContextUtils.parseSimpleJson("   ", map);
        assertTrue(map.isEmpty());
    }

    public void testMergeJsonWithEmptyStrings() {
        // Empty string base returns override
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.mergeJson("", "{\"key\":\"value\"}"));
        // Empty string override returns base
        assertEquals("{\"key\":\"value\"}", EncryptionContextUtils.mergeJson("{\"key\":\"value\"}", ""));
        // Both empty strings - base empty so returns override which is empty string
        assertEquals("", EncryptionContextUtils.mergeJson("", ""));
    }
}

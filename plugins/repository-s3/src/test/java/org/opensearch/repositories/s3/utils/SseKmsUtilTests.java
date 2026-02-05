/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.S3BlobStore;
import org.opensearch.repositories.s3.async.UploadRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SseKmsUtilTests extends OpenSearchTestCase {

    // Test mergeAndEncodeEncryptionContexts - delegates to EncryptionContextUtils
    public void testMergeAndEncodeEncryptionContexts() {
        // Test basic delegation
        String indexContext = "key1=value1";
        String repoContext = Base64.getEncoder().encodeToString("{\"repo\":\"test\"}".getBytes(StandardCharsets.UTF_8));

        String result = SseKmsUtil.mergeAndEncodeEncryptionContexts(indexContext, repoContext);
        assertNotNull(result);
        String decoded = new String(Base64.getDecoder().decode(result), StandardCharsets.UTF_8);
        assertTrue(decoded.contains("\"key1\":\"value1\""));
        assertTrue(decoded.contains("\"repo\":\"test\""));

        // Test null handling
        assertNull(SseKmsUtil.mergeAndEncodeEncryptionContexts(null, null));
    }

    // Test configureEncryptionSettings for CreateMultipartUploadRequest with AES256
    public void testConfigureEncryptionSettingsMultipartWithAES256() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, null);

        CreateMultipartUploadRequest request = builder.build();
        assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
    }

    // Test configureEncryptionSettings for CreateMultipartUploadRequest with KMS
    public void testConfigureEncryptionSettingsMultipartWithKmsNoCryptoMetadata() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(blobStore.serverSideEncryptionKmsKey()).thenReturn("arn:aws:kms:us-east-1:123:key/repo-key");
        when(blobStore.serverSideEncryptionBucketKey()).thenReturn(true);
        when(blobStore.serverSideEncryptionEncryptionContext()).thenReturn(null);

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, null);

        CreateMultipartUploadRequest request = builder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals("arn:aws:kms:us-east-1:123:key/repo-key", request.ssekmsKeyId());
        assertTrue(request.bucketKeyEnabled());
        assertNull(request.ssekmsEncryptionContext());
    }

    // Test configureEncryptionSettings for CreateMultipartUploadRequest with KMS and CryptoMetadata
    public void testConfigureEncryptionSettingsMultipartWithKmsAndCryptoMetadata() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(blobStore.serverSideEncryptionKmsKey()).thenReturn("arn:aws:kms:us-east-1:123:key/repo-key");
        when(blobStore.serverSideEncryptionBucketKey()).thenReturn(false);
        String repoContext = Base64.getEncoder().encodeToString("{\"repo\":\"segment\"}".getBytes(StandardCharsets.UTF_8));
        when(blobStore.serverSideEncryptionEncryptionContext()).thenReturn(repoContext);

        // Create CryptoMetadata with index-level settings
        Settings settings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-east-1:123:key/index-key")
            .put("kms.encryption_context", "tenant=acme")
            .build();
        CryptoMetadata cryptoMetadata = new CryptoMetadata("provider", "aws-kms", settings);

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, cryptoMetadata);

        CreateMultipartUploadRequest request = builder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals("arn:aws:kms:us-east-1:123:key/index-key", request.ssekmsKeyId()); // Index key takes precedence
        assertFalse(request.bucketKeyEnabled());

        // Verify merged encryption context
        String decoded = new String(Base64.getDecoder().decode(request.ssekmsEncryptionContext()), StandardCharsets.UTF_8);
        assertTrue(decoded.contains("\"tenant\":\"acme\""));
        assertTrue(decoded.contains("\"repo\":\"segment\""));
    }

    // Test configureEncryptionSettings for PutObjectRequest with AES256
    public void testConfigureEncryptionSettingsPutObjectWithAES256() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());

        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, null);

        PutObjectRequest request = builder.build();
        assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
    }

    // Test configureEncryptionSettings for PutObjectRequest with KMS
    public void testConfigureEncryptionSettingsPutObjectWithKms() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(blobStore.serverSideEncryptionKmsKey()).thenReturn("arn:aws:kms:us-east-1:123:key/repo-key");
        when(blobStore.serverSideEncryptionBucketKey()).thenReturn(true);
        when(blobStore.serverSideEncryptionEncryptionContext()).thenReturn(null);

        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, null);

        PutObjectRequest request = builder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals("arn:aws:kms:us-east-1:123:key/repo-key", request.ssekmsKeyId());
        assertTrue(request.bucketKeyEnabled());
    }

    // Test convenience overload without CryptoMetadata
    public void testConfigureEncryptionSettingsPutObjectConvenienceOverload() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());

        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore);

        PutObjectRequest request = builder.build();
        assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
    }

    // Test configureEncryptionSettings with UploadRequest for CreateMultipartUploadRequest
    public void testConfigureEncryptionSettingsMultipartWithUploadRequestAES256() {
        UploadRequest uploadRequest = mock(UploadRequest.class);
        when(uploadRequest.getServerSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, uploadRequest);

        CreateMultipartUploadRequest request = builder.build();
        assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
    }

    public void testConfigureEncryptionSettingsMultipartWithUploadRequestKms() {
        UploadRequest uploadRequest = mock(UploadRequest.class);
        when(uploadRequest.getServerSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(uploadRequest.getServerSideEncryptionKmsKey()).thenReturn("arn:aws:kms:us-east-1:123:key/test-key");
        when(uploadRequest.getServerSideEncryptionBucketKey()).thenReturn(true);
        String encContext = Base64.getEncoder().encodeToString("{\"ctx\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        when(uploadRequest.getServerSideEncryptionEncryptionContext()).thenReturn(encContext);

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, uploadRequest);

        CreateMultipartUploadRequest request = builder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals("arn:aws:kms:us-east-1:123:key/test-key", request.ssekmsKeyId());
        assertTrue(request.bucketKeyEnabled());
        assertEquals(encContext, request.ssekmsEncryptionContext());
    }

    // Test configureEncryptionSettings with UploadRequest for PutObjectRequest
    public void testConfigureEncryptionSettingsPutObjectWithUploadRequestAES256() {
        UploadRequest uploadRequest = mock(UploadRequest.class);
        when(uploadRequest.getServerSideEncryptionType()).thenReturn(ServerSideEncryption.AES256.toString());

        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, uploadRequest);

        PutObjectRequest request = builder.build();
        assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
    }

    public void testConfigureEncryptionSettingsPutObjectWithUploadRequestKms() {
        UploadRequest uploadRequest = mock(UploadRequest.class);
        when(uploadRequest.getServerSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(uploadRequest.getServerSideEncryptionKmsKey()).thenReturn("arn:aws:kms:us-east-1:123:key/test-key");
        when(uploadRequest.getServerSideEncryptionBucketKey()).thenReturn(false);
        when(uploadRequest.getServerSideEncryptionEncryptionContext()).thenReturn(null);

        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, uploadRequest);

        PutObjectRequest request = builder.build();
        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals("arn:aws:kms:us-east-1:123:key/test-key", request.ssekmsKeyId());
        assertFalse(request.bucketKeyEnabled());
        assertNull(request.ssekmsEncryptionContext());
    }

    // Test KMS without index key (fallback to repo key)
    public void testConfigureEncryptionSettingsKmsFallbackToRepoKey() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(ServerSideEncryption.AWS_KMS.toString());
        when(blobStore.serverSideEncryptionKmsKey()).thenReturn("arn:aws:kms:us-east-1:123:key/repo-key");
        when(blobStore.serverSideEncryptionBucketKey()).thenReturn(true);
        when(blobStore.serverSideEncryptionEncryptionContext()).thenReturn(null);

        // CryptoMetadata without kms.key_arn
        Settings settings = Settings.builder().put("kms.encryption_context", "tenant=acme").build();
        CryptoMetadata cryptoMetadata = new CryptoMetadata("provider", "aws-kms", settings);

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, cryptoMetadata);

        CreateMultipartUploadRequest request = builder.build();
        // Should use repo key since index key is not set
        assertEquals("arn:aws:kms:us-east-1:123:key/repo-key", request.ssekmsKeyId());
    }

    // Test no encryption type set (neither AES256 nor KMS)
    public void testConfigureEncryptionSettingsNoEncryption() {
        S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.serverSideEncryptionType()).thenReturn(""); // No encryption

        CreateMultipartUploadRequest.Builder builder = CreateMultipartUploadRequest.builder();
        SseKmsUtil.configureEncryptionSettings(builder, blobStore, null);

        CreateMultipartUploadRequest request = builder.build();
        // No encryption should be set
        assertNull(request.serverSideEncryption());
    }
}

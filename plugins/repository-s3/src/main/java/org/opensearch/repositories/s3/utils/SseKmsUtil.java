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
import org.opensearch.common.Nullable;
import org.opensearch.repositories.blobstore.EncryptionContextUtils;
import org.opensearch.repositories.s3.S3BlobStore;
import org.opensearch.repositories.s3.async.UploadRequest;

public class SseKmsUtil {
    /**
     * Merges index-level and repository-level encryption contexts, converts to JSON format if needed,
     * and Base64 encodes for S3.
     * <p>
     * Delegates to centralized EncryptionContextUtils for consistent behavior.
     *
     * @param indexEncContext Index-level encryption context - can be cryptofs or JSON format
     * @param repoEncContext  Repository-level encryption context - already Base64 encoded JSON
     * @return Base64 encoded merged JSON encryption context, or null if both are null
     */
    public static String mergeAndEncodeEncryptionContexts(@Nullable String indexEncContext, @Nullable String repoEncContext) {
        return EncryptionContextUtils.mergeAndEncodeEncryptionContexts(indexEncContext, repoEncContext);
    }

    public static void configureEncryptionSettings(
        CreateMultipartUploadRequest.Builder builder,
        S3BlobStore blobStore,
        @Nullable CryptoMetadata cryptoMetadata
    ) {
        if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AES256.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AES256);
        } else if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AWS_KMS.toString())) {
            String indexKmsKey = null;
            String indexEncContext = null;

            if (cryptoMetadata != null) {
                indexKmsKey = cryptoMetadata.getKeyArn().orElse(null);
                indexEncContext = cryptoMetadata.getEncryptionContext().orElse(null);
            }

            String kmsKey = (indexKmsKey != null) ? indexKmsKey : blobStore.serverSideEncryptionKmsKey();
            String encContext = mergeAndEncodeEncryptionContexts(indexEncContext, blobStore.serverSideEncryptionEncryptionContext());

            builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            builder.ssekmsKeyId(kmsKey);
            builder.bucketKeyEnabled(blobStore.serverSideEncryptionBucketKey());
            builder.ssekmsEncryptionContext(encContext);
        }
    }

    public static void configureEncryptionSettings(
        PutObjectRequest.Builder builder,
        S3BlobStore blobStore,
        @Nullable CryptoMetadata cryptoMetadata
    ) {
        if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AES256.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AES256);
        } else if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AWS_KMS.toString())) {
            String indexKmsKey = null;
            String indexEncContext = null;

            if (cryptoMetadata != null) {
                indexKmsKey = cryptoMetadata.getKeyArn().orElse(null);
                indexEncContext = cryptoMetadata.getEncryptionContext().orElse(null);
            }

            String kmsKey = (indexKmsKey != null) ? indexKmsKey : blobStore.serverSideEncryptionKmsKey();
            String encContext = mergeAndEncodeEncryptionContexts(indexEncContext, blobStore.serverSideEncryptionEncryptionContext());

            builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            builder.ssekmsKeyId(kmsKey);
            builder.bucketKeyEnabled(blobStore.serverSideEncryptionBucketKey());
            builder.ssekmsEncryptionContext(encContext);
        }
    }

    public static void configureEncryptionSettings(PutObjectRequest.Builder builder, S3BlobStore blobStore) {
        configureEncryptionSettings(builder, blobStore, null);
    }

    public static void configureEncryptionSettings(CreateMultipartUploadRequest.Builder builder, UploadRequest uploadRequest) {
        if (uploadRequest.getServerSideEncryptionType().equals(ServerSideEncryption.AES256.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AES256);
        } else if (uploadRequest.getServerSideEncryptionType().equals(ServerSideEncryption.AWS_KMS.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            builder.ssekmsKeyId(uploadRequest.getServerSideEncryptionKmsKey());
            builder.bucketKeyEnabled(uploadRequest.getServerSideEncryptionBucketKey());
            builder.ssekmsEncryptionContext(uploadRequest.getServerSideEncryptionEncryptionContext());
        }
    }

    public static void configureEncryptionSettings(PutObjectRequest.Builder builder, UploadRequest uploadRequest) {
        if (uploadRequest.getServerSideEncryptionType().equals(ServerSideEncryption.AES256.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AES256);
        } else if (uploadRequest.getServerSideEncryptionType().equals(ServerSideEncryption.AWS_KMS.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            builder.ssekmsKeyId(uploadRequest.getServerSideEncryptionKmsKey());
            builder.bucketKeyEnabled(uploadRequest.getServerSideEncryptionBucketKey());
            builder.ssekmsEncryptionContext(uploadRequest.getServerSideEncryptionEncryptionContext());
        }
    }
}

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

import org.opensearch.repositories.s3.S3BlobStore;
import org.opensearch.repositories.s3.async.UploadRequest;

public class SseKmsUtil {

    public static void configureEncryptionSettings(CreateMultipartUploadRequest.Builder builder, S3BlobStore blobStore) {
        if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AES256.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AES256);
        } else if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AWS_KMS.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            builder.ssekmsKeyId(blobStore.serverSideEncryptionKmsKey());
            builder.bucketKeyEnabled(blobStore.serverSideEncryptionBucketKey());
            builder.ssekmsEncryptionContext(blobStore.serverSideEncryptionEncryptionContext());
        }
    }

    public static void configureEncryptionSettings(PutObjectRequest.Builder builder, S3BlobStore blobStore) {
        if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AES256.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AES256);
        } else if (blobStore.serverSideEncryptionType().equals(ServerSideEncryption.AWS_KMS.toString())) {
            builder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
            builder.ssekmsKeyId(blobStore.serverSideEncryptionKmsKey());
            builder.bucketKeyEnabled(blobStore.serverSideEncryptionBucketKey());
            builder.ssekmsEncryptionContext(blobStore.serverSideEncryptionEncryptionContext());
        }
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

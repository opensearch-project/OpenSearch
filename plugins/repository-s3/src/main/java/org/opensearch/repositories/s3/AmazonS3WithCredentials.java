/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import org.opensearch.common.Nullable;

/**
 * The holder of the AmazonS3 and AwsCredentialsProvider
 */
final class AmazonS3WithCredentials {
    private final S3Client client;
    private final AwsCredentialsProvider credentials;

    private AmazonS3WithCredentials(final S3Client client, @Nullable final AwsCredentialsProvider credentials) {
        this.client = client;
        this.credentials = credentials;
    }

    S3Client client() {
        return client;
    }

    AwsCredentialsProvider credentials() {
        return credentials;
    }

    static AmazonS3WithCredentials create(final S3Client client, @Nullable final AwsCredentialsProvider credentials) {
        return new AmazonS3WithCredentials(client, credentials);
    }
}

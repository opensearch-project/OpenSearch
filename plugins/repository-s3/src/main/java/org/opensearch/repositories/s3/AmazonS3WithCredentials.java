/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.AmazonS3;

import org.opensearch.common.Nullable;

/**
 * The holder of the AmazonS3 and AwsCredentialsProvider
 */
final class AmazonS3WithCredentials {
    private final AmazonS3 client;
    private final AwsCredentialsProvider credentials;

    private AmazonS3WithCredentials(final AmazonS3 client, @Nullable final AwsCredentialsProvider credentials) {
        this.client = client;
        this.credentials = credentials;
    }

    AmazonS3 client() {
        return client;
    }

    AwsCredentialsProvider credentials() {
        return credentials;
    }

    static AmazonS3WithCredentials create(final AmazonS3 client, @Nullable final AwsCredentialsProvider credentials) {
        return new AmazonS3WithCredentials(client, credentials);
    }
}

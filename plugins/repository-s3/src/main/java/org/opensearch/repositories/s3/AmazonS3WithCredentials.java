/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

import org.opensearch.common.Nullable;

/**
 * The holder of the AmazonS3 and AWSCredentialsProvider
 */
final class AmazonS3WithCredentials {
    private final AmazonS3 client;
    private final AWSCredentialsProvider credentials;

    private AmazonS3WithCredentials(final AmazonS3 client, @Nullable final AWSCredentialsProvider credentials) {
        this.client = client;
        this.credentials = credentials;
    }

    AmazonS3 client() {
        return client;
    }

    AWSCredentialsProvider credentials() {
        return credentials;
    }

    static AmazonS3WithCredentials create(final AmazonS3 client, @Nullable final AWSCredentialsProvider credentials) {
        return new AmazonS3WithCredentials(client, credentials);
    }
}

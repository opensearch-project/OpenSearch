/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.common.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * The holder of the AmazonS3 and AWSCredentialsProvider
 */
final class AmazonAsyncS3WithCredentials {
    private final S3AsyncClient client;
    private final S3AsyncClient priorityClient;
    private final AwsCredentialsProvider credentials;

    private AmazonAsyncS3WithCredentials(
        final S3AsyncClient client,
        final S3AsyncClient priorityClient,
        @Nullable final AwsCredentialsProvider credentials
    ) {
        this.client = client;
        this.credentials = credentials;
        this.priorityClient = priorityClient;
    }

    S3AsyncClient client() {
        return client;
    }

    S3AsyncClient priorityClient() {
        return priorityClient;
    }

    AwsCredentialsProvider credentials() {
        return credentials;
    }

    static AmazonAsyncS3WithCredentials create(
        final S3AsyncClient client,
        final S3AsyncClient priorityClient,
        @Nullable final AwsCredentialsProvider credentials
    ) {
        return new AmazonAsyncS3WithCredentials(client, priorityClient, credentials);
    }
}

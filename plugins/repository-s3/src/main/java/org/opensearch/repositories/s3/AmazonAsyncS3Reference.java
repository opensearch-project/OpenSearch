/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.concurrent.RefCountedReleasable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.io.Closeable;
import java.io.IOException;

/**
 * Handles the shutdown of the wrapped {@link software.amazon.awssdk.services.s3.S3AsyncClient} using reference
 * counting.
 */
public class AmazonAsyncS3Reference extends RefCountedReleasable<AmazonAsyncS3WithCredentials> {

    private static final Logger logger = LogManager.getLogger(AmazonAsyncS3Reference.class);

    AmazonAsyncS3Reference(AmazonAsyncS3WithCredentials client) {
        super("AWS_S3_CLIENT", client, () -> {
            client.client().close();
            client.priorityClient().close();
            AwsCredentialsProvider credentials = client.credentials();
            if (credentials instanceof Closeable) {
                try {
                    ((Closeable) credentials).close();
                } catch (IOException e) {
                    logger.error("Exception while closing AwsCredentialsProvider", e);
                }
            }
        });
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.RefCountedReleasable;

/**
 * Handles the shutdown of the wrapped {@link S3Client} using reference
 * counting.
 */
public class AmazonS3Reference extends RefCountedReleasable<S3Client> {
    AmazonS3Reference(S3Client client) {
        this(client, null);
    }

    AmazonS3Reference(AmazonS3WithCredentials client) {
        this(client.client(), client.credentials());
    }

    AmazonS3Reference(S3Client client, @Nullable AwsCredentialsProvider credentials) {
        super("AWS_S3_CLIENT", client, () -> {
            client.close();
            if (credentials instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) credentials).close();
                } catch (Exception e) {
                    /* Do nothing here */
                }
            }
        });
    }
}

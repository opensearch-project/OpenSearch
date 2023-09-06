/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import software.amazon.awssdk.services.kms.KmsClient;

import org.opensearch.common.concurrent.RefCountedReleasable;

/**
 * Handles the shutdown of the wrapped {@link KmsClient} using reference
 * counting.
 */
public class AmazonKmsClientReference extends RefCountedReleasable<KmsClient> {

    AmazonKmsClientReference(KmsClient client) {
        super("AWS_KMS_CLIENT", client, client::close);
    }
}

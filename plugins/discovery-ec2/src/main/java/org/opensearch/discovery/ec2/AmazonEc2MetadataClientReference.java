/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery.ec2;

import software.amazon.awssdk.imds.Ec2MetadataClient;

import org.opensearch.common.concurrent.RefCountedReleasable;

/**
 * Handles the shutdown of the wrapped {@link Ec2MetadataClient} using reference counting.
 */
public class AmazonEc2MetadataClientReference extends RefCountedReleasable<Ec2MetadataClient> {

    AmazonEc2MetadataClientReference(Ec2MetadataClient client) {
        super("AWS_EC2_METADATA_CLIENT", client, client::close);
    }
}

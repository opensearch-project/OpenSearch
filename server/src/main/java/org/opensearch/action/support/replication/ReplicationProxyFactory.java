/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.action.support.replication.ReplicationOperation.ReplicationOverridePolicy;

import java.util.Optional;

/**
 * Factory that returns the {@link ReplicationProxy} instance basis the {@link ReplicationOverridePolicy}.
 *
 * @opensearch.internal
 */
public class ReplicationProxyFactory {

    public static <ReplicaRequest> ReplicationProxy<ReplicaRequest> create(final Optional<ReplicationOverridePolicy> overridePolicy) {
        if (overridePolicy.isEmpty()) {
            return new FanoutReplicationProxy<>();
        } else {
            return new ReplicationModeAwareOverrideProxy<>(overridePolicy.get());
        }
    }
}

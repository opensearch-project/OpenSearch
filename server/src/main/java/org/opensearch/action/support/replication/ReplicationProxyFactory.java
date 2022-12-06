/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.index.seqno.ReplicationTracker.ReplicationMode;

import java.util.Optional;

/**
 * Factory that returns the {@link ReplicationProxy} instance basis the {@link ReplicationMode}.
 *
 * @opensearch.internal
 */
public class ReplicationProxyFactory {

    public static <ReplicaRequest> ReplicationProxy<ReplicaRequest> create(final Optional<ReplicationMode> replicationMode) {
        if (replicationMode.isEmpty()) {
            return new FanoutReplicationProxy<>();
        } else {
            return new ReplicationModeAwareProxy<>(replicationMode.get());
        }
    }
}

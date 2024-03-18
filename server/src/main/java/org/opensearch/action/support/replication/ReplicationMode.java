/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.common.annotation.PublicApi;

/**
 * The type of replication used for inter-node replication.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public enum ReplicationMode {
    /**
     * In this mode, a {@code TransportReplicationAction} is fanned out to underlying concerned shard and is replicated logically.
     * In short, this mode would replicate the {@link ReplicationRequest} to
     * the replica shard along with primary term validation.
     */
    FULL_REPLICATION,
    /**
     * In this mode, a {@code TransportReplicationAction} is fanned out to underlying concerned shard and used for
     * primary term validation only. The request is not replicated logically.
     */
    PRIMARY_TERM_VALIDATION,
    /**
     * In this mode, a {@code TransportReplicationAction} does not fan out to the underlying concerned shard.
     */
    NO_REPLICATION;
}

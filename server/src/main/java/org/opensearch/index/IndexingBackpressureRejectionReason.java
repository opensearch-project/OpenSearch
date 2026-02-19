/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

/**
 * Operation type and rejection reason for indexing backpressure metrics.
 * Used by {@link ShardIndexingPressure} and {@link IndexingPressureMetrics}.
 *
 * @opensearch.internal
 */
final class IndexingBackpressureRejectionReason {

    private IndexingBackpressureRejectionReason() {}

    enum OperationType {
        COORDINATING("coordinating"),
        PRIMARY("primary"),
        REPLICA("replica");

        private final String name;

        OperationType(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }
    }

    enum RejectionReason {
        NODE_LIMIT("node_limit"),
        SHARD_LIMIT("shard_limit"),
        THROUGHPUT_DEGRADATION("throughput_degradation"),
        LAST_SUCCESSFUL_REQUEST("last_successful_request");

        private final String name;

        RejectionReason(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }
    }
}

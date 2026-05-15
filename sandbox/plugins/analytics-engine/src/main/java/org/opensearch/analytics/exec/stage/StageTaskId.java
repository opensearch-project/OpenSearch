/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

/**
 * Identity of a single task within a stage. A stage of distribution N emits N tasks —
 * one per shard (SOURCE), one per hash partition (HASH_PARTITIONED), or one total
 * (COORDINATOR). Unique within a query.
 *
 * @opensearch.internal
 */
public record StageTaskId(int stageId, int partitionId) {
    @Override
    public String toString() {
        return stageId + "." + partitionId;
    }
}

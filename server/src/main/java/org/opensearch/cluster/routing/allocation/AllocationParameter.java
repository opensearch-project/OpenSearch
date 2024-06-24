/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

/**
 * Allocation Constraint Parameters
 */
public class AllocationParameter {
    private final float primaryBalanceShardBuffer;

    private final float primaryBalanceIndexBuffer;

    private final float shardBalanceBuffer;

    public AllocationParameter(float shardBalanceBuffer, float primaryBalanceIndexBuffer, float primaryBalanceShardBuffer) {
        this.shardBalanceBuffer = shardBalanceBuffer;
        this.primaryBalanceShardBuffer = primaryBalanceShardBuffer;
        this.primaryBalanceIndexBuffer = primaryBalanceIndexBuffer;
    }

    public float getPrimaryBalanceShardBuffer() {
        return primaryBalanceShardBuffer;
    }

    public float getPrimaryBalanceIndexBuffer() {
        return primaryBalanceIndexBuffer;
    }

    public float getShardBalanceBuffer() {
        return shardBalanceBuffer;
    }

}

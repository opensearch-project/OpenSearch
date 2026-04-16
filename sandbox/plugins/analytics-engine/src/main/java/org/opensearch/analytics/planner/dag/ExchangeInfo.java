/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelDistribution;
import org.opensearch.analytics.planner.rel.ShuffleImpl;

import java.util.List;

/**
 * Exchange metadata extracted from exchange RelNodes during DAG construction.
 * Describes how a child stage delivers data to its parent stage.
 *
 * @param distributionType    distribution type from the exchange operator's trait
 * @param shuffleImpl         shuffle implementation (null for SINGLETON)
 * @param partitionKeyIndices field indices for hash/range partitioning (empty for SINGLETON)
 * @param partitionCount      number of partitions (1 for SINGLETON, N for HASH_DISTRIBUTED; must be &ge; 1)
 * @opensearch.internal
 */
public record ExchangeInfo(RelDistribution.Type distributionType, ShuffleImpl shuffleImpl, List<Integer> partitionKeyIndices,
    int partitionCount) {
    public ExchangeInfo {
        partitionKeyIndices = List.copyOf(partitionKeyIndices);
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1, got " + partitionCount);
        }
        if (distributionType == RelDistribution.Type.HASH_DISTRIBUTED) {
            if (partitionKeyIndices.isEmpty()) {
                throw new IllegalArgumentException("HASH_DISTRIBUTED requires non-empty partitionKeyIndices");
            }
        }
    }

    /**
     * Convenience constructor for non-shuffle exchanges. Sets partitionCount to 1.
     */
    public ExchangeInfo(RelDistribution.Type distributionType, ShuffleImpl shuffleImpl, List<Integer> partitionKeyIndices) {
        this(distributionType, shuffleImpl, partitionKeyIndices, 1);
    }

    public boolean isShuffle() {
        return shuffleImpl != null;
    }
}

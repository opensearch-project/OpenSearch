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
 * @param distributionType distribution type from the exchange operator's trait
 * @param shuffleImpl      shuffle implementation (null for SINGLETON)
 * @param partitionKeyIndices field indices for hash/range partitioning (empty for SINGLETON)
 * @opensearch.internal
 */
public record ExchangeInfo(
    RelDistribution.Type distributionType,
    ShuffleImpl shuffleImpl,
    List<Integer> partitionKeyIndices
) {
    public ExchangeInfo {
        partitionKeyIndices = List.copyOf(partitionKeyIndices);
    }

    public boolean isShuffle() {
        return shuffleImpl != null;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelDistribution;

import java.util.List;

/**
 * Exchange metadata extracted from exchange RelNodes during DAG construction.
 * Describes how a child stage delivers data to its parent stage.
 *
 * <p>TODO: add shuffleImpl and partitionCount when HASH/RANGE shuffle exchanges are implemented.
 *
 * @param distributionType    distribution type from the exchange operator's trait
 * @param partitionKeyIndices field indices for hash/range partitioning (empty for SINGLETON)
 * @opensearch.internal
 */
public record ExchangeInfo(RelDistribution.Type distributionType, List<Integer> partitionKeyIndices) {

    /** Convenience factory for SINGLETON exchanges. */
    public static ExchangeInfo singleton() {
        return new ExchangeInfo(RelDistribution.Type.SINGLETON, List.of());
    }
}

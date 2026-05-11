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
 * @param distributionType    distribution type from the exchange operator's trait
 * @param partitionKeyIndices field indices for hash/range partitioning (empty for SINGLETON)
 * @param partitionCount      number of output partitions (0 for SINGLETON/unpartitioned)
 * @opensearch.internal
 */
public record ExchangeInfo(RelDistribution.Type distributionType, List<Integer> partitionKeyIndices, int partitionCount) {

    /** Convenience factory for SINGLETON exchanges. */
    public static ExchangeInfo singleton() {
        return new ExchangeInfo(RelDistribution.Type.SINGLETON, List.of(), 0);
    }

    /** Convenience factory for HASH_DISTRIBUTED exchanges with given keys and partition count. */
    public static ExchangeInfo hash(List<Integer> keys, int partitionCount) {
        return new ExchangeInfo(RelDistribution.Type.HASH_DISTRIBUTED, keys, partitionCount);
    }
}

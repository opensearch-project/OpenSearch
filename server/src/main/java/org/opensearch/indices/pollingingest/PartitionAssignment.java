/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.metadata.IngestionSource.PartitionStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Computes which source stream partitions a given OpenSearch shard should consume,
 * based on the configured {@link PartitionStrategy}.
 */
public class PartitionAssignment {

    private PartitionAssignment() {
        // utility class
    }

    /**
     * Computes the list of source partition IDs that a shard should consume.
     *
     * @param shardId             the OpenSearch shard ID
     * @param numShards           total number of shards in the index
     * @param numSourcePartitions total number of partitions in the source stream
     * @param strategy            the partition assignment strategy
     * @return unmodifiable list of partition IDs assigned to this shard
     * @throws IllegalArgumentException if numSourcePartitions is less than numShards for FIXED strategy,
     *                                  or if no partitions are assigned to the shard
     */
    public static List<Integer> assignPartitions(int shardId, int numShards, int numSourcePartitions, PartitionStrategy strategy) {
        if (numSourcePartitions <= 0) {
            throw new IllegalArgumentException("Number of source partitions must be positive, got: " + numSourcePartitions);
        }
        assert shardId >= 0 && shardId < numShards : "Shard ID [" + shardId + "] must be >= 0 and < numShards [" + numShards + "]";

        // TODO - support "RANGE" below when we implement https://github.com/opensearch-project/OpenSearch/issues/21267
        switch (strategy) {
            case FIXED:
                if (shardId >= numSourcePartitions) {
                    throw new IllegalArgumentException(
                        "Shard ["
                            + shardId
                            + "] cannot be assigned a partition: source has only ["
                            + numSourcePartitions
                            + "] partitions but shard ID requires partition ["
                            + shardId
                            + "]. Use partition_strategy=auto to map multiple partitions per shard."
                    );
                }
                return List.of(shardId);

            case AUTO:
                if (numSourcePartitions < numShards) {
                    throw new IllegalArgumentException(
                        "Number of source partitions ["
                            + numSourcePartitions
                            + "] must be >= number of shards ["
                            + numShards
                            + "] for auto partition strategy"
                    );
                }
                List<Integer> result = new ArrayList<>();
                for (int p = 0; p < numSourcePartitions; p++) {
                    if (p % numShards == shardId) {
                        result.add(p);
                    }
                }
                return Collections.unmodifiableList(result);

            default:
                throw new IllegalArgumentException("Unsupported partition strategy: " + strategy);
        }
    }
}

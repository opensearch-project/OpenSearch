/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.action.search.SearchShardIterator;
import org.opensearch.cluster.routing.GroupShardsIterator;

import java.util.BitSet;
import java.util.Objects;

/**
 * Immutable result of one index-pruning pass.
 *
 * The result records shard group indexes in the original {@link GroupShardsIterator}. It does not mutate iterators;
 * the caller materializes pruning by marking selected {@link SearchShardIterator}s as skipped.
 */
public final class SearchIndexPruningResult {
    private final GroupShardsIterator<SearchShardIterator> shardIterators;
    private final BitSet prunedShardGroupIndexes;

    private SearchIndexPruningResult(GroupShardsIterator<SearchShardIterator> shardIterators, BitSet prunedShardGroupIndexes) {
        this.shardIterators = Objects.requireNonNull(shardIterators, "shardIterators must not be null");
        this.prunedShardGroupIndexes = copyOf(prunedShardGroupIndexes);
    }

    /**
     * Creates a result representing no pruning.
     */
    public static SearchIndexPruningResult notPruned(GroupShardsIterator<SearchShardIterator> shardIterators) {
        return new SearchIndexPruningResult(shardIterators, new BitSet(shardIterators.size()));
    }

    /**
     * Creates a result containing pruned shard group indexes.
     */
    public static SearchIndexPruningResult pruned(GroupShardsIterator<SearchShardIterator> shardIterators, BitSet prunedShardGroupIndexes) {
        Objects.requireNonNull(shardIterators, "shardIterators must not be null");
        Objects.requireNonNull(prunedShardGroupIndexes, "prunedShardGroupIndexes must not be null");

        int prunedShardGroups = prunedShardGroupIndexes.cardinality();
        if (prunedShardGroups < 1 || prunedShardGroups >= shardIterators.size()) {
            throw new IllegalArgumentException("pruned shard group count must be between 1 and total shard groups - 1");
        }
        if (prunedShardGroupIndexes.length() > shardIterators.size()) {
            throw new IllegalArgumentException("pruned shard group indexes must be within the shard iterator bounds");
        }
        return new SearchIndexPruningResult(shardIterators, prunedShardGroupIndexes);
    }

    /**
     * Original shard iterators supplied to the pruning pass.
     */
    public GroupShardsIterator<SearchShardIterator> shardIterators() {
        return shardIterators;
    }

    /**
     * Number of original shard groups.
     */
    public int originalShardGroups() {
        return shardIterators.size();
    }

    /**
     * Whether the original shard group index was pruned.
     */
    public boolean isPrunedShardGroup(int shardGroupIndex) {
        return prunedShardGroupIndexes.get(shardGroupIndex);
    }

    /**
     * Number of pruned shard groups.
     */
    public int prunedShardGroups() {
        return prunedShardGroupIndexes.cardinality();
    }

    /**
     * Whether at least one shard group was pruned.
     */
    public boolean pruned() {
        return prunedShardGroupIndexes.isEmpty() == false;
    }

    private static BitSet copyOf(BitSet bitSet) {
        return (BitSet) Objects.requireNonNull(bitSet, "bitSet must not be null").clone();
    }
}

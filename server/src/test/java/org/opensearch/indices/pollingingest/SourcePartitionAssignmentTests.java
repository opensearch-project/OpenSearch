/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.metadata.IngestionSource.PartitionStrategy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SourcePartitionAssignmentTests extends OpenSearchTestCase {

    // --- SIMPLE strategy tests ---

    public void testSimpleStrategy_OneToOneMapping() {
        List<Integer> partitions = SourcePartitionAssignment.assignPartitions(0, 4, 4, PartitionStrategy.SIMPLE);
        assertEquals(List.of(0), partitions);

        partitions = SourcePartitionAssignment.assignPartitions(3, 4, 4, PartitionStrategy.SIMPLE);
        assertEquals(List.of(3), partitions);
    }

    public void testSimpleStrategy_MorePartitionsThanShards() {
        // shard 0 still gets partition 0, even if there are more partitions
        List<Integer> partitions = SourcePartitionAssignment.assignPartitions(0, 4, 64, PartitionStrategy.SIMPLE);
        assertEquals(List.of(0), partitions);
    }

    public void testSimpleStrategy_ShardIdExceedsPartitionCount() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SourcePartitionAssignment.assignPartitions(4, 8, 4, PartitionStrategy.SIMPLE)
        );
        assertTrue(e.getMessage().contains("cannot be assigned a partition"));
        assertTrue(e.getMessage().contains("Use partition_strategy=modulo"));
    }

    // --- MODULO strategy tests ---

    public void testModuloStrategy_EqualPartitionsAndShards() {
        // 4 partitions, 4 shards → each shard gets exactly 1 partition (same as simple)
        for (int s = 0; s < 4; s++) {
            List<Integer> partitions = SourcePartitionAssignment.assignPartitions(s, 4, 4, PartitionStrategy.MODULO);
            assertEquals(List.of(s), partitions);
        }
    }

    public void testModuloStrategy_DoublePartitions() {
        // 8 partitions, 4 shards → each shard gets 2 partitions
        assertEquals(List.of(0, 4), SourcePartitionAssignment.assignPartitions(0, 4, 8, PartitionStrategy.MODULO));
        assertEquals(List.of(1, 5), SourcePartitionAssignment.assignPartitions(1, 4, 8, PartitionStrategy.MODULO));
        assertEquals(List.of(2, 6), SourcePartitionAssignment.assignPartitions(2, 4, 8, PartitionStrategy.MODULO));
        assertEquals(List.of(3, 7), SourcePartitionAssignment.assignPartitions(3, 4, 8, PartitionStrategy.MODULO));
    }

    public void testModuloStrategy_ManyPartitions() {
        // 64 partitions, 4 shards → each shard gets 16 partitions
        List<Integer> shard0 = SourcePartitionAssignment.assignPartitions(0, 4, 64, PartitionStrategy.MODULO);
        assertEquals(16, shard0.size());
        assertEquals(0, (int) shard0.get(0));
        assertEquals(4, (int) shard0.get(1));
        assertEquals(60, (int) shard0.get(15));

        List<Integer> shard3 = SourcePartitionAssignment.assignPartitions(3, 4, 64, PartitionStrategy.MODULO);
        assertEquals(16, shard3.size());
        assertEquals(3, (int) shard3.get(0));
        assertEquals(63, (int) shard3.get(15));
    }

    public void testModuloStrategy_SingleShard() {
        // 1 shard → consumes ALL partitions
        List<Integer> partitions = SourcePartitionAssignment.assignPartitions(0, 1, 64, PartitionStrategy.MODULO);
        assertEquals(64, partitions.size());
        for (int i = 0; i < 64; i++) {
            assertEquals(i, (int) partitions.get(i));
        }
    }

    public void testModuloStrategy_UnevenDistribution() {
        // 5 partitions, 3 shards → uneven (shard 0 gets [0,3], shard 1 gets [1,4], shard 2 gets [2])
        assertEquals(List.of(0, 3), SourcePartitionAssignment.assignPartitions(0, 3, 5, PartitionStrategy.MODULO));
        assertEquals(List.of(1, 4), SourcePartitionAssignment.assignPartitions(1, 3, 5, PartitionStrategy.MODULO));
        assertEquals(List.of(2), SourcePartitionAssignment.assignPartitions(2, 3, 5, PartitionStrategy.MODULO));
    }

    public void testModuloStrategy_FewerPartitionsThanShards() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SourcePartitionAssignment.assignPartitions(0, 8, 4, PartitionStrategy.MODULO)
        );
        assertTrue(e.getMessage().contains("must be >= number of shards"));
    }

    // --- Error cases ---

    public void testInvalidShardId() {
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> SourcePartitionAssignment.assignPartitions(-1, 4, 8, PartitionStrategy.MODULO)
        );
        assertTrue(e.getMessage().contains("Shard ID"));
    }

    public void testZeroSourcePartitions() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SourcePartitionAssignment.assignPartitions(0, 4, 0, PartitionStrategy.MODULO)
        );
        assertTrue(e.getMessage().contains("must be positive"));
    }

    public void testResultIsUnmodifiable() {
        List<Integer> partitions = SourcePartitionAssignment.assignPartitions(0, 4, 64, PartitionStrategy.MODULO);
        expectThrows(UnsupportedOperationException.class, () -> partitions.add(99));
    }

    // --- All partitions are covered (completeness check) ---

    public void testAllPartitionsCovered() {
        int numShards = 4;
        int numPartitions = 64;
        boolean[] covered = new boolean[numPartitions];

        for (int s = 0; s < numShards; s++) {
            List<Integer> assigned = SourcePartitionAssignment.assignPartitions(s, numShards, numPartitions, PartitionStrategy.MODULO);
            for (int p : assigned) {
                assertFalse("Partition " + p + " assigned to multiple shards", covered[p]);
                covered[p] = true;
            }
        }

        for (int p = 0; p < numPartitions; p++) {
            assertTrue("Partition " + p + " not assigned to any shard", covered[p]);
        }
    }
}

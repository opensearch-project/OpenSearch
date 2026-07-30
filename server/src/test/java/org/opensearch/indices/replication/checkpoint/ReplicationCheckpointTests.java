/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.lucene.codecs.Codec;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ReplicationCheckpointTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("index", "uuid"), 0);
    private static final String CODEC = Codec.getDefault().getName();

    private ReplicationCheckpoint checkpoint(long primaryTerm, long segmentsGen, long segmentInfosVersion) {
        return new ReplicationCheckpoint(SHARD_ID, primaryTerm, segmentsGen, segmentInfosVersion, CODEC);
    }

    /**
     * Two equal checkpoints must compare as 0 in both directions (reflexive/antisymmetric),
     * otherwise the {@link Comparable} contract is violated.
     */
    public void testCompareToReturnsZeroForEqualCheckpoints() {
        ReplicationCheckpoint a = checkpoint(20, 101, 5);
        ReplicationCheckpoint b = checkpoint(20, 101, 5);
        assertEquals(0, a.compareTo(b));
        assertEquals(0, b.compareTo(a));
    }

    /**
     * The checkpoint that is ahead must sort first (natural order), and the relation must be
     * antisymmetric.
     */
    public void testCompareToOrdersByAheadness() {
        ReplicationCheckpoint older = checkpoint(20, 101, 5);
        ReplicationCheckpoint newerVersion = checkpoint(20, 101, 6);
        ReplicationCheckpoint newerTerm = checkpoint(21, 101, 1);

        assertEquals(-1, newerVersion.compareTo(older));
        assertEquals(1, older.compareTo(newerVersion));
        assertEquals(-1, newerTerm.compareTo(newerVersion));
        assertEquals(1, newerVersion.compareTo(newerTerm));
    }

    /**
     * Regression for the comparator-contract violation. Sorting a large list with many duplicates AND
     * many distinct checkpoints (shuffled) forces TimSort onto its merge path, where a comparator that
     * does not return 0 for equal elements throws
     * "Comparison method violates its general contract!". Must sort cleanly with the fix.
     */
    public void testSortManyCheckpointsHonorsComparatorContract() {
        List<ReplicationCheckpoint> checkpoints = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            // small value range => lots of equal checkpoints interleaved with distinct ones
            checkpoints.add(checkpoint(20, 101, randomLongBetween(1, 20)));
        }
        Collections.shuffle(checkpoints, random());
        checkpoints.sort(Comparator.nullsLast(Comparator.naturalOrder()));
        for (int i = 1; i < checkpoints.size(); i++) {
            assertTrue("list must be totally ordered", checkpoints.get(i - 1).compareTo(checkpoints.get(i)) <= 0);
        }
    }
}

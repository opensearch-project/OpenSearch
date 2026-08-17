/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PackedOrdinalBucketOrdsTests extends OpenSearchTestCase {

    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    public void testBitsRequired() {
        assertEquals(0, PackedOrdinalBucketOrds.bitsRequired(0));
        assertEquals(0, PackedOrdinalBucketOrds.bitsRequired(-1));
        assertEquals(0, PackedOrdinalBucketOrds.bitsRequired(1)); // [0,1) = {0} needs 0 bits
        assertEquals(1, PackedOrdinalBucketOrds.bitsRequired(2)); // [0,2) = {0,1} needs 1 bit
        assertEquals(2, PackedOrdinalBucketOrds.bitsRequired(3)); // [0,3) needs 2 bits
        assertEquals(2, PackedOrdinalBucketOrds.bitsRequired(4)); // [0,4) needs 2 bits
        assertEquals(3, PackedOrdinalBucketOrds.bitsRequired(5)); // [0,5) needs 3 bits
        assertEquals(3, PackedOrdinalBucketOrds.bitsRequired(8)); // [0,8) needs 3 bits
        assertEquals(20, PackedOrdinalBucketOrds.bitsRequired(1_000_000));
        assertEquals(30, PackedOrdinalBucketOrds.bitsRequired(1L << 30));
    }

    public void testFitsInSingleLong() {
        assertTrue(PackedOrdinalBucketOrds.fitsInSingleLong(new long[] { 1000, 1000 }));
        // 31 + 32 = 63 bits — exactly fits
        assertTrue(PackedOrdinalBucketOrds.fitsInSingleLong(new long[] { 1L << 31, 1L << 32 }));
        // 32 + 32 = 64 bits — does not fit
        assertFalse(PackedOrdinalBucketOrds.fitsInSingleLong(new long[] { 1L << 32, 1L << 32 }));
    }

    public void testFitsInTwoLongs() {
        assertTrue(PackedOrdinalBucketOrds.fitsInTwoLongs(new long[] { 1000, 1000 }));
        // 3 fields at 42 bits each = 126 — exactly fits
        assertTrue(PackedOrdinalBucketOrds.fitsInTwoLongs(new long[] { 1L << 42, 1L << 42, 1L << 42 }));
        // 3 fields at 43 bits each = 129 — does not fit
        assertFalse(PackedOrdinalBucketOrds.fitsInTwoLongs(new long[] { 1L << 43, 1L << 43, 1L << 43 }));
    }

    public void testSingleLongPathRoundTrip() {
        long[] maxOrds = { 1000, 1000 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            assertTrue(ords.isSingleLongPath());

            long[] ordinals = { 500, 300 };
            long packed = ords.packSingleLong(ordinals);
            long[] unpacked = ords.unpackSingleLong(packed);
            assertArrayEquals(ordinals, unpacked);
        }
    }

    public void testTwoLongPathRoundTrip() {
        // 40 bits each × 3 = 120 bits total — needs two longs
        long[] maxOrds = { 1L << 40, 1L << 40, 1L << 40 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            assertFalse(ords.isSingleLongPath());
            assertTrue(ords.totalBits() > 63);

            long[] ordinals = { (1L << 39), (1L << 39), (1L << 39) };
            long long1 = ords.packLong1(ordinals);
            long long2 = ords.packLong2(ordinals);
            long[] unpacked = ords.unpackTwoLongs(long1, long2);
            assertArrayEquals(ordinals, unpacked);
        }
    }

    public void testBoundaryCaseSingleLong63Bits() {
        // 31 + 31 + 1 = 63 bits — exactly at the boundary
        long[] maxOrds = { 1L << 31, 1L << 31, 1L << 1 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            assertTrue(ords.isSingleLongPath());
            assertEquals(63, ords.totalBits());

            long[] ordinals = { (1L << 31) - 1, (1L << 31) - 1, 1 };
            long packed = ords.packSingleLong(ordinals);
            long[] unpacked = ords.unpackSingleLong(packed);
            assertArrayEquals(ordinals, unpacked);
        }
    }

    public void testStraddlingFieldsTwoLongs() {
        // 50 + 50 = 100 bits — second field straddles the 63-bit boundary
        long[] maxOrds = { 1L << 50, 1L << 50 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            assertFalse(ords.isSingleLongPath());

            long[] ordinals = { (1L << 49), (1L << 49) };
            long long1 = ords.packLong1(ordinals);
            long long2 = ords.packLong2(ordinals);
            long[] unpacked = ords.unpackTwoLongs(long1, long2);
            assertArrayEquals(ordinals, unpacked);
        }
    }

    public void testAddAndRetrieveSingleLong() {
        long[] maxOrds = { 1000, 1000 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            long[] ordinals1 = { 100, 200 };
            long[] ordinals2 = { 300, 400 };

            long bucketOrd1 = ords.add(0, ordinals1);
            long bucketOrd2 = ords.add(0, ordinals2);
            long bucketOrd3 = ords.add(0, ordinals1); // duplicate

            assertTrue(bucketOrd1 >= 0);
            assertTrue(bucketOrd2 >= 0);
            assertEquals(-1 - bucketOrd1, bucketOrd3);

            assertEquals(2, ords.size());
            assertEquals(2, ords.bucketsInOrd(0));

            MultiTermsBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            List<long[]> retrieved = new ArrayList<>();
            while (ordsEnum.next()) {
                retrieved.add(ordsEnum.ordinals());
            }
            assertEquals(2, retrieved.size());
            assertTrue(containsArray(retrieved, ordinals1));
            assertTrue(containsArray(retrieved, ordinals2));
        }
    }

    public void testAddAndRetrieveTwoLongs() {
        long[] maxOrds = { 1L << 40, 1L << 40, 1L << 40 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            long[] ordinals1 = { 1L << 39, 1L << 39, 1L << 39 };
            long[] ordinals2 = { 100, 200, 300 };

            long bucketOrd1 = ords.add(0, ordinals1);
            long bucketOrd2 = ords.add(0, ordinals2);
            long bucketOrd3 = ords.add(0, ordinals1); // duplicate

            assertTrue(bucketOrd1 >= 0);
            assertTrue(bucketOrd2 >= 0);
            assertEquals(-1 - bucketOrd1, bucketOrd3);

            assertEquals(2, ords.size());
            assertEquals(2, ords.bucketsInOrd(0));

            MultiTermsBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            List<long[]> retrieved = new ArrayList<>();
            while (ordsEnum.next()) {
                retrieved.add(ordsEnum.ordinals());
            }
            assertEquals(2, retrieved.size());
            assertTrue(containsArray(retrieved, ordinals1));
            assertTrue(containsArray(retrieved, ordinals2));
        }
    }

    public void testManyFieldsSingleLong() {
        // 6 fields at 4 bits each = 24 bits total
        long[] maxOrds = { 10, 10, 10, 10, 10, 10 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.ONE, maxOrds)) {
            assertTrue(ords.isSingleLongPath());

            long[] ordinals = { 5, 5, 5, 5, 5, 5 };
            long packed = ords.packSingleLong(ordinals);
            long[] unpacked = ords.unpackSingleLong(packed);
            assertArrayEquals(ordinals, unpacked);
        }
    }

    public void testMultipleOwningBucketsSingleLong() {
        // Verify that LongKeyedBucketOrds (single-long path) correctly partitions by owningBucketOrd.
        long[] maxOrds = { 1000, 1000 };
        try (PackedOrdinalBucketOrds ords = new PackedOrdinalBucketOrds(bigArrays, CardinalityUpperBound.MANY, maxOrds)) {
            assertTrue(ords.isSingleLongPath());

            long[] ordsA = { 1, 2 };
            long[] ordsB = { 1, 2 }; // same ordinals but different owning bucket
            long[] ordsC = { 3, 4 };

            long id0A = ords.add(0, ordsA);
            long id1A = ords.add(1, ordsA);
            long id0C = ords.add(0, ordsC);

            // Same ordinals in different owning buckets must get independent IDs
            assertNotEquals("Different owning buckets must produce independent bucket ords", id0A, id1A);
            assertTrue(id0A >= 0);
            assertTrue(id1A >= 0);
            assertTrue(id0C >= 0);

            // Duplicate in same owning bucket returns negated id
            long id0ADup = ords.add(0, ordsA);
            assertTrue("Duplicate should return negated id", id0ADup < 0);
            assertEquals(-1 - id0A, id0ADup);

            // owning bucket 0 should have 2 entries; owning bucket 1 should have 1
            assertEquals(2, ords.bucketsInOrd(0));
            assertEquals(1, ords.bucketsInOrd(1));
        }
    }

    /** Helper: check if a list of long[] contains an array with matching content. */
    private static boolean containsArray(List<long[]> list, long[] target) {
        for (long[] item : list) {
            if (Arrays.equals(item, target)) {
                return true;
            }
        }
        return false;
    }
}

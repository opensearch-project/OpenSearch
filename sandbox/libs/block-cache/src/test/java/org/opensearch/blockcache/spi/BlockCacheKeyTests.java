/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.spi;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for {@link BlockCacheKey}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Namespacing across {@code repositoryType} and {@code repositoryName} (requirements 9.1, 9.2).</li>
 *   <li>Range validation on construction (requirement 1.6).</li>
 *   <li>{@code equals} / {@code hashCode} contract (requirement 9.3).</li>
 *   <li>Null-component rejection on the three string fields (requirement 1.6 precondition).</li>
 * </ul>
 */
public class BlockCacheKeyTests {

    // ── Namespacing (P6) ──────────────────────────────────────────────────────

    @Test
    public void testDifferentTypesAreNeverEqual() {
        BlockCacheKey s3 = new BlockCacheKey("s3", "repo", "k", 0, 1);
        BlockCacheKey gcs = new BlockCacheKey("gcs", "repo", "k", 0, 1);
        assertNotEquals("types differ → keys must differ", s3, gcs);
        // Hash codes are allowed to collide, but for records derived from distinct
        // String components, they almost certainly won't. The contract requires
        // inequality of equals() only.
    }

    @Test
    public void testDifferentNamesAreNeverEqual() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo-a", "k", 0, 1);
        BlockCacheKey b = new BlockCacheKey("s3", "repo-b", "k", 0, 1);
        assertNotEquals("names differ → keys must differ", a, b);
    }

    @Test
    public void testDifferentObjectKeysAreNeverEqual() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo", "object-a", 0, 1);
        BlockCacheKey b = new BlockCacheKey("s3", "repo", "object-b", 0, 1);
        assertNotEquals(a, b);
    }

    @Test
    public void testDifferentRangesAreNeverEqual() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo", "k", 0, 100);
        BlockCacheKey b = new BlockCacheKey("s3", "repo", "k", 0, 200);
        assertNotEquals(a, b);
    }

    // ── Range validation (requirement 1.6) ────────────────────────────────────

    @Test
    public void testRejectsEmptyRange() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> new BlockCacheKey("s3", "repo", "k", 10, 10));
        assertEquals("rangeEnd must be > rangeStart", ex.getMessage());
    }

    @Test
    public void testRejectsInvertedRange() {
        assertThrows(IllegalArgumentException.class, () -> new BlockCacheKey("s3", "repo", "k", 10, 5));
    }

    @Test
    public void testAcceptsSingleByteRange() {
        // rangeEnd == rangeStart + 1 is the smallest legal range.
        BlockCacheKey k = new BlockCacheKey("s3", "repo", "k", 0, 1);
        assertEquals(1L, k.rangeEnd() - k.rangeStart());
    }

    @Test
    public void testAcceptsLargeRange() {
        BlockCacheKey k = new BlockCacheKey("s3", "repo", "k", 0L, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, k.rangeEnd());
    }

    @Test
    public void testAcceptsNegativeStartWithLargerEnd() {
        // The contract is rangeEnd > rangeStart; both can be negative.
        BlockCacheKey k = new BlockCacheKey("s3", "repo", "k", -100, -50);
        assertEquals(50L, k.rangeEnd() - k.rangeStart());
    }

    // ── Null-component rejection ──────────────────────────────────────────────

    @Test
    public void testRejectsNullRepositoryType() {
        assertThrows(NullPointerException.class, () -> new BlockCacheKey(null, "repo", "k", 0, 1));
    }

    @Test
    public void testRejectsNullRepositoryName() {
        assertThrows(NullPointerException.class, () -> new BlockCacheKey("s3", null, "k", 0, 1));
    }

    @Test
    public void testRejectsNullObjectKey() {
        assertThrows(NullPointerException.class, () -> new BlockCacheKey("s3", "repo", null, 0, 1));
    }

    // ── equals / hashCode contract (requirement 9.3) ──────────────────────────

    @Test
    public void testIdenticalComponentsAreEqual() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo", "object/key", 100, 200);
        BlockCacheKey b = new BlockCacheKey("s3", "repo", "object/key", 100, 200);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testEqualsIsReflexive() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo", "k", 0, 1);
        assertEquals(a, a);
    }

    @Test
    public void testNotEqualToNull() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo", "k", 0, 1);
        assertNotEquals(a, null);
    }

    @Test
    public void testNotEqualToDifferentType() {
        BlockCacheKey a = new BlockCacheKey("s3", "repo", "k", 0, 1);
        assertNotEquals(a, "not a BlockCacheKey");
    }

    // ── Component accessors ───────────────────────────────────────────────────

    @Test
    public void testAccessorsReturnOriginalValues() {
        BlockCacheKey k = new BlockCacheKey("s3", "my-repo", "some/object", 1024L, 2048L);
        assertEquals("s3", k.repositoryType());
        assertEquals("my-repo", k.repositoryName());
        assertEquals("some/object", k.objectKey());
        assertEquals(1024L, k.rangeStart());
        assertEquals(2048L, k.rangeEnd());
    }
}

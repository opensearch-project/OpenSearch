/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link PartitionLanePolicy} parsing, lane resolution, and validation.
 */
public class PartitionLanePolicyTests extends OpenSearchTestCase {

    // ── parse(...) accepts canonical strings ──

    public void testParsePerShardCanonical() {
        assertSame(PartitionLanePolicy.PER_SHARD, PartitionLanePolicy.parse("per_shard"));
    }

    public void testParseSingleCanonical() {
        assertSame(PartitionLanePolicy.SINGLE, PartitionLanePolicy.parse("single"));
    }

    public void testParseCapWithIntegerParameter() {
        PartitionLanePolicy policy = PartitionLanePolicy.parse("cap:8");
        assertEquals(PartitionLanePolicy.Kind.CAP, policy.kind());
        assertEquals(8, policy.parameter());
    }

    public void testParseRatioWithIntegerParameter() {
        PartitionLanePolicy policy = PartitionLanePolicy.parse("ratio:4");
        assertEquals(PartitionLanePolicy.Kind.RATIO, policy.kind());
        assertEquals(4, policy.parameter());
    }

    // ── parse(...) is lenient on whitespace and case ──

    public void testParseTolerantToCaseAndWhitespace() {
        assertSame(PartitionLanePolicy.PER_SHARD, PartitionLanePolicy.parse("PER_SHARD"));
        assertSame(PartitionLanePolicy.PER_SHARD, PartitionLanePolicy.parse(" per_shard "));
        assertSame(PartitionLanePolicy.SINGLE, PartitionLanePolicy.parse("Single"));
        PartitionLanePolicy cap = PartitionLanePolicy.parse(" CAP : 16 ");
        assertEquals(PartitionLanePolicy.Kind.CAP, cap.kind());
        assertEquals(16, cap.parameter());
    }

    // ── parse(...) rejects malformed strings with helpful messages ──

    public void testParseNullThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse(null));
        assertTrue(e.getMessage().contains("must not be null"));
    }

    public void testParseEmptyStringThrows() {
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse(""));
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("   "));
    }

    public void testParseUnknownPolicyNameThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("round_robin"));
        assertTrue("error must list valid policies: " + e.getMessage(), e.getMessage().contains("per_shard"));
    }

    public void testParseUnknownPolicyWithColonThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("split:4"));
        assertTrue(e.getMessage().contains("split"));
    }

    public void testParseCapWithNonIntegerParameterThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("cap:abc"));
        assertTrue(e.getMessage().contains("requires an integer"));
    }

    public void testParseCapWithMissingParameterThrows() {
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("cap:"));
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse(":4"));
    }

    public void testParseCapWithZeroOrNegativeThrows() {
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("cap:0"));
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("cap:-1"));
    }

    public void testParseRatioWithZeroOrNegativeThrows() {
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("ratio:0"));
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.parse("ratio:-2"));
    }

    public void testParseExceedingMaxParameterThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionLanePolicy.parse("cap:" + (PartitionLanePolicy.MAX_PARAMETER + 1))
        );
        assertTrue(e.getMessage().contains("must be <= " + PartitionLanePolicy.MAX_PARAMETER));
    }

    // ── resolveLanes() lane-count math ──

    public void testPerShardReturnsProducerCount() {
        PartitionLanePolicy p = PartitionLanePolicy.PER_SHARD;
        assertEquals(1, p.resolveLanes(1));
        assertEquals(4, p.resolveLanes(4));
        assertEquals(50, p.resolveLanes(50));
    }

    public void testSingleAlwaysReturnsOne() {
        PartitionLanePolicy p = PartitionLanePolicy.SINGLE;
        assertEquals(1, p.resolveLanes(1));
        assertEquals(1, p.resolveLanes(4));
        assertEquals(1, p.resolveLanes(50));
    }

    public void testCapClampsAtParameter() {
        PartitionLanePolicy p = PartitionLanePolicy.cap(8);
        assertEquals(1, p.resolveLanes(1));
        assertEquals(4, p.resolveLanes(4));
        assertEquals(8, p.resolveLanes(8));
        assertEquals(8, p.resolveLanes(9));
        assertEquals(8, p.resolveLanes(50));
    }

    public void testRatioFloorsAtOne() {
        PartitionLanePolicy p = PartitionLanePolicy.ratio(4);
        // 1 producer / 4 → 0, but we floor at 1.
        assertEquals(1, p.resolveLanes(1));
        assertEquals(1, p.resolveLanes(3));
        assertEquals(1, p.resolveLanes(4));
        assertEquals(2, p.resolveLanes(8));
        assertEquals(12, p.resolveLanes(50));
    }

    public void testResolveLanesClampsZeroOrNegativeProducerCount() {
        // The factory normalises producer count to >= 1 before calling resolveLanes(),
        // but the helper should still produce a valid (>= 1) lane count defensively.
        for (PartitionLanePolicy p : new PartitionLanePolicy[] {
            PartitionLanePolicy.PER_SHARD,
            PartitionLanePolicy.SINGLE,
            PartitionLanePolicy.cap(8),
            PartitionLanePolicy.ratio(4), }) {
            assertEquals("policy " + p + " must clamp 0 producers to 1 lane", 1, p.resolveLanes(0));
            assertEquals("policy " + p + " must clamp -1 producers to 1 lane", 1, p.resolveLanes(-1));
        }
    }

    // ── Factory method validation ──

    public void testCapFactoryRejectsZeroOrNegative() {
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.cap(0));
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.cap(-1));
    }

    public void testRatioFactoryRejectsZeroOrNegative() {
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.ratio(0));
        expectThrows(IllegalArgumentException.class, () -> PartitionLanePolicy.ratio(-3));
    }

    // ── Round-trip + equality ──

    public void testWireStringRoundTripsForAllShapes() {
        for (PartitionLanePolicy p : new PartitionLanePolicy[] {
            PartitionLanePolicy.PER_SHARD,
            PartitionLanePolicy.SINGLE,
            PartitionLanePolicy.cap(8),
            PartitionLanePolicy.ratio(4), }) {
            assertEquals("round-trip failed for " + p, p, PartitionLanePolicy.parse(p.toWireString()));
        }
    }

    public void testEqualsAndHashCode() {
        assertEquals(PartitionLanePolicy.cap(8), PartitionLanePolicy.cap(8));
        assertEquals(PartitionLanePolicy.cap(8).hashCode(), PartitionLanePolicy.cap(8).hashCode());
        assertNotEquals(PartitionLanePolicy.cap(8), PartitionLanePolicy.cap(16));
        assertNotEquals(PartitionLanePolicy.cap(8), PartitionLanePolicy.ratio(8));
        assertNotEquals(PartitionLanePolicy.PER_SHARD, PartitionLanePolicy.SINGLE);
    }
}

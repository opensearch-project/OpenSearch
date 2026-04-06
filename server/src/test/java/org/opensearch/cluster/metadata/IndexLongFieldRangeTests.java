/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class IndexLongFieldRangeTests extends OpenSearchTestCase {

    public void testUnknownState() {
        IndexLongFieldRange range = IndexLongFieldRange.UNKNOWN;
        assertTrue(range.isUnknown());
        assertFalse(range.isEmpty());
        assertEquals(IndexLongFieldRange.Relation.UNKNOWN, range.relation(0, 100));
    }

    public void testEmptyState() {
        IndexLongFieldRange range = IndexLongFieldRange.EMPTY;
        assertFalse(range.isUnknown());
        assertTrue(range.isEmpty());
        assertEquals(IndexLongFieldRange.Relation.DISJOINT, range.relation(0, 100));
    }

    public void testConcreteRange() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertFalse(range.isUnknown());
        assertFalse(range.isEmpty());
        assertEquals(100, range.getMin());
        assertEquals(200, range.getMax());
    }

    public void testInvalidRange() {
        expectThrows(IllegalArgumentException.class, () -> IndexLongFieldRange.of(200, 100));
    }

    public void testRelationDisjointBefore() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.DISJOINT, range.relation(0, 99));
    }

    public void testRelationDisjointAfter() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.DISJOINT, range.relation(201, 300));
    }

    public void testRelationWithin() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.WITHIN, range.relation(50, 250));
    }

    public void testRelationWithinExact() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.WITHIN, range.relation(100, 200));
    }

    public void testRelationIntersectsLeft() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.INTERSECTS, range.relation(50, 150));
    }

    public void testRelationIntersectsRight() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.INTERSECTS, range.relation(150, 250));
    }

    public void testRelationIntersectsContains() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        assertEquals(IndexLongFieldRange.Relation.INTERSECTS, range.relation(120, 180));
    }

    public void testExtendWithValueFromUnknown() {
        IndexLongFieldRange result = IndexLongFieldRange.UNKNOWN.extendWithValue(150);
        assertFalse(result.isUnknown());
        assertEquals(150, result.getMin());
        assertEquals(150, result.getMax());
    }

    public void testExtendWithValueFromEmpty() {
        IndexLongFieldRange result = IndexLongFieldRange.EMPTY.extendWithValue(150);
        assertFalse(result.isEmpty());
        assertEquals(150, result.getMin());
        assertEquals(150, result.getMax());
    }

    public void testExtendWithValueExpands() {
        IndexLongFieldRange range = IndexLongFieldRange.of(100, 200);
        IndexLongFieldRange result = range.extendWithValue(50);
        assertEquals(50, result.getMin());
        assertEquals(200, result.getMax());

        result = range.extendWithValue(250);
        assertEquals(100, result.getMin());
        assertEquals(250, result.getMax());
    }

    public void testUnionBothKnown() {
        IndexLongFieldRange a = IndexLongFieldRange.of(100, 200);
        IndexLongFieldRange b = IndexLongFieldRange.of(150, 300);
        IndexLongFieldRange result = a.union(b);
        assertEquals(100, result.getMin());
        assertEquals(300, result.getMax());
    }

    public void testUnionWithUnknown() {
        IndexLongFieldRange a = IndexLongFieldRange.of(100, 200);
        IndexLongFieldRange result = a.union(IndexLongFieldRange.UNKNOWN);
        assertTrue(result.isUnknown());

        result = IndexLongFieldRange.UNKNOWN.union(a);
        assertTrue(result.isUnknown());
    }

    public void testUnionWithEmpty() {
        IndexLongFieldRange a = IndexLongFieldRange.of(100, 200);
        IndexLongFieldRange result = a.union(IndexLongFieldRange.EMPTY);
        assertEquals(100, result.getMin());
        assertEquals(200, result.getMax());

        result = IndexLongFieldRange.EMPTY.union(a);
        assertEquals(100, result.getMin());
        assertEquals(200, result.getMax());
    }

    public void testSerializationRoundTripUnknown() throws IOException {
        assertSerializationRoundTrip(IndexLongFieldRange.UNKNOWN);
    }

    public void testSerializationRoundTripEmpty() throws IOException {
        assertSerializationRoundTrip(IndexLongFieldRange.EMPTY);
    }

    public void testSerializationRoundTripRange() throws IOException {
        assertSerializationRoundTrip(IndexLongFieldRange.of(1000, 2000));
    }

    public void testEquality() {
        assertEquals(IndexLongFieldRange.UNKNOWN, IndexLongFieldRange.UNKNOWN);
        assertEquals(IndexLongFieldRange.EMPTY, IndexLongFieldRange.EMPTY);
        assertEquals(IndexLongFieldRange.of(100, 200), IndexLongFieldRange.of(100, 200));
        assertNotEquals(IndexLongFieldRange.of(100, 200), IndexLongFieldRange.of(100, 300));
        assertNotEquals(IndexLongFieldRange.UNKNOWN, IndexLongFieldRange.EMPTY);
    }

    private void assertSerializationRoundTrip(IndexLongFieldRange original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        IndexLongFieldRange deserialized = new IndexLongFieldRange(in);
        assertEquals(original, deserialized);
    }
}

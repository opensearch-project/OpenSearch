/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class LongSetTests extends OpenSearchTestCase {

    public void testCanonicalisesToSortedDistinct() {
        LongSet set = new LongSet("orderkey", new long[] { 5, 1, 5, 3, 1 });
        assertArrayEquals(new long[] { 1, 3, 5 }, set.values());
        assertEquals(1L, set.min());
        assertEquals(5L, set.max());
    }

    public void testRejectsEmpty() {
        // An empty build side is a whole-query short circuit, not a filter that
        // prunes everything — see the class doc. It must not be constructible.
        expectThrows(IllegalArgumentException.class, () -> new LongSet("k", new long[0]));
        // De-duplication cannot produce an empty set from a non-empty input, so
        // the only route to empty is an empty input.
        expectThrows(NullPointerException.class, () -> new LongSet("k", null));
        expectThrows(NullPointerException.class, () -> new LongSet(null, new long[] { 1 }));
    }

    public void testRoundTripThroughSerializer() throws IOException {
        LongSet original = new LongSet("l_orderkey", new long[] { 7, 3, 11 });
        byte[] bytes = CanMatchFilterSerializer.serialize(List.of(original));
        List<CanMatchFilter> decoded = CanMatchFilterSerializer.deserialize(bytes);

        assertEquals(1, decoded.size());
        assertEquals(original, decoded.get(0));
        LongSet set = (LongSet) decoded.get(0);
        assertEquals("l_orderkey", set.column());
        assertArrayEquals(new long[] { 3, 7, 11 }, set.values());
    }

    public void testRoundTripMixedWithRange() throws IOException {
        // AND semantics across the list, so a runtime filter riding alongside a
        // WHERE-derived range has to survive the same envelope.
        List<CanMatchFilter> originals = List.of(new LongRange("@timestamp", 100L, 200L), new LongSet("custkey", new long[] { 42, 7 }));
        List<CanMatchFilter> decoded = CanMatchFilterSerializer.deserialize(CanMatchFilterSerializer.serialize(originals));

        assertEquals(2, decoded.size());
        assertTrue(decoded.get(0) instanceof LongRange);
        assertEquals(new LongSet("custkey", new long[] { 7, 42 }), decoded.get(1));
    }

    public void testRoundTripPreservesExtremeValues() throws IOException {
        LongSet original = new LongSet("k", new long[] { Long.MIN_VALUE, 0, Long.MAX_VALUE });
        LongSet decoded = (LongSet) CanMatchFilterSerializer.deserialize(CanMatchFilterSerializer.serialize(List.of(original))).get(0);
        assertArrayEquals(new long[] { Long.MIN_VALUE, 0, Long.MAX_VALUE }, decoded.values());
    }

    public void testEqualityIsByValueNotIdentity() {
        // A record with an array component gets identity equals from the compiler,
        // which would make two equal filters compare unequal. Overridden, so check it.
        assertEquals(new LongSet("k", new long[] { 1, 2 }), new LongSet("k", new long[] { 2, 1 }));
        assertEquals(new LongSet("k", new long[] { 1, 2 }).hashCode(), new LongSet("k", new long[] { 2, 1 }).hashCode());
        assertNotEquals(new LongSet("k", new long[] { 1, 2 }), new LongSet("k", new long[] { 1, 3 }));
        assertNotEquals(new LongSet("a", new long[] { 1 }), new LongSet("b", new long[] { 1 }));
    }

    public void testToStringSummarisesRatherThanDumpingValues() {
        // These can hold thousands of keys; a log line must stay readable.
        String rendered = new LongSet("k", new long[] { 3, 1, 2 }).toString();
        assertEquals("LongSet[column=k, size=3, min=1, max=3]", rendered);
    }
}

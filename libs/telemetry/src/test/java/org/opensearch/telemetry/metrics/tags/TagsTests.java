/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.tags;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class TagsTests extends OpenSearchTestCase {

    // --- EMPTY ---

    public void testEmptyHasZeroSize() {
        assertEquals(0, Tags.EMPTY.size());
    }

    public void testEmptyToMapReturnsEmptyMap() {
        assertTrue(Tags.EMPTY.toMap().isEmpty());
    }

    public void testEmptyGetTagsMapReturnsEmptyMap() {
        assertTrue(Tags.EMPTY.getTagsMap().isEmpty());
    }

    // --- of(key, value) ---

    public void testOfSingleTag() {
        Tags t = Tags.of("env", "prod");
        assertEquals(1, t.size());
        assertEquals("env", t.getKey(0));
        assertEquals("prod", t.getValue(0));
    }

    public void testOfNullKeyThrows() {
        expectThrows(NullPointerException.class, () -> Tags.of(null, "v"));
    }

    public void testOfNullValueThrows() {
        expectThrows(NullPointerException.class, () -> Tags.of("k", (Object) null));
    }

    // --- of(k1,v1,k2,v2) sorted ---

    public void testOfTwoTagsSorted() {
        Tags t = Tags.of("z", "1", "a", "2");
        assertEquals(2, t.size());
        assertEquals("a", t.getKey(0));
        assertEquals("z", t.getKey(1));
    }

    public void testOfTwoTagsDuplicateKeyLastWins() {
        Tags t = Tags.of("k", "first", "k", "second");
        assertEquals(1, t.size());
        assertEquals("second", t.getValue(0));
    }

    // --- of(k1,v1,k2,v2,k3,v3) ---

    public void testOfThreeTagsSorted() {
        Tags t = Tags.of("c", "3", "a", "1", "b", "2");
        assertEquals(3, t.size());
        assertEquals("a", t.getKey(0));
        assertEquals("b", t.getKey(1));
        assertEquals("c", t.getKey(2));
    }

    // --- of(varargs) ---

    public void testOfVarargsEmpty() {
        assertSame(Tags.EMPTY, Tags.of(new String[0]));
    }

    public void testOfVarargsOddLengthThrows() {
        expectThrows(IllegalArgumentException.class, () -> Tags.of("a", "b", "c"));
    }

    public void testOfVarargsSorted() {
        Tags t = Tags.of("z", "1", "a", "2", "m", "3");
        assertEquals(3, t.size());
        assertEquals("a", t.getKey(0));
        assertEquals("m", t.getKey(1));
        assertEquals("z", t.getKey(2));
    }

    // --- concat ---

    public void testConcatMergesTwoTags() {
        Tags a = Tags.of("a", "1");
        Tags b = Tags.of("b", "2");
        Tags merged = Tags.concat(a, b);
        assertEquals(2, merged.size());
        assertEquals("a", merged.getKey(0));
        assertEquals("b", merged.getKey(1));
    }

    public void testConcatBWinsOnCollision() {
        Tags a = Tags.of("k", "old");
        Tags b = Tags.of("k", "new");
        Tags merged = Tags.concat(a, b);
        assertEquals(1, merged.size());
        assertEquals("new", merged.getValue(0));
    }

    public void testConcatWithNullReturnsOther() {
        Tags a = Tags.of("k", "v");
        assertSame(a, Tags.concat(a, null));
        assertSame(a, Tags.concat(null, a));
    }

    public void testConcatWithEmptyReturnsOther() {
        Tags a = Tags.of("k", "v");
        assertSame(a, Tags.concat(a, Tags.EMPTY));
        assertSame(a, Tags.concat(Tags.EMPTY, a));
    }

    public void testConcatBothNullReturnsEmpty() {
        assertSame(Tags.EMPTY, Tags.concat(null, null));
    }

    public void testConcatPartialOverlapMergesAndDeduplicates() {
        Tags a = Tags.of("a", "1", "c", "3");
        Tags b = Tags.of("b", "2", "c", "4");
        Tags merged = Tags.concat(a, b);
        assertEquals(3, merged.size());
        assertEquals("a", merged.getKey(0));
        assertEquals("1", merged.getValue(0));
        assertEquals("b", merged.getKey(1));
        assertEquals("2", merged.getValue(1));
        assertEquals("c", merged.getKey(2));
        assertEquals("4", merged.getValue(2));
    }

    public void testConcatInterleavedNoOverlap() {
        Tags a = Tags.of("a", "1", "c", "3", "e", "5");
        Tags b = Tags.of("b", "2", "d", "4");
        Tags merged = Tags.concat(a, b);
        assertEquals(5, merged.size());
        assertEquals("a", merged.getKey(0));
        assertEquals("b", merged.getKey(1));
        assertEquals("c", merged.getKey(2));
        assertEquals("d", merged.getKey(3));
        assertEquals("e", merged.getKey(4));
    }

    public void testConcatFullOverlapBWins() {
        Tags a = Tags.of("a", "old_a", "b", "old_b");
        Tags b = Tags.of("a", "new_a", "b", "new_b");
        Tags merged = Tags.concat(a, b);
        assertEquals(2, merged.size());
        assertEquals("new_a", merged.getValue(0));
        assertEquals("new_b", merged.getValue(1));
    }

    public void testConcatLargeRemainderPath() {
        Tags a = Tags.of("z", "26");
        Tags b = Tags.of("a", "1", "b", "2", "c", "3", "d", "4");
        Tags merged = Tags.concat(a, b);
        assertEquals(5, merged.size());
        assertEquals("a", merged.getKey(0));
        assertEquals("d", merged.getKey(3));
        assertEquals("z", merged.getKey(4));
        assertEquals("26", merged.getValue(4));
    }

    public void testConcatHashConsistency() {
        Tags viaOf = Tags.of("a", "1", "b", "2");
        Tags viaConcat = Tags.concat(Tags.of("a", "1"), Tags.of("b", "2"));
        assertEquals(viaOf, viaConcat);
        assertEquals(viaOf.hashCode(), viaConcat.hashCode());
    }

    public void testConcatResultIsSorted() {
        Tags a = Tags.of("x", "1");
        Tags b = Tags.of("a", "2", "m", "3");
        Tags merged = Tags.concat(a, b);
        for (int i = 0; i < merged.size() - 1; i++) {
            assertTrue(merged.getKey(i).compareTo(merged.getKey(i + 1)) < 0);
        }
    }

    // --- fromMap ---

    public void testFromMapRoundTrips() {
        Map<String, String> map = new HashMap<>();
        map.put("b", "2");
        map.put("a", "1");
        Tags t = Tags.fromMap(map);
        assertEquals(2, t.size());
        assertEquals("a", t.getKey(0));
        assertEquals("b", t.getKey(1));
        assertEquals("1", t.getValue(0));
        assertEquals("2", t.getValue(1));
    }

    public void testFromMapNullReturnsEmpty() {
        assertSame(Tags.EMPTY, Tags.fromMap(null));
    }

    public void testFromMapEmptyReturnsEmpty() {
        assertSame(Tags.EMPTY, Tags.fromMap(Map.of()));
    }

    // --- toMap ---

    public void testToMapConvertsValuesToStrings() {
        Tags t = Tags.of("k", (Object) 42L);
        Map<String, String> map = t.toMap();
        assertEquals("42", map.get("k"));
    }

    public void testToMapWithStringValues() {
        Tags t = Tags.of("a", "1", "b", "2");
        Map<String, String> map = t.toMap();
        assertEquals("1", map.get("a"));
        assertEquals("2", map.get("b"));
    }

    // --- getTagsMap ---

    public void testGetTagsMapPreservesOriginalTypes() {
        Tags t = Tags.of("num", (Object) 42L);
        assertEquals(42L, t.getTagsMap().get("num"));
    }

    // --- equals / hashCode ---

    public void testEqualTagsAreEqual() {
        Tags a = Tags.of("x", "1", "y", "2");
        Tags b = Tags.of("x", "1", "y", "2");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testDifferentTagsAreNotEqual() {
        Tags a = Tags.of("x", "1");
        Tags b = Tags.of("x", "2");
        assertNotEquals(a, b);
    }

    public void testEmptyEqualsEmpty() {
        assertEquals(Tags.EMPTY, Tags.create());
    }

    public void testIdentityEquals() {
        Tags t = Tags.of("k", "v");
        assertEquals(t, t);
    }

    public void testDifferentSizeNotEqual() {
        Tags a = Tags.of("k", "v");
        Tags b = Tags.of("k", "v", "k2", "v2");
        assertNotEquals(a, b);
    }

    // --- toString ---

    public void testToStringEmpty() {
        assertEquals("Tags{}", Tags.EMPTY.toString());
    }

    public void testToStringWithTags() {
        Tags t = Tags.of("a", "1");
        assertEquals("Tags{a=1}", t.toString());
    }

    // --- Deprecated API backward compatibility ---

    public void testCreateReturnsEmpty() {
        assertSame(Tags.EMPTY, Tags.create());
    }

    public void testAddTagReturnsNewInstance() {
        Tags original = Tags.create();
        Tags updated = original.addTag("k", "v");
        assertNotSame(original, updated);
        assertEquals(0, original.size());
        assertEquals(1, updated.size());
    }

    public void testAddTagChaining() {
        Tags t = Tags.create().addTag("a", "1").addTag("b", "2").addTag("c", "3");
        assertEquals(3, t.size());
        assertEquals("a", t.getKey(0));
        assertEquals("b", t.getKey(1));
        assertEquals("c", t.getKey(2));
    }

    public void testAddTagLong() {
        Tags t = Tags.create().addTag("num", 42L);
        assertEquals(1, t.size());
        assertEquals(42L, t.getValue(0));
    }

    public void testAddTagDouble() {
        Tags t = Tags.create().addTag("val", 3.14);
        assertEquals(1, t.size());
        assertEquals(3.14, t.getValue(0));
    }

    public void testAddTagBoolean() {
        Tags t = Tags.create().addTag("flag", true);
        assertEquals(1, t.size());
        assertEquals(true, t.getValue(0));
    }

    public void testAddTagOverwritesPreviousValue() {
        Tags t = Tags.create().addTag("k", "old").addTag("k", "new");
        assertEquals(1, t.size());
        assertEquals("new", t.getValue(0));
    }

    // --- Concurrent safety: Tags is immutable so no sharing issues ---

    public void testUsableAsMapKey() {
        Tags t1 = Tags.of("k", "v");
        Tags t2 = Tags.of("k", "v");
        Map<Tags, String> map = new HashMap<>();
        map.put(t1, "found");
        assertEquals("found", map.get(t2));
    }
}

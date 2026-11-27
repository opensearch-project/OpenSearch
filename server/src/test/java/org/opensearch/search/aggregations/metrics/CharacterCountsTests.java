/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CharacterCountsTests extends OpenSearchTestCase {

    private static final int REPLACEMENT = 0xFFFD;

    private static BytesRef utf8(String value) {
        return new BytesRef(value.getBytes(StandardCharsets.UTF_8));
    }

    private static BytesRef bytes(int... values) {
        final byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return new BytesRef(bytes);
    }

    private static Map<Integer, Long> asMap(CharacterCounts counts) {
        final Map<Integer, Long> map = new TreeMap<>();
        for (int codePoint : counts.sortedCodePoints()) {
            map.put(codePoint, counts.get(codePoint));
        }
        return map;
    }

    private static Map<Integer, Long> expectedCounts(String... values) {
        final Map<Integer, Long> expected = new TreeMap<>();
        for (String value : values) {
            value.codePoints().forEach(codePoint -> expected.merge(codePoint, 1L, Long::sum));
        }
        return expected;
    }

    public void testEmpty() {
        final CharacterCounts counts = new CharacterCounts();

        assertEquals(0, counts.size());
        assertEquals(0, counts.sortedCodePoints().length);
        assertEquals(0L, counts.get('a'));
    }

    public void testEmptyValueHasNoCharacters() {
        final CharacterCounts counts = new CharacterCounts();

        assertEquals(0, counts.addUtf8(new BytesRef()));

        assertEquals(0, counts.size());
    }

    public void testAscii() {
        final CharacterCounts counts = new CharacterCounts();

        assertEquals(10, counts.addUtf8(utf8("google.com")));

        assertEquals(expectedCounts("google.com"), asMap(counts));
        assertEquals(3L, counts.get('o'));
        assertEquals(2L, counts.get('g'));
    }

    public void testCountsAccumulateAcrossValues() {
        final CharacterCounts counts = new CharacterCounts();

        counts.addUtf8(utf8("google.com"));
        counts.addUtf8(utf8("x7k2qz9vh3.net"));
        counts.addUtf8(utf8("mail.google.com"));

        assertEquals(expectedCounts("google.com", "x7k2qz9vh3.net", "mail.google.com"), asMap(counts));
        assertEquals(6L, counts.get('o'));
        assertEquals(4L, counts.get('.'));
    }

    public void testMultiByteCharactersCountOnce() {
        final CharacterCounts counts = new CharacterCounts();

        // two bytes, three bytes, and a supplementary character that takes four bytes and two UTF-16 units
        final String value = "é€😀";
        assertEquals(3, counts.addUtf8(utf8(value)));

        assertEquals(expectedCounts(value), asMap(counts));
        assertEquals(1L, counts.get(0x1F600));
        assertEquals(0L, counts.get(0xD83D));
    }

    public void testBytesRefOffset() {
        final byte[] buffer = "xxabcxx".getBytes(StandardCharsets.UTF_8);
        final CharacterCounts counts = new CharacterCounts();

        assertEquals(3, counts.addUtf8(new BytesRef(buffer, 2, 3)));

        assertEquals(expectedCounts("abc"), asMap(counts));
    }

    public void testNulCharacterIsACharacter() {
        final CharacterCounts counts = new CharacterCounts();

        assertEquals(3, counts.addUtf8(utf8("a\u0000a")));

        assertEquals(2L, counts.get('a'));
        assertEquals(1L, counts.get(0));
    }

    public void testRandomUnicodeMatchesTheJdkDecoder() {
        final CharacterCounts counts = new CharacterCounts();
        final String[] values = new String[randomIntBetween(1, 50)];
        long expectedLength = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = randomRealisticUnicodeOfCodepointLengthBetween(0, 40);
            final int length = counts.addUtf8(utf8(values[i]));
            assertEquals(values[i].codePointCount(0, values[i].length()), length);
            expectedLength += length;
        }

        assertEquals(expectedCounts(values), asMap(counts));
        assertEquals(expectedLength, asMap(counts).values().stream().mapToLong(Long::longValue).sum());
    }

    public void testInvalidUtf8CountsOneReplacementCharacterPerBadByte() {
        // a lone continuation byte, an invalid lead byte, and bytes that are never valid in UTF-8
        assertReplacements(1, bytes(0x80));
        assertReplacements(1, bytes(0xFF));
        assertReplacements(1, bytes(0xF8));
        // a truncated sequence: the lead byte and the bytes after it are bad one by one
        assertReplacements(1, bytes(0xC3));
        assertReplacements(2, bytes(0xE2, 0x82));
        assertReplacements(3, bytes(0xF0, 0x9F, 0x98));
        // an overlong encoding of NUL and of a slash
        assertReplacements(2, bytes(0xC0, 0x80));
        assertReplacements(3, bytes(0xE0, 0x80, 0xAF));
        // a UTF-16 surrogate encoded as UTF-8, and a code point above U+10FFFF
        assertReplacements(3, bytes(0xED, 0xA0, 0x80));
        assertReplacements(4, bytes(0xF4, 0x90, 0x80, 0x80));
    }

    private void assertReplacements(int expected, BytesRef value) {
        final CharacterCounts counts = new CharacterCounts();

        assertEquals(expected, counts.addUtf8(value));

        assertEquals(1, counts.size());
        assertEquals((long) expected, counts.get(REPLACEMENT));
    }

    public void testInvalidBytesBetweenValidCharacters() {
        final CharacterCounts counts = new CharacterCounts();

        // 'a', a truncated two-byte sequence, then 'b' which must not be swallowed by the bad lead byte
        assertEquals(3, counts.addUtf8(bytes('a', 0xC3, 'b')));

        final Map<Integer, Long> expected = new TreeMap<>();
        expected.put((int) 'a', 1L);
        expected.put((int) 'b', 1L);
        expected.put(REPLACEMENT, 1L);
        assertEquals(expected, asMap(counts));
    }

    public void testRandomBytesNeverFail() {
        final CharacterCounts counts = new CharacterCounts();
        for (int i = 0; i < 200; i++) {
            final byte[] random = randomByteArrayOfLength(randomIntBetween(0, 64));
            final int length = counts.addUtf8(new BytesRef(random));
            // every code point takes at least one byte, so a value can not have more characters than bytes
            assertTrue(length <= random.length);
            assertTrue(length >= random.length / 4);
        }
        for (int codePoint : counts.sortedCodePoints()) {
            assertTrue(codePoint >= 0 && codePoint <= Character.MAX_CODE_POINT);
            assertFalse("a surrogate is not a character", codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE);
        }
    }

    public void testAddMergesCounts() {
        final CharacterCounts counts = new CharacterCounts();

        counts.add('a', 3);
        counts.add('b', 1);
        counts.add('a', 4);

        assertEquals(2, counts.size());
        assertEquals(7L, counts.get('a'));
        assertEquals(1L, counts.get('b'));
    }

    public void testSortedCodePoints() {
        final CharacterCounts counts = new CharacterCounts();
        counts.addUtf8(utf8("zebra😀é€ " + "\u0000"));

        final int[] sorted = counts.sortedCodePoints();

        for (int i = 1; i < sorted.length; i++) {
            assertTrue(sorted[i - 1] < sorted[i]);
        }
        assertEquals(counts.size(), sorted.length);
        assertEquals(0, sorted[0]);
        assertEquals(0x1F600, sorted[sorted.length - 1]);
    }

    public void testGrowsPastTheInitialCapacity() {
        final CharacterCounts counts = new CharacterCounts();
        final Map<Integer, Long> expected = new HashMap<>();
        // 5_000 distinct code points, each added a different number of times, forces several rehashes
        for (int codePoint = 1; codePoint <= 5_000; codePoint++) {
            if (Character.isSurrogate((char) codePoint)) {
                continue;
            }
            final long times = 1 + codePoint % 7;
            for (long t = 0; t < times; t++) {
                counts.add(codePoint, 1);
            }
            expected.merge(codePoint, times, Long::sum);
        }

        assertEquals(expected.size(), counts.size());
        for (Map.Entry<Integer, Long> entry : expected.entrySet()) {
            assertEquals((long) entry.getValue(), counts.get(entry.getKey()));
        }
        assertEquals(new TreeMap<>(expected), asMap(counts));
    }

    public void testRamBytesUsedGrowsWithTheNumberOfCharacters() {
        final CharacterCounts counts = new CharacterCounts();
        final long empty = counts.ramBytesUsed();
        assertTrue(empty > 0);

        long previous = empty;
        for (int codePoint = 0x4E00; codePoint < 0x4E00 + 2_000; codePoint++) {
            counts.add(codePoint, 1);
            assertTrue("memory never shrinks", counts.ramBytesUsed() >= previous);
            previous = counts.ramBytesUsed();
        }

        // two thousand distinct characters need at least an int key and a long count each
        assertTrue(previous - empty >= 2_000 * (Long.BYTES + Integer.BYTES));
    }
}

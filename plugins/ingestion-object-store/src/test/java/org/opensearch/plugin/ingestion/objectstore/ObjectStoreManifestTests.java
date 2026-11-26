/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link ObjectStoreManifest}.
 */
public class ObjectStoreManifestTests extends OpenSearchTestCase {

    private ObjectStoreManifest sampleManifest() {
        final Map<String, Long> map = new HashMap<>();
        map.put("charlie", 30L);
        map.put("alpha", 10L);
        map.put("bravo", 20L);
        return new ObjectStoreManifest(map);
    }

    /**
     * Verifies keys are returned in lexicographic order.
     */
    public void testKeysAreSorted() {
        assertThat(sampleManifest().keys(), contains("alpha", "bravo", "charlie"));
    }

    /**
     * Verifies size lookups, totals, count, and membership.
     */
    public void testAccessors() {
        final ObjectStoreManifest manifest = sampleManifest();
        assertThat(manifest.sizeOf("alpha"), is(10L));
        assertThat(manifest.sizeOf("charlie"), is(30L));
        assertThat(manifest.objectCount(), is(3));
        assertThat(manifest.contains("bravo"), is(true));
        assertThat(manifest.contains("missing"), is(false));
    }

    /**
     * Verifies the sorted map view is ordered and unmodifiable.
     */
    public void testAsSortedMap() {
        final SortedMap<String, Long> view = sampleManifest().asSortedMap();
        assertThat(view.firstKey(), is("alpha"));
        assertThat(view.lastKey(), is("charlie"));
        expectThrows(UnsupportedOperationException.class, () -> view.put("delta", 40L));
    }

    /**
     * Verifies an absent key lookup throws.
     */
    public void testSizeOfAbsentKeyThrows() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> sampleManifest().sizeOf("nope"));
        assertThat(e.getMessage(), containsString("nope"));
    }

    /**
     * Verifies a negative size is rejected at construction.
     */
    public void testNegativeSizeThrows() {
        final Map<String, Long> map = new HashMap<>();
        map.put("bad", -1L);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreManifest(map));
        assertThat(e.getMessage(), containsString("bad"));
    }

    /**
     * Verifies the key in the negative-size message is sanitized, so a key carrying control characters cannot forge
     * extra log lines.
     */
    public void testNegativeSizeMessageSanitizesTheKey() {
        final Map<String, Long> map = new HashMap<>();
        map.put("bad\nkey\r0", -1L);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreManifest(map));
        assertThat(e.getMessage(), not(containsString("\n")));
        assertThat(e.getMessage(), not(containsString("\r")));
    }

    /**
     * Verifies a null key is rejected at construction.
     */
    public void testUnpairedHighSurrogateKeyRejected() {
        final Map<String, Long> map = new HashMap<>();
        map.put("a\uD800", 1L);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreManifest(map));
        assertThat(e.getMessage(), containsString("unpaired surrogate"));
    }

    public void testUnpairedLowSurrogateKeyRejected() {
        final Map<String, Long> map = new HashMap<>();
        map.put("a\uDC00", 1L);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreManifest(map));
        assertThat(e.getMessage(), containsString("unpaired surrogate"));
    }

    public void testWellFormedSupplementaryKeyAccepted() {
        final ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("a\uD83D\uDE00", 7L));
        assertThat(manifest.sizeOf("a\uD83D\uDE00"), is(7L));
    }

    public void testNullKeyThrows() {
        final Map<String, Long> map = new HashMap<>();
        map.put(null, 5L);
        expectThrows(NullPointerException.class, () -> new ObjectStoreManifest(map));
    }

    /**
     * Verifies a null size value is rejected at construction.
     */
    public void testNullSizeThrows() {
        final Map<String, Long> map = new HashMap<>();
        map.put("key", null);
        expectThrows(NullPointerException.class, () -> new ObjectStoreManifest(map));
    }

    /**
     * Verifies a null map is rejected at construction.
     */
    public void testNullMapThrows() {
        expectThrows(NullPointerException.class, () -> new ObjectStoreManifest(null));
    }

    /**
     * Verifies an empty manifest round-trips and reports zero totals.
     */
    public void testDistinctKeysThatWouldShareUtf8BytesAreRejected() {
        Map<String, Long> map = new HashMap<>();
        map.put("\uD800", 10L);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ObjectStoreManifest(map));
        assertThat(e.getMessage(), containsString("unpaired surrogate"));
    }

    /**
     * Verifies that an empty manifest reports zero objects, no keys, and equals another empty manifest.
     */
    public void testByteOrderMatchesComparingUtf8BytesOverRandomKeys() {
        for (int i = 0; i < 2000; i++) {
            String a = randomUnicodeOfLengthBetween(1, 12);
            String b = randomBoolean() ? a : randomUnicodeOfLengthBetween(1, 12);
            int expected = Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
            int actual = ObjectStoreManifest.BYTE_ORDER.compare(a, b);
            assertThat("disagreed on [" + a + "] vs [" + b + "]", Integer.signum(actual), is(Integer.signum(expected)));
        }
    }

    public void testByteOrderPlacesSupplementaryPlaneAboveTheBmp() {
        assertThat(ObjectStoreManifest.BYTE_ORDER.compare("\uFFFD", "\uD83D\uDE00") < 0, is(true));
        assertThat(ObjectStoreManifest.BYTE_ORDER.compare("a", "ab") < 0, is(true));
        assertThat(ObjectStoreManifest.BYTE_ORDER.compare("a", "a"), is(0));
    }

    public void testEmptyManifest() {
        final ObjectStoreManifest empty = new ObjectStoreManifest(Map.of());
        assertThat(empty.objectCount(), is(0));
        assertThat(empty.keys(), is(empty()));
        assertThat(empty.asSortedMap().isEmpty(), is(true));
        assertThat(empty.contains("anything"), is(false));
        assertThat(empty, equalTo(new ObjectStoreManifest(Map.of())));
    }

    /**
     * Verifies equals, hashCode, and toString behavior.
     */
    public void testEqualsHashCodeToString() {
        final ObjectStoreManifest a = sampleManifest();
        final ObjectStoreManifest b = sampleManifest();
        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), is(b.hashCode()));
        assertThat(a.equals(a), is(true));
        assertThat(a.equals(null), is(false));
        assertThat(a.equals("not a manifest"), is(false));
        assertThat(a.toString(), containsString("alpha"));

        final Map<String, Long> other = new HashMap<>();
        other.put("alpha", 99L);
        assertThat(a.equals(new ObjectStoreManifest(other)), is(false));
    }
}

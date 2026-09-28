/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.opensearch.rule.MatchLabel;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class AttributeValueStoreTests extends OpenSearchTestCase {

    AttributeValueStore<String, String> subjectUnderTest;
    final static String ALPHA_NUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        subjectUnderTest = new DefaultAttributeValueStore<>(new PatriciaTrie<>());
    }

    private Set<String> extractFeatureValues(List<MatchLabel<String>> labels) {
        return labels.stream().map(MatchLabel::getFeatureValue).collect(Collectors.toSet());
    }

    public void testPut() {
        subjectUnderTest.put("foo", "bar");
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("foo")).contains("bar"));

        subjectUnderTest.put("foo", "sing");
        assertEquals(2, subjectUnderTest.getMatches("foo").size());
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("foo")).contains("sing"));
    }

    public void testRemove() {
        subjectUnderTest.put("foo", "bar");
        subjectUnderTest.remove("foo", "bar");
        assertEquals(0, subjectUnderTest.size());
        assertTrue(subjectUnderTest.getMatches("foo").isEmpty());
    }

    public void testGet() {
        subjectUnderTest.put("foo", "bar");
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("foo")).contains("bar"));

        subjectUnderTest.put("foo", "sing");
        assertEquals(2, subjectUnderTest.getMatches("foo").size());
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("foo")).contains("sing"));
    }

    public void testGetWhenNoProperPrefixIsPresent() {
        subjectUnderTest.put("foo", "bar");
        subjectUnderTest.put("foodip", "sing");

        assertTrue(subjectUnderTest.getMatches("foxtail").isEmpty());

        // A wildcard value "fox*" prefix-matches the request "foxtail".
        subjectUnderTest.put("fox*", "lucy");
        assertFalse(subjectUnderTest.getMatches("foxtail").isEmpty());
    }

    public void testExactValueDoesNotPrefixMatch() {
        // A value stored without a trailing wildcard is matched exactly, not as a prefix.
        subjectUnderTest.put("fox", "lucy");
        assertTrue(subjectUnderTest.getMatches("foxtail").isEmpty());
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("fox")).contains("lucy"));
    }

    public void testWildcardValuePrefixMatches() {
        // A value stored with a trailing wildcard matches both the exact stem and longer values.
        subjectUnderTest.put("fox*", "lucy");
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("fox")).contains("lucy"));
        assertTrue(extractFeatureValues(subjectUnderTest.getMatches("foxtail")).contains("lucy"));
        assertTrue(subjectUnderTest.getMatches("fo").isEmpty());
    }

    public void testWildcardRequestKeyMatchesWildcardValueOnce() {
        // A request key that is itself a wildcard expression ("fox*") must match a stored "fox*" value
        // only once (via the prefix branch), not additionally via an exact match, to avoid inflating score.
        subjectUnderTest.put("fox*", "lucy");
        List<MatchLabel<String>> matches = subjectUnderTest.getMatches("fox*");
        assertEquals(1, matches.size());
        assertEquals("lucy", matches.get(0).getFeatureValue());
        // Stem "fox" (length 3) over request "fox*" (length 4) scores 0.75; without the exact-branch guard the
        // exact hit on "fox*" would add a second 1.0 match and the caller's score summation would inflate it.
        assertEquals(0.75f, matches.get(0).getMatchScore(), 0.0f);

        // A wildcard request key must not exact-match a non-wildcard stored value of the same stem.
        subjectUnderTest.put("fox", "exact");
        assertEquals(Set.of("lucy"), extractFeatureValues(subjectUnderTest.getMatches("fox*")));
    }

    public void testExactAndWildcardValuesWithSameStemAreDistinct() {
        // "fox" (exact) and "fox*" (prefix) are stored under distinct keys and matched independently.
        subjectUnderTest.put("fox", "exact");
        subjectUnderTest.put("fox*", "prefix");

        // Request "fox" matches the exact value (score 1.0) and the wildcard stem (score 1.0).
        assertEquals(Set.of("exact", "prefix"), extractFeatureValues(subjectUnderTest.getMatches("fox")));
        // A longer request matches only the wildcard value.
        assertEquals(Set.of("prefix"), extractFeatureValues(subjectUnderTest.getMatches("foxtail")));
    }

    public void testEmptyKeyValueMatchesEveryRequestAsNoConstraint() {
        // Values stored under the empty key represent "no constraint" and must match any request key at score 0.
        // This pins the explicit empty-key lookup in getMatches; without it, unconstrained-attribute rules stop matching.
        subjectUnderTest.put("", "no_constraint");

        List<MatchLabel<String>> matches = subjectUnderTest.getMatches("anything");
        assertEquals(1, matches.size());
        assertEquals("no_constraint", matches.get(0).getFeatureValue());
        assertEquals(0f, matches.get(0).getMatchScore(), 0.0f);
        assertEquals(Set.of("no_constraint"), extractFeatureValues(subjectUnderTest.getMatches("")));
    }

    public void testBareWildcardValueMatchesEveryNonEmptyRequest() {
        // A bare "*" value (valid per RuleValidator) is reached via the shrinking-prefix loop at the empty prefix,
        // so it matches every non-empty request key at score 0, but does not match an empty request key.
        subjectUnderTest.put("*", "catch_all");

        assertEquals(Set.of("catch_all"), extractFeatureValues(subjectUnderTest.getMatches("anything")));
        assertEquals(0f, subjectUnderTest.getMatches("anything").get(0).getMatchScore(), 0.0f);
        assertTrue(subjectUnderTest.getMatches("").isEmpty());
    }

    public void testClear() {
        subjectUnderTest.put("foo", "bar");
        subjectUnderTest.clear();
        assertEquals(0, subjectUnderTest.size());
        assertTrue(subjectUnderTest.getMatches("foo").isEmpty());
    }

    public void testConcurrentUpdatesAndReads() {
        final List<String> randomStrings = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            randomStrings.add(generateRandom(20));
        }
        List<Thread> readerThreads = new ArrayList<>();
        List<Thread> writerThreads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            readerThreads.add(new AttributeValueStoreReader(subjectUnderTest, randomStrings));
            writerThreads.add(new AttributeValueStoreWriter(subjectUnderTest, randomStrings));
        }

        for (int i = 0; i < 10; i++) {
            readerThreads.get(i).start();
            writerThreads.get(i).start();
        }
    }

    public static String generateRandom(int maxLength) {
        int length = random().nextInt(maxLength) + 1;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHA_NUMERIC.charAt(random().nextInt(ALPHA_NUMERIC.length())));
        }
        return sb.toString();
    }

    private static class AttributeValueStoreReader extends Thread {
        private final AttributeValueStore<String, String> subjectUnderTest;
        private final List<String> toReadKeys;

        AttributeValueStoreReader(AttributeValueStore<String, String> subjectUnderTest, List<String> toReadKeys) {
            this.subjectUnderTest = subjectUnderTest;
            this.toReadKeys = toReadKeys;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(random().nextInt(100));
                for (String key : toReadKeys) {
                    subjectUnderTest.getMatches(key);
                }
            } catch (InterruptedException ignored) {}
        }
    }

    private static class AttributeValueStoreWriter extends Thread {
        private final AttributeValueStore<String, String> subjectUnderTest;
        private final List<String> toWriteKeys;

        AttributeValueStoreWriter(AttributeValueStore<String, String> subjectUnderTest, List<String> toWriteKeys) {
            this.subjectUnderTest = subjectUnderTest;
            this.toWriteKeys = toWriteKeys;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(random().nextInt(100));
                for (String key : toWriteKeys) {
                    subjectUnderTest.put(key, key);
                }
            } catch (InterruptedException e) {}
        }
    }

    public void testDefaultMethods() {
        class DummyStore implements AttributeValueStore<String, String> {
            boolean removeCalled = false;

            @Override
            public void put(String key, String value) {}

            @Override
            public void remove(String key) {
                removeCalled = true;
            }

            @Override
            public Optional<String> get(String key) {
                return Optional.empty();
            }

            @Override
            public void clear() {}

            @Override
            public int size() {
                return 0;
            }
        }

        DummyStore store = new DummyStore();
        store.remove("foo", "bar");
        assertTrue(store.removeCalled);
        List<MatchLabel<String>> result = store.getMatches("foo");
        assertNotNull(result);
        assertTrue(result.isEmpty());
        List<MatchLabel<String>> exactMatches = store.getExactMatch("foo");
        assertNotNull(exactMatches);
        assertTrue(exactMatches.isEmpty());
    }
}

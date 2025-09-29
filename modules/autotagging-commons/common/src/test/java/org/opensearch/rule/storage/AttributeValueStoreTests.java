/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AttributeValueStoreTests extends OpenSearchTestCase {

    AttributeValueStore<String, String> subjectUnderTest;
    final static String ALPHA_NUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public void setUp() throws Exception {
        super.setUp();
        subjectUnderTest = new DefaultAttributeValueStore<>(new PatriciaTrie<>());
    }

    public void testPut() {
        subjectUnderTest.put("foo", "bar");
        assertEquals("bar", subjectUnderTest.getAll("foo").getFirst().iterator().next());
        subjectUnderTest.put("foo", "sing");
        assertEquals(1, subjectUnderTest.getAll("foo").size());
        assertEquals(2, subjectUnderTest.getAll("foo").get(0).size());
        assertTrue(subjectUnderTest.getAll("foo").get(0).contains("sing"));
    }

    public void testRemove() {
        subjectUnderTest.put("foo", "bar");
        subjectUnderTest.remove("foo", "bar");
        assertEquals(0, subjectUnderTest.size());
    }

    public void tesGet() {
        subjectUnderTest.put("foo", "bar");
        assertEquals("bar", subjectUnderTest.getAll("foo").getFirst());
        subjectUnderTest.put("foo", "sing");
        assertEquals(2, subjectUnderTest.getAll("foo").size());
    }

    public void testGetWhenNoProperPrefixIsPresent() {
        subjectUnderTest.put("foo", "bar");
        subjectUnderTest.put("foodip", "sing");
        assertTrue(subjectUnderTest.getAll("foxtail").isEmpty());
        subjectUnderTest.put("fox", "lucy");

        assertFalse(subjectUnderTest.getAll("foxtail").isEmpty());
    }

    public void testClear() {
        subjectUnderTest.put("foo", "bar");
        subjectUnderTest.clear();
        assertEquals(0, subjectUnderTest.size());
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

        for (int ii = 0; ii < 10; ii++) {
            readerThreads.get(ii).start();
            writerThreads.get(ii).start();
        }
    }

    public static String generateRandom(int maxLength) {
        int length = random().nextInt(maxLength) + 1; // +1 to avoid length 0
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHA_NUMERIC.charAt(random().nextInt(ALPHA_NUMERIC.length())));
        }
        return sb.toString();
    }

    private static class AttributeValueStoreReader extends Thread {
        private final AttributeValueStore<String, String> subjectUnderTest;
        private final List<String> toReadKeys;

        public AttributeValueStoreReader(AttributeValueStore<String, String> subjectUnderTest, List<String> toReadKeys) {
            super();
            this.subjectUnderTest = subjectUnderTest;
            this.toReadKeys = toReadKeys;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(random().nextInt(100));
                for (String key : toReadKeys) {
                    subjectUnderTest.getAll(key);
                }
            } catch (InterruptedException e) {}
        }
    }

    private static class AttributeValueStoreWriter extends Thread {
        private final AttributeValueStore<String, String> subjectUnderTest;
        private final List<String> toWriteKeys;

        public AttributeValueStoreWriter(AttributeValueStore<String, String> subjectUnderTest, List<String> toWriteKeys) {
            super();
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
        List<Set<String>> result = store.getAll("foo");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}

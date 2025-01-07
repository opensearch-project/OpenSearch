/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    FastPrefixMatchingStructureTests.InsertionTests.class,
    FastPrefixMatchingStructureTests.SearchTests.class,
    FastPrefixMatchingStructureTests.DeletionTests.class,
    FastPrefixMatchingStructureTests.EdgeCaseTests.class })
public class FastPrefixMatchingStructureTests {

    public static class BaseTest extends OpenSearchTestCase {
        protected FastPrefixMatchingStructure trie;

        public void setUp() throws Exception {
            super.setUp();
            trie = new RuleAttributeTrie();
        }
    }

    public static class InsertionTests extends BaseTest {
        @Test
        public void testInsertSinglePair() {
            trie.insert("apple", "fruit");
            assertEquals(Collections.singletonList("fruit"), trie.search("apple"));
        }

        @Test
        public void testInsertMultiplePairs() {
            trie.insert("apple", "fruit");
            trie.insert("app", "application");
            trie.insert("application", "software");

            assertEquals(Collections.singletonList("fruit"), trie.search("apple"));
            assertEquals(Collections.singletonList("application"), trie.search("app"));
            assertEquals(Collections.singletonList("software"), trie.search("application"));
        }

        @Test
        public void testOverwriteExistingKey() {
            trie.insert("apple", "fruit");
            trie.insert("apple", "company");
            assertEquals(Collections.singletonList("company"), trie.search("apple"));
        }

        @Test
        public void testInsertKeysWithCommonPrefixes() {
            trie.insert("car", "vehicle");
            trie.insert("cart", "shopping");
            trie.insert("cartoon", "animation");

            assertEquals(Collections.singletonList("vehicle"), trie.search("car"));
            assertEquals(Collections.singletonList("shopping"), trie.search("cart"));
            assertEquals(Collections.singletonList("animation"), trie.search("cartoon"));
        }
    }

    public static class SearchTests extends BaseTest {

        public void setUp() throws Exception {
            super.setUp();
            trie.insert("apple", "fruit");
            trie.insert("app", "application");
            trie.insert("application", "software");
            trie.insert("appreciate", "value");
            trie.insert("book", "reading");
            trie.insert("bookstore", "shop");
        }

        @Test
        public void testSearchExistingKeys() {
            assertEquals(Collections.singletonList("fruit"), trie.search("apple"));
            assertEquals(Collections.singletonList("application"), trie.search("app"));
            assertEquals(Collections.singletonList("reading"), trie.search("book"));
        }

        @Test
        public void testSearchNonExistingKeys() {
            assertTrue(trie.search("cocktail").isEmpty());
            assertTrue(trie.search("mock").isEmpty());
        }

        @Test
        public void testSearchPartialKeys() {
            List<String> result = trie.search("ap");
            assertEquals(4, result.size());
            assertTrue(result.containsAll(Arrays.asList("fruit", "application", "software", "value")));
        }
    }

    public static class DeletionTests extends BaseTest {

        public void setUp() throws Exception {
            super.setUp();
            trie.insert("apple", "fruit");
            trie.insert("app", "application");
            trie.insert("application", "software");
            trie.insert("appreciate", "value");
            trie.insert("book", "reading");
            trie.insert("bookstore", "shop");
        }

        @Test
        public void testDeleteExistingKey() {
            assertTrue(trie.delete("apple"));
            assertTrue(trie.search("apple").isEmpty());
            assertFalse(trie.search("app").isEmpty());
        }

        @Test
        public void testDeleteNonExistingKey() {
            assertFalse(trie.delete("appl"));
            assertFalse(trie.search("apple").isEmpty());
        }

        @Test
        public void testDeleteKeyAndVerifyPartialSearch() {
            assertTrue(trie.delete("app"));
            List<String> result = trie.search("ap");
            assertEquals(3, result.size());
            assertTrue(result.containsAll(Arrays.asList("fruit", "software", "value")));
        }

        @Test
        public void testDeleteAllKeysWithCommonPrefix() {
            assertTrue(trie.delete("apple"));
            assertTrue(trie.delete("app"));
            assertTrue(trie.delete("application"));
            assertTrue(trie.delete("appreciate"));

            assertTrue(trie.search("ap").isEmpty());
            assertFalse(trie.search("book").isEmpty());
        }
    }

    public static class EdgeCaseTests extends BaseTest {

        @Test
        public void testInsertAndSearchEmptyString() {
            trie.insert("", "empty");
            assertEquals(Collections.singletonList("empty"), trie.search(""));
        }

        @Test
        public void testInsertEmptyValue() {
            trie.insert("emptyvalue", "");
            assertEquals(Collections.singletonList(""), trie.search("emptyvalue"));
        }

        @Test
        public void testDeleteEmptyString() {
            trie.insert("", "empty");
            assertTrue(trie.delete(""));
            assertTrue(trie.search("").isEmpty());
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FastPrefixMatchingStructureTests extends OpenSearchTestCase {
    FastPrefixMatchingStructure trie;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        trie = new RuleAttributeTrie();
        trie.insert("apple", "fruit");
        trie.insert("app", "application");
        trie.insert("application", "software");
        trie.insert("appreciate", "value");
        trie.insert("book", "reading");
        trie.insert("bookstore", "shop");
    }

    public void testInsertSinglePair() {
        assertEquals(Collections.singletonList("fruit"), trie.search("apple"));
    }

    public void testInsertMultiplePairs() {
        assertEquals(Collections.singletonList("fruit"), trie.search("apple"));
        assertEquals(Collections.singletonList("application"), trie.search("app"));
        assertEquals(Collections.singletonList("software"), trie.search("application"));
    }

    public void testOverwriteExistingKey() {
        trie.insert("apple", "company");
        assertEquals(Collections.singletonList("company"), trie.search("apple"));
    }

    public void testInsertKeysWithCommonPrefixes() {
        trie.insert("car", "vehicle");
        trie.insert("cart", "shopping");
        trie.insert("cartoon", "animation");

        assertEquals(Collections.singletonList("vehicle"), trie.search("car"));
        assertEquals(Collections.singletonList("shopping"), trie.search("cart"));
        assertEquals(Collections.singletonList("animation"), trie.search("cartoon"));
    }

    public void testSearchExistingKeys() {
        assertEquals(Collections.singletonList("fruit"), trie.search("apple"));
        assertEquals(Collections.singletonList("application"), trie.search("app"));
        assertEquals(Collections.singletonList("reading"), trie.search("book"));
    }

    public void testSearchNonExistingKeys() {
        assertTrue(trie.search("cocktail").isEmpty());
        assertTrue(trie.search("mock").isEmpty());
    }

    public void testSearchPartialKeys() {
        List<String> result = trie.search("ap");
        assertEquals(4, result.size());
        assertTrue(result.containsAll(Arrays.asList("fruit", "application", "software", "value")));
    }

    public void testDeleteExistingKey() {
        assertTrue(trie.delete("apple"));
        assertTrue(trie.search("apple").isEmpty());
        assertFalse(trie.search("app").isEmpty());
    }

    public void testDeleteNonExistingKey() {
        assertFalse(trie.delete("appl"));
        assertFalse(trie.search("apple").isEmpty());
    }

    public void testDeleteKeyAndVerifyPartialSearch() {
        assertTrue(trie.delete("app"));
        List<String> result = trie.search("ap");
        assertEquals(3, result.size());
        assertTrue(result.containsAll(Arrays.asList("fruit", "software", "value")));
    }

    public void testDeleteAllKeysWithCommonPrefix() {
        assertTrue(trie.delete("apple"));
        assertTrue(trie.delete("app"));
        assertTrue(trie.delete("application"));
        assertTrue(trie.delete("appreciate"));

        assertTrue(trie.search("ap").isEmpty());
        assertFalse(trie.search("book").isEmpty());
    }

    public void testInsertAndSearchEmptyString() {
        trie.insert("", "empty");
        assertEquals(Collections.singletonList("empty"), trie.search(""));
    }

    public void testDeleteEmptyString() {
        trie = new RuleAttributeTrie();
        trie.insert("", "empty");
        assertTrue(trie.delete(""));
        assertTrue(trie.search("").isEmpty());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.storage;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.opensearch.test.OpenSearchTestCase;


public class AttributeValueStoreTests extends OpenSearchTestCase {

    AttributeValueStore subjectUnderTest;

    public void setUp() throws Exception {
        super.setUp();
        subjectUnderTest = new TrieBasedStore(new PatriciaTrie<>());
    }

    public void testAddValue() {
        subjectUnderTest.addValue("foo", "bar");
        assertEquals("bar", subjectUnderTest.getValue("foo").get());
    }

    public void testRemoveValue() {
        subjectUnderTest.addValue("foo", "bar");
        subjectUnderTest.removeValue("foo");
        assertEquals(0, subjectUnderTest.size());
    }

    public void tesGetValue() {
        subjectUnderTest.addValue("foo", "bar");
        assertEquals("bar", subjectUnderTest.getValue("foo").get());
    }

    public void testGetValueWhenNoProperPrefixIsPresent() {
        subjectUnderTest.addValue("foo", "bar");
        subjectUnderTest.addValue("foodip", "sing");
        assertTrue(subjectUnderTest.getValue("foxtail").isEmpty());
        subjectUnderTest.addValue("fox", "lucy");

        assertFalse(subjectUnderTest.getValue("foxtail").isEmpty());
    }


    public void testClear() {
        subjectUnderTest.addValue("foo", "bar");
        subjectUnderTest.clear();
        assertEquals(0, subjectUnderTest.size());
    }
}

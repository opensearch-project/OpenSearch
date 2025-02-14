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

    AttributeValueStore<String, String> subjectUnderTest;

    public void setUp() throws Exception {
        super.setUp();
        subjectUnderTest = new DefaultAttributeValueStore<>(new PatriciaTrie<>());
    }

    public void testAdd() {
        subjectUnderTest.add("foo", "bar");
        assertEquals("bar", subjectUnderTest.get("foo").get());
    }

    public void testRemove() {
        subjectUnderTest.add("foo", "bar");
        subjectUnderTest.remove("foo");
        assertEquals(0, subjectUnderTest.size());
    }

    public void tesGet() {
        subjectUnderTest.add("foo", "bar");
        assertEquals("bar", subjectUnderTest.get("foo").get());
    }

    public void testGetWhenNoProperPrefixIsPresent() {
        subjectUnderTest.add("foo", "bar");
        subjectUnderTest.add("foodip", "sing");
        assertTrue(subjectUnderTest.get("foxtail").isEmpty());
        subjectUnderTest.add("fox", "lucy");

        assertFalse(subjectUnderTest.get("foxtail").isEmpty());
    }


    public void testClear() {
        subjectUnderTest.add("foo", "bar");
        subjectUnderTest.clear();
        assertEquals(0, subjectUnderTest.size());
    }
}

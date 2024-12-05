/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.filter;

import org.opensearch.test.OpenSearchTestCase;

public class WildcardMatcherTests extends OpenSearchTestCase {

    static private WildcardMatcher wc(String pattern) {
        return WildcardMatcher.from(pattern);
    }

    static private WildcardMatcher iwc(String pattern) {
        return WildcardMatcher.from(pattern, false);
    }

    public void testWildcardMatcherClasses() {
        assertFalse(wc("a*?").test("a"));
        assertTrue(wc("a*?").test("aa"));
        assertTrue(wc("a*?").test("ab"));
        assertTrue(wc("a*?").test("abb"));
        assertTrue(wc("*my*index").test("myindex"));
        assertFalse(wc("*my*index").test("myindex1"));
        assertTrue(wc("*my*index?").test("myindex1"));
        assertTrue(wc("*my*index").test("this_is_my_great_index"));
        assertFalse(wc("*my*index").test("MYindex"));
        assertFalse(wc("?kibana").test("kibana"));
        assertTrue(wc("?kibana").test(".kibana"));
        assertFalse(wc("?kibana").test("kibana."));
        assertTrue(wc("?kibana?").test("?kibana."));
        assertTrue(wc("/(\\d{3}-?\\d{2}-?\\d{4})/").test("123-45-6789"));
        assertFalse(wc("(\\d{3}-?\\d{2}-?\\d{4})").test("123-45-6789"));
        assertTrue(wc("/\\S+/").test("abc"));
        assertTrue(wc("abc").test("abc"));
        assertFalse(wc("ABC").test("abc"));
        assertFalse(wc(null).test("abc"));
        assertTrue(WildcardMatcher.from(null, "abc").test("abc"));
    }

    public void testWildcardMatcherClassesCaseInsensitive() {
        assertTrue(iwc("AbC").test("abc"));
        assertTrue(iwc("abc").test("aBC"));
        assertTrue(iwc("A*b").test("ab"));
        assertTrue(iwc("A*b").test("aab"));
        assertTrue(iwc("A*b").test("abB"));
        assertFalse(iwc("abc").test("AB"));
        assertTrue(iwc("/^\\w+$/").test("AbCd"));
    }

    public void testWildcardMatchers() {
        assertTrue(!WildcardMatcher.from("a*?").test("a"));
        assertTrue(WildcardMatcher.from("a*?").test("aa"));
        assertTrue(WildcardMatcher.from("a*?").test("ab"));
        // assertTrue(WildcardMatcher.pattern("a*?").test( "abb"));
        assertTrue(WildcardMatcher.from("*my*index").test("myindex"));
        assertTrue(!WildcardMatcher.from("*my*index").test("myindex1"));
        assertTrue(WildcardMatcher.from("*my*index?").test("myindex1"));
        assertTrue(WildcardMatcher.from("*my*index").test("this_is_my_great_index"));
        assertTrue(!WildcardMatcher.from("*my*index").test("MYindex"));
        assertTrue(!WildcardMatcher.from("?kibana").test("kibana"));
        assertTrue(WildcardMatcher.from("?kibana").test(".kibana"));
        assertTrue(!WildcardMatcher.from("?kibana").test("kibana."));
        assertTrue(WildcardMatcher.from("?kibana?").test("?kibana."));
        assertTrue(WildcardMatcher.from("/(\\d{3}-?\\d{2}-?\\d{4})/").test("123-45-6789"));
        assertTrue(!WildcardMatcher.from("(\\d{3}-?\\d{2}-?\\d{4})").test("123-45-6789"));
        assertTrue(WildcardMatcher.from("/\\S*/").test("abc"));
        assertTrue(WildcardMatcher.from("abc").test("abc"));
        assertTrue(!WildcardMatcher.from("ABC").test("abc"));
    }
}

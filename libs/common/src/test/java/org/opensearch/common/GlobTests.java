/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;

public class GlobTests extends OpenSearchTestCase {

    public void testGlobMatchNoWildcard() {
        assertTrue(Glob.globMatch("test", "test"));
        assertFalse(Glob.globMatch("test", "testing"));
    }

    public void testGlobMatchWildcardAtBeginning() {
        assertTrue(Glob.globMatch("*test", "thisisatest"));
        assertFalse(Glob.globMatch("*test", "thisisatesting"));
    }

    public void testGlobMatchWildcardAtEnd() {
        assertTrue(Glob.globMatch("test*", "testthisisa"));
        assertFalse(Glob.globMatch("test*", "atestthisis"));
    }

    public void testGlobMatchWildcardAtMiddle() {
        assertTrue(Glob.globMatch("test*ing", "testthisisaing"));
        assertFalse(Glob.globMatch("test*ing", "testthisisa"));
    }

    public void testGlobMatchMultipleWildcards() {
        assertTrue(Glob.globMatch("*test*", "thisisatesting"));
        assertFalse(Glob.globMatch("*test*", "thisisatesing"));
        assertTrue(Glob.globMatch("*test*test", "thisisatestingtest"));
        assertFalse(Glob.globMatch("*test*test", "thisisatesting"));
    }

    public void testGlobalMatchDoubleWildcard() {
        assertTrue(Glob.globMatch("**test", "thisisatest"));
        assertFalse(Glob.globMatch("**test", "thisisatesting"));
        assertTrue(Glob.globMatch("test**", "testthisisa"));
        assertFalse(Glob.globMatch("test**", "atestthisis"));
        assertTrue(Glob.globMatch("**test**", "thisisatesting"));
        assertFalse(Glob.globMatch("**test**", "thisisatesing"));
        assertTrue(Glob.globMatch("**test**test", "thisisatestingtest"));
        assertFalse(Glob.globMatch("**test**test", "thisisatesting"));
    }

    public void testGlobMatchMultipleCharactersWithSingleWildcard() {
        assertTrue(Glob.globMatch("a*b", "acb"));
        assertTrue(Glob.globMatch("f*o", "foo"));
        assertTrue(Glob.globMatch("f*oo", "foo"));
        assertTrue(Glob.globMatch("a*b", "aab"));
        assertTrue(Glob.globMatch("a*b", "aaab"));
        assertFalse(Glob.globMatch("a*b", "ac"));
    }

    public void testGlobMatchWildcardWithEmptyString() {
        assertTrue(Glob.globMatch("*", ""));
        assertTrue(Glob.globMatch("a*", "a"));
        assertFalse(Glob.globMatch("a*", ""));
    }

    public void testGlobMatchMultipleWildcardsWithMultipleCharacters() {
        assertTrue(Glob.globMatch("a*b*c", "abc"));
        assertTrue(Glob.globMatch("a*b*c", "axxxbxbc"));
        assertTrue(Glob.globMatch("a*b*c", "aabc"));
        assertTrue(Glob.globMatch("a*b*c", "abac"));
        assertFalse(Glob.globMatch("a*b*c", "abca"));
        assertFalse(Glob.globMatch("a*b*c", "ac"));
    }

    public void testGlobMatchNullPattern() {
        assertFalse(Glob.globMatch(null, "test"));
    }

    public void testGlobMatchNullString() {
        assertFalse(Glob.globMatch("test", null));
    }

    public void testGlobMatchNullPatternAndString() {
        assertFalse(Glob.globMatch(null, null));
    }
}

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

    public void testGlobMatchForNull() {
        assertFalse(Glob.globMatch(null, "test"));
        assertFalse(Glob.globMatch("test", null));
        assertFalse(Glob.globMatch(null, null));
    }

    public void testGlobMatchNoWildcard() {
        assertTrue(Glob.globMatch("abcd", "abcd"));
        assertFalse(Glob.globMatch("abcd", "foobar"));
    }

    public void testGlobMatchSingleWildcard() {
        assertTrue(Glob.globMatch("*foo", "barfoo"));
        assertFalse(Glob.globMatch("*foo", "foobar"));
        assertTrue(Glob.globMatch("foo*", "foobarfoo"));
        assertFalse(Glob.globMatch("foo*", "barfoobar"));
        assertTrue(Glob.globMatch("foo*bar", "foobarnfoosbar"));
    }

    public void testGlobMatchMultipleWildcards() {
        assertTrue(Glob.globMatch("*foo*", "barfoobar"));
        assertFalse(Glob.globMatch("*foo*", "baroofbar"));
        assertTrue(Glob.globMatch("*foo*bar", "abcdfooefghbar"));
        assertFalse(Glob.globMatch("*foo*bar", "foonotbars"));
    }

    public void testGlobalMatchDoubleWildcard() {
        assertTrue(Glob.globMatch("**foo", "barbarfoo"));
        assertFalse(Glob.globMatch("**foo", "barbarfoowoof"));
        assertTrue(Glob.globMatch("**bar**", "foobarfoo"));
        assertFalse(Glob.globMatch("**bar**", "foobanfoo"));
    }

    public void testGlobMatchMultipleCharactersWithSingleWildcard() {
        assertTrue(Glob.globMatch("a*b", "acb"));
        assertTrue(Glob.globMatch("f*oo", "foo"));
        assertTrue(Glob.globMatch("a*b", "aab"));
        assertTrue(Glob.globMatch("a*b", "aaab"));
    }

    public void testGlobMatchWildcardWithEmptyString() {
        assertTrue(Glob.globMatch("*", ""));
        assertTrue(Glob.globMatch("a*", "a"));
        assertFalse(Glob.globMatch("a*", ""));
    }

    public void testGlobMatchMultipleWildcardsWithMultipleCharacters() {
        assertTrue(Glob.globMatch("a*b*c", "abc"));
        assertTrue(Glob.globMatch("a*b*c", "axxxbxbc"));
        assertFalse(Glob.globMatch("a*b*c", "abca"));
        assertFalse(Glob.globMatch("a*b*c", "ac"));
    }
}

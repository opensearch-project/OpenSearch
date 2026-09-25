/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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

    public void testGlobMatchWildcardSpansAsteriskInString() {
        assertTrue(Glob.globMatch("*", "*foo"));
        assertTrue(Glob.globMatch("*", "**"));
        assertTrue(Glob.globMatch("*foo", "*barfoo"));
        assertTrue(Glob.globMatch("foo*", "foo*bar"));
        assertTrue(Glob.globMatch("foo*bar", "foo*bazbar"));
        assertTrue(Glob.globMatch("*bar", "*foobar"));
        assertFalse(Glob.globMatch("*bar", "*foobaz"));
    }

    public void testGlobMatchAsteriskInStringIsAnOrdinaryCharacter() {
        assertTrue(Glob.globMatch("a*c", "a*bc"));
        assertTrue(Glob.globMatch("a*b", "a*b"));
        assertTrue(Glob.globMatch("user.*", "user.*name"));
        assertFalse(Glob.globMatch("abc", "a*c"));
    }

    /**
     * The only metacharacter is '*' in the pattern; every character of the string, '*' included, is
     * an ordinary character. Cross-check every pattern and string over {a, b, *} up to length four
     * against a regular expression built to those semantics, which is also what
     * {@code Regex.simpleMatchToAutomaton} builds for the same pattern.
     */
    public void testGlobMatchAgreesWithEquivalentRegex() {
        List<String> words = new ArrayList<>();
        words.add("");
        List<String> previous = new ArrayList<>(words);
        for (int length = 1; length <= 4; length++) {
            List<String> next = new ArrayList<>();
            for (String word : previous) {
                for (char c : new char[] { 'a', 'b', '*' }) {
                    next.add(word + c);
                }
            }
            words.addAll(next);
            previous = next;
        }

        for (String pattern : words) {
            for (String str : words) {
                assertEquals(
                    "pattern [" + pattern + "] against [" + str + "]",
                    matchesEquivalentRegex(pattern, str),
                    Glob.globMatch(pattern, str)
                );
            }
        }
    }

    private static boolean matchesEquivalentRegex(String pattern, String str) {
        StringBuilder regex = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '*') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append(".*");
            } else {
                literal.append(c);
            }
        }
        if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL).matcher(str).matches();
    }
}

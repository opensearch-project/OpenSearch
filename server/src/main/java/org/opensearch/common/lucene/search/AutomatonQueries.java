/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.lucene.util.automaton.MinimizationOperations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Helper functions for creating various forms of {@link AutomatonQuery}
 *
 * @opensearch.internal
 */
public class AutomatonQueries {

    /** Build an automaton query accepting all terms with the specified prefix, ASCII case insensitive. */
    public static Automaton caseInsensitivePrefix(String s) {
        List<Automaton> list = new ArrayList<>();
        Iterator<Integer> iter = s.codePoints().iterator();
        while (iter.hasNext()) {
            list.add(toCaseInsensitiveChar(iter.next(), Integer.MAX_VALUE));
        }
        list.add(Automata.makeAnyString());

        Automaton a = Operations.concatenate(list);
        a = MinimizationOperations.minimize(a, Integer.MAX_VALUE);
        return a;
    }

    /**
     * Build an automaton query accepting all terms with the specified prefix, ASCII case insensitive.
     */
    public static AutomatonQuery caseInsensitivePrefixQuery(Term prefix) {
        return caseInsensitivePrefixQuery(prefix, MultiTermQuery.CONSTANT_SCORE_REWRITE);
    }

    /**
     * Build an automaton query accepting all terms with the specified prefix, ASCII case insensitive.
     */
    public static AutomatonQuery caseInsensitivePrefixQuery(Term prefix, MultiTermQuery.RewriteMethod method) {
        return createAutomatonQuery(prefix, caseInsensitivePrefix(prefix.text()), method);
    }

    /**
     * Build an automaton accepting all terms ASCII case insensitive.
     */
    public static AutomatonQuery caseInsensitiveTermQuery(Term term) {
        BytesRef prefix = term.bytes();
        return new AutomatonQuery(term, toCaseInsensitiveString(prefix, Integer.MAX_VALUE));
    }

    /**
     * Build an automaton matching a wildcard pattern, ASCII case insensitive, if the method is null, then will use {@link  MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE}.
     */
    public static AutomatonQuery caseInsensitiveWildcardQuery(Term wildcardquery, MultiTermQuery.RewriteMethod method) {
        return createAutomatonQuery(wildcardquery, toCaseInsensitiveWildcardAutomaton(wildcardquery, Integer.MAX_VALUE), method);
    }

    /**
     * Build an automaton matching a given pattern with rewrite method, if the rewrite method is null, then will use {@link  MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE}.
     */
    public static AutomatonQuery createAutomatonQuery(Term term, Automaton automaton, MultiTermQuery.RewriteMethod method) {
        if (method == null) {
            method = MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
        }
        return new AutomatonQuery(term, automaton, false, method);
    }

    /**
     * String equality with support for wildcards
     */
    public static final char WILDCARD_STRING = '*';

    /**
     * Char equality with support for wildcards
     */
    public static final char WILDCARD_CHAR = '?';

    /**
     * Escape character
     */
    public static final char WILDCARD_ESCAPE = '\\';

    /**
     * Convert Lucene wildcard syntax into an automaton.
     */
    @SuppressWarnings("fallthrough")
    public static Automaton toCaseInsensitiveWildcardAutomaton(Term wildcardquery, int maxDeterminizedStates) {
        List<Automaton> automata = new ArrayList<>();

        String wildcardText = wildcardquery.text();

        for (int i = 0; i < wildcardText.length();) {
            final int c = wildcardText.codePointAt(i);
            int length = Character.charCount(c);
            switch (c) {
                case WILDCARD_STRING:
                    automata.add(Automata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    automata.add(Automata.makeAnyChar());
                    break;
                case WILDCARD_ESCAPE:
                    // add the next codepoint instead, if it exists
                    if (i + length < wildcardText.length()) {
                        final int nextChar = wildcardText.codePointAt(i + length);
                        length += Character.charCount(nextChar);
                        automata.add(Automata.makeChar(nextChar));
                        break;
                    } // else fallthru, lenient parsing with a trailing \
                default:
                    automata.add(toCaseInsensitiveChar(c, maxDeterminizedStates));
            }
            i += length;
        }

        return Operations.concatenate(automata);
    }

    protected static Automaton toCaseInsensitiveString(BytesRef br, int maxDeterminizedStates) {
        return toCaseInsensitiveString(br.utf8ToString(), maxDeterminizedStates);
    }

    public static Automaton toCaseInsensitiveString(String s, int maxDeterminizedStates) {
        List<Automaton> list = new ArrayList<>();
        Iterator<Integer> iter = s.codePoints().iterator();
        while (iter.hasNext()) {
            list.add(toCaseInsensitiveChar(iter.next(), maxDeterminizedStates));
        }

        Automaton a = Operations.concatenate(list);
        a = MinimizationOperations.minimize(a, maxDeterminizedStates);
        return a;

    }

    public static Automaton toCaseInsensitiveChar(int codepoint, int maxDeterminizedStates) {
        Automaton case1 = Automata.makeChar(codepoint);
        // For now we only work with ASCII characters
        if (codepoint > 128) {
            return case1;
        }
        int altCase = Character.isLowerCase(codepoint) ? Character.toUpperCase(codepoint) : Character.toLowerCase(codepoint);
        Automaton result;
        if (altCase != codepoint) {
            result = Operations.union(case1, Automata.makeChar(altCase));
            result = MinimizationOperations.minimize(result, maxDeterminizedStates);
        } else {
            result = case1;
        }
        return result;
    }
}

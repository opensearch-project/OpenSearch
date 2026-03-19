/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link AutomatonQueries}
 */
public class AutomatonQueriesTests extends OpenSearchTestCase {

    /**
     * Test that toCaseInsensitiveChar works correctly for ASCII characters
     */
    public void testToCaseInsensitiveCharAscii() {
        // Test lowercase ASCII
        Automaton automaton = AutomatonQueries.toCaseInsensitiveChar('a');
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match lowercase 'a'", runAutomaton.run("a"));
        assertTrue("Should match uppercase 'A'", runAutomaton.run("A"));
        assertFalse("Should not match 'b'", runAutomaton.run("b"));

        // Test uppercase ASCII
        automaton = AutomatonQueries.toCaseInsensitiveChar('Z');
        runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match uppercase 'Z'", runAutomaton.run("Z"));
        assertTrue("Should match lowercase 'z'", runAutomaton.run("z"));
        assertFalse("Should not match 'y'", runAutomaton.run("y"));
    }

    /**
     * Test that toCaseInsensitiveChar works correctly for Turkish characters.
     * This is a regression test for bug #20843 where case-insensitive wildcard queries
     * failed for languages with special case-folding rules.
     */
    public void testToCaseInsensitiveCharTurkish() {
        // Test Turkish dotted capital I (İ, U+0130)
        // In Turkish: İ (U+0130) lowercases to i (U+0069)
        // Note: Character.toLowerCase uses locale-independent rules
        Automaton automaton = AutomatonQueries.toCaseInsensitiveChar('\u0130'); // İ
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);

        assertTrue("Should match Turkish capital İ (U+0130)", runAutomaton.run("\u0130"));
        assertTrue("Should match lowercase i (U+0069)", runAutomaton.run("i"));

        // Test lowercase Turkish dotless i (ı, U+0131)
        automaton = AutomatonQueries.toCaseInsensitiveChar('\u0131'); // ı
        runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match Turkish lowercase ı (U+0131)", runAutomaton.run("\u0131"));
        assertTrue("Should match uppercase I (U+0049)", runAutomaton.run("I"));
    }

    /**
     * Test that toCaseInsensitiveChar works correctly for Cyrillic characters (Ukrainian/Russian).
     * This is part of the fix for bug #20843.
     */
    public void testToCaseInsensitiveCharCyrillic() {
        // Test Cyrillic capital letter A (А, U+0410)
        Automaton automaton = AutomatonQueries.toCaseInsensitiveChar('\u0410'); // А
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match Cyrillic capital А (U+0410)", runAutomaton.run("\u0410"));
        assertTrue("Should match Cyrillic lowercase а (U+0430)", runAutomaton.run("\u0430"));

        // Test Cyrillic lowercase letter б (U+0431)
        automaton = AutomatonQueries.toCaseInsensitiveChar('\u0431'); // б
        runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match Cyrillic lowercase б (U+0431)", runAutomaton.run("\u0431"));
        assertTrue("Should match Cyrillic uppercase Б (U+0411)", runAutomaton.run("\u0411"));
    }

    /**
     * Test that toCaseInsensitiveChar works correctly for Greek characters
     */
    public void testToCaseInsensitiveCharGreek() {
        // Test Greek capital letter Sigma (Σ, U+03A3)
        Automaton automaton = AutomatonQueries.toCaseInsensitiveChar('\u03A3'); // Σ
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match Greek capital Σ (U+03A3)", runAutomaton.run("\u03A3"));
        assertTrue("Should match Greek lowercase σ (U+03C3)", runAutomaton.run("\u03C3"));
    }

    /**
     * Test that toCaseInsensitiveChar handles characters with no case variants
     */
    public void testToCaseInsensitiveCharNoCaseVariant() {
        // Test a digit (no case variants)
        Automaton automaton = AutomatonQueries.toCaseInsensitiveChar('5');
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match digit '5'", runAutomaton.run("5"));
        assertFalse("Should not match digit '6'", runAutomaton.run("6"));

        // Test a symbol (no case variants)
        automaton = AutomatonQueries.toCaseInsensitiveChar('$');
        runAutomaton = new CharacterRunAutomaton(automaton);
        assertTrue("Should match symbol '$'", runAutomaton.run("$"));
        assertFalse("Should not match symbol '%'", runAutomaton.run("%"));
    }

    /**
     * Test the toCaseInsensitiveString method with mixed scripts
     */
    public void testToCaseInsensitiveStringMixed() {
        // Test a string with mixed ASCII and non-ASCII
        Automaton automaton = AutomatonQueries.toCaseInsensitiveString("İstanbul");
        CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);

        assertTrue("Should match original 'İstanbul'", runAutomaton.run("İstanbul"));
        assertTrue("Should match lowercase 'istanbul'", runAutomaton.run("istanbul"));
        assertTrue("Should match uppercase 'ISTANBUL'", runAutomaton.run("ISTANBUL"));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

import java.io.IOException;

/**
 * Tests for {@link TamilStemmer}.
 */
public class TamilStemmerTests extends BaseTokenStreamTestCase {

    /**
     * Test basic suffix stripping for common case markers.
     */
    public void testBasicSuffixStripping() throws IOException {
        // "school" + dative "to" -> should strip க்கு
        assertStem("பள்ளிக்கு", "பள்ளி", 2);

        // "house" + locative "in" -> should strip ில்
        assertStem("வீட்டில்", "வீட்ட", 2);

        // "child" + accusative (யை form after vowel) -> should strip யை
        assertStem("குழந்தையை", "குழந்தை", 2);
    }

    /**
     * Test plural + case combinations (longer suffixes).
     */
    public void testPluralCaseCombinations() throws IOException {
        // "children" + dative -> should strip களுக்கு
        assertStem("குழந்தைகளுக்கு", "குழந்தை", 2);

        // "houses" + accusative -> should strip களை
        assertStem("வீடுகளை", "வீடு", 2);

        // "schools" (plural only) -> should strip கள்
        assertStem("பள்ளிகள்", "பள்ளி", 2);
    }

    /**
     * Test that min_stem_length guard prevents over-stemming.
     */
    public void testMinStemLengthGuard() throws IOException {
        // With min_stem_length=4, short stems should NOT be produced
        // வீடுகள் (houses, len=6) -> வீடு (house, len=4) - OK with min=4
        assertStem("வீடுகள்", "வீடு", 4);

        // With min_stem_length=5, the same word should NOT be stemmed
        // because வீடு (len=4) is below the threshold
        assertStem("வீடுகள்", "வீடுகள்", 5);  // blocked because stem would be < 5 chars
    }

    /**
     * Test that bare roots (no suffix) pass through unchanged.
     */
    public void testBareRootPassthrough() throws IOException {
        assertStem("தமிழ்", "தமிழ்", 2);
        assertStem("பள்ளி", "பள்ளி", 2);
        assertStem("நூல்", "நூல்", 2);
    }

    /**
     * Test longest-first matching: longer suffix takes precedence.
     */
    public void testLongestFirstMatching() throws IOException {
        // களுக்கு should match before க்கு
        assertStem("மாணவர்களுக்கு", "மாணவர்", 2);
    }

    /**
     * Test that only one suffix is stripped per token.
     */
    public void testSingleSuffixStrip() throws IOException {
        // Even if the result still ends with a suffix-like sequence,
        // only one layer is stripped per pass
        String input = "பள்ளிகளுக்கு";  // school + plural + dative
        String expected = "பள்ளி";        // strips களுக்கு
        assertStem(input, expected, 2);
    }

    /**
     * Test handling of tokens with combining characters.
     */
    public void testCombiningCharacters() throws IOException {
        // Tamil text with combining marks should survive intact
        assertStem("சென்னை", "சென்னை", 2);  // no suffix to strip
        assertStem("தமிழ்நாடு", "தமிழ்நாடு", 2);  // compound, no recognized suffix
    }

    private void assertStem(String input, String expected, int minStemLength) throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream stream = new TamilStemmer(tokenizer, minStemLength);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        assertAnalyzesTo(analyzer, input, new String[] { expected });
        analyzer.close();
    }

    /**
     * Test multiple tokens in sequence.
     */
    public void testMultipleTokens() throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream stream = new TamilStemmer(tokenizer, 2);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        // பள்ளிக்கு -> பள்ளி (strip க்கு)
        // வீட்டில் -> வீட்ட (strip ில்)
        // குழந்தைகள் -> குழந்தை (strip கள்)
        assertAnalyzesTo(analyzer, "பள்ளிக்கு வீட்டில் குழந்தைகள்", new String[] { "பள்ளி", "வீட்ட", "குழந்தை" });
        analyzer.close();
    }
}

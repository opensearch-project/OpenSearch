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
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link TamilStemmer}.
 */
public class TamilStemmerTests extends BaseTokenStreamTestCase {

    /**
     * Test that default suffixes are loaded from resource file.
     */
    public void testDefaultSuffixesLoaded() throws IOException {
        TamilStemmer stemmer = new TamilStemmer(new WhitespaceTokenizer());
        assertTrue("Default suffixes should be loaded", stemmer.getSuffixes().length > 0);
        stemmer.close();
    }

    /**
     * Test that default prefixes are loaded from resource file.
     */
    public void testDefaultPrefixesLoaded() throws IOException {
        TamilStemmer stemmer = new TamilStemmer(new WhitespaceTokenizer());
        assertTrue("Default prefixes should be loaded", stemmer.getPrefixes().length > 0);
        stemmer.close();
    }

    /**
     * Test basic suffix stripping for common case markers.
     */
    public void testBasicSuffixStripping() throws IOException {
        // "school" + dative "to" -> should strip க்கு
        assertStem("பள்ளிக்கு", "பள்ளி", 2);

        // "house" + locative "in" -> should strip ில்
        assertStem("வீட்டில்", "வீட்ட", 2);
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
     * Test possessive suffix stripping - key for Thirukkural searches.
     * அன்புடையார் = அன்பு + உடையார் (one who has love)
     * The suffix டையார் strips to leave அன்பு
     */
    public void testPossessiveSuffixes() throws IOException {
        // அன்புடையார் (one who has love) -> அன்பு
        assertStem("அன்புடையார்", "அன்பு", 2);

        // அன்புடைமை (having love) -> அன்பு
        assertStem("அன்புடைமை", "அன்பு", 2);
    }

    /**
     * Test negative/lacking suffix stripping.
     */
    public void testNegativeSuffixes() throws IOException {
        // அன்பில்லார் (one without love) -> அன்ப
        assertStem("அன்பில்லார்", "அன்ப", 2);
    }

    /**
     * Test verb tense suffixes.
     */
    public void testVerbTenseSuffixes() throws IOException {
        // Present tense - கிறார் suffix
        assertStem("செய்கிறார்", "செய்", 2);

        // Past tense - த்தார் suffix
        assertStem("செய்தார்", "செய்", 2);

        // Future tense - வார் suffix
        assertStem("செய்வார்", "செய்", 2);
    }

    /**
     * Test prefix stripping.
     */
    public void testPrefixStripping() throws IOException {
        // சிறுவன் (small + boy) - சிறு is prefix, ன் is suffix
        // With both enabled, prefix stripped first, then suffix
        assertStemBoth("சிறுவன்", "வ", 1);
    }

    /**
     * Test prefix only stripping.
     */
    public void testPrefixOnlyStripping() throws IOException {
        List<String> prefixes = Arrays.asList("சிறு", "பெரு");
        Analyzer analyzer = createAnalyzer(2, true, false, null, prefixes);
        // With only prefix stripping, சிறுவன் -> வன்
        assertAnalyzesTo(analyzer, "சிறுவன்", new String[]{"வன்"});
        analyzer.close();
    }

    /**
     * Test that min_stem_length guard prevents over-stemming.
     */
    public void testMinStemLengthGuard() throws IOException {
        // With min_stem_length=4, stems of length 4 or more are OK
        // வீடுகள் -> வீடு (length 4) - OK
        assertStem("வீடுகள்", "வீடு", 4);

        // With min_stem_length=6, வீடு (length 4) is too short, so no stemming
        assertStem("வீடுகள்", "வீடுகள்", 6);
    }

    /**
     * Test that bare roots (no affix) pass through unchanged.
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

        // டையார் should match (possessive suffix)
        assertStem("அன்புடையார்", "அன்பு", 2);
    }

    /**
     * Test custom suffixes via list.
     */
    public void testCustomSuffixesList() throws IOException {
        List<String> customSuffixes = Arrays.asList("கள்", "க்கு");
        Analyzer analyzer = createAnalyzer(2, false, true, customSuffixes, null);
        assertAnalyzesTo(analyzer, "பள்ளிகள்", new String[]{"பள்ளி"});
        analyzer.close();
    }

    /**
     * Test custom suffixes via reader.
     */
    public void testCustomSuffixesReader() throws IOException {
        String suffixContent = "# Custom suffixes\nகள்\nக்கு\n";
        Analyzer analyzer = createAnalyzerWithReaders(2, false, true,
            new StringReader(suffixContent), null);
        assertAnalyzesTo(analyzer, "பள்ளிகள்", new String[]{"பள்ளி"});
        analyzer.close();
    }

    /**
     * Test disabling suffix stripping.
     */
    public void testDisableSuffixStripping() throws IOException {
        List<String> prefixes = Arrays.asList("சிறு");
        Analyzer analyzer = createAnalyzer(2, true, false, null, prefixes);
        // With suffixes disabled, only prefix should be stripped
        assertAnalyzesTo(analyzer, "சிறுவன்", new String[]{"வன்"});
        analyzer.close();
    }

    /**
     * Test multiple tokens in sequence.
     */
    public void testMultipleTokens() throws IOException {
        Analyzer analyzer = createAnalyzer(2, false, true, null, null);
        assertAnalyzesTo(analyzer,
            "பள்ளிக்கு வீட்டில் குழந்தைகள்",
            new String[]{"பள்ளி", "வீட்ட", "குழந்தை"});
        analyzer.close();
    }

    // Helper methods

    private void assertStem(String input, String expected, int minStemLength) throws IOException {
        Analyzer analyzer = createAnalyzer(minStemLength, false, true, null, null);
        assertAnalyzesTo(analyzer, input, new String[]{expected});
        analyzer.close();
    }

    private void assertStemBoth(String input, String expected, int minStemLength) throws IOException {
        Analyzer analyzer = createAnalyzer(minStemLength, true, true, null, null);
        assertAnalyzesTo(analyzer, input, new String[]{expected});
        analyzer.close();
    }

    private Analyzer createAnalyzer(int minStemLength, boolean stripPrefixes, boolean stripSuffixes,
                                    List<String> suffixes, List<String> prefixes) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream stream = new TamilStemmer(tokenizer, minStemLength,
                    stripPrefixes, stripSuffixes, suffixes, prefixes);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };
    }

    private Analyzer createAnalyzerWithReaders(int minStemLength, boolean stripPrefixes, boolean stripSuffixes,
                                               StringReader suffixReader, StringReader prefixReader) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream stream = new TamilStemmer(tokenizer, minStemLength,
                    stripPrefixes, stripSuffixes, suffixReader, prefixReader);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };
    }
}

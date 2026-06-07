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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for Tamil analysis plugin components.
 */
public class TamilAnalysisTests extends OpenSearchTestCase {

    /**
     * Test that the plugin registers the expected components.
     */
    public void testPluginRegistersComponents() throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisTamilPlugin());

        // Verify token filters are registered
        TokenFilterFactory stemmerFactory = analysis.tokenFilter.get("tamil_stemmer");
        assertThat(stemmerFactory, instanceOf(TamilStemTokenFilterFactory.class));

        TokenFilterFactory stopFactory = analysis.tokenFilter.get("tamil_stop");
        assertThat(stopFactory, instanceOf(TamilStopTokenFilterFactory.class));

        // Verify analyzer is registered - it's accessed through indexAnalyzers
        assertNotNull(analysis.indexAnalyzers.get("tamil"));
    }

    /**
     * Test that tamil_stop filter removes stopwords.
     */
    public void testStopFilter() throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisTamilPlugin());

        TokenFilterFactory stopFactory = analysis.tokenFilter.get("tamil_stop");
        assertNotNull(stopFactory);

        // நான் (I) is a stopword, தமிழ் (Tamil) is not
        List<String> tokens = analyzeWithFilter(stopFactory, "நான் தமிழ்");

        // நான் should be removed, தமிழ் should remain
        assertEquals(1, tokens.size());
        assertEquals("தமிழ்", tokens.get(0));
    }

    /**
     * Test that tamil_stemmer filter strips suffixes.
     */
    public void testStemmerFilter() throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisTamilPlugin());

        TokenFilterFactory stemmerFactory = analysis.tokenFilter.get("tamil_stemmer");
        assertNotNull(stemmerFactory);

        // பள்ளிக்கு = பள்ளி + க்கு (school + dative)
        List<String> tokens = analyzeWithFilter(stemmerFactory, "பள்ளிக்கு");

        assertEquals(1, tokens.size());
        assertEquals("பள்ளி", tokens.get(0));
    }

    /**
     * Test the prebuilt tamil analyzer.
     */
    public void testTamilAnalyzer() throws IOException {
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), Settings.EMPTY, new AnalysisTamilPlugin());

        Analyzer analyzer = analysis.indexAnalyzers.get("tamil");
        assertNotNull(analyzer);

        // Test with a sentence containing stopwords and inflected forms
        // நான் = I (stopword)
        // பள்ளிக்கு = to school (should be stemmed to பள்ளி)
        // போனேன் = went (no recognized suffix, passes through)
        List<String> tokens = analyzeWithAnalyzer(analyzer, "நான் பள்ளிக்கு போனேன்");

        // நான் should be removed (stopword)
        // பள்ளிக்கு should become பள்ளி (stemmed)
        // போனேன் should pass through
        assertFalse(tokens.contains("நான்"));
        assertTrue(tokens.contains("பள்ளி"));
        assertTrue(tokens.contains("போனேன்"));
    }

    /**
     * Test custom min_stem_length setting.
     */
    public void testCustomMinStemLength() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_stemmer.type", "tamil_stemmer")
            .put("index.analysis.filter.my_stemmer.min_stem_length", 6)
            .build();

        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisTamilPlugin());

        TokenFilterFactory stemmerFactory = analysis.tokenFilter.get("my_stemmer");
        assertNotNull(stemmerFactory);

        // With min_stem_length=6, shorter stems should not be produced
        // பள்ளி has length 5, so பள்ளிக்கு should NOT be stemmed (would produce stem < 6)
        List<String> tokens = analyzeWithFilter(stemmerFactory, "பள்ளிக்கு");
        assertEquals(1, tokens.size());
        assertEquals("பள்ளிக்கு", tokens.get(0));  // unchanged due to min_stem_length
    }

    /**
     * Test custom stopwords setting.
     */
    public void testCustomStopwords() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_stop.type", "tamil_stop")
            .putList("index.analysis.filter.my_stop.stopwords", "தமிழ்", "நாடு")
            .build();

        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisTamilPlugin());

        TokenFilterFactory stopFactory = analysis.tokenFilter.get("my_stop");
        assertNotNull(stopFactory);

        // தமிழ் and நாடு are now custom stopwords
        // நான் is NOT in the custom list (it's in the default list but custom replaces it)
        List<String> tokens = analyzeWithFilter(stopFactory, "நான் தமிழ் நாடு");

        // Only நான் should remain (custom list doesn't include it)
        assertEquals(1, tokens.size());
        assertEquals("நான்", tokens.get(0));
    }

    private List<String> analyzeWithFilter(TokenFilterFactory factory, String text) throws IOException {
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                org.apache.lucene.analysis.Tokenizer tokenizer = new org.apache.lucene.analysis.standard.StandardTokenizer();
                TokenStream stream = factory.create(tokenizer);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        return analyzeWithAnalyzer(analyzer, text);
    }

    private List<String> analyzeWithAnalyzer(Analyzer analyzer, String text) throws IOException {
        List<String> result = new ArrayList<>();
        try (TokenStream ts = analyzer.tokenStream("test", new StringReader(text))) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                result.add(termAtt.toString());
            }
            ts.end();
        }
        return result;
    }
}

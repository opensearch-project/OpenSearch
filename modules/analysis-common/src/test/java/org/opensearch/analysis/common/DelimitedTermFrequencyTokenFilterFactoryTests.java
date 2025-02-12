/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.StringReader;

public class DelimitedTermFrequencyTokenFilterFactoryTests extends OpenSearchTokenStreamTestCase {

    public void testDefault() throws Exception {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_delimited_term_freq.type", "delimited_term_freq")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        doTest(analysis, "cat|4 dog|5");
    }

    public void testDelimiter() throws Exception {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_delimited_term_freq.type", "delimited_term_freq")
                .put("index.analysis.filter.my_delimited_term_freq.delimiter", ":")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        doTest(analysis, "cat:4 dog:5");
    }

    public void testDelimiterLongerThanOneCharThrows() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .put("index.analysis.filter.my_delimited_term_freq.type", "delimited_term_freq")
                    .put("index.analysis.filter.my_delimited_term_freq.delimiter", "^^")
                    .build(),
                new CommonAnalysisModulePlugin()
            )
        );

        assertEquals("Setting [delimiter] must be a single, non-null character. [^^] was provided.", ex.getMessage());
    }

    private void doTest(OpenSearchTestCase.TestAnalysis analysis, String source) throws Exception {
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_delimited_term_freq");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        TokenStream stream = tokenFilter.create(tokenizer);

        CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
        TermFrequencyAttribute tfAtt = stream.getAttribute(TermFrequencyAttribute.class);
        stream.reset();
        assertTermEquals("cat", stream, termAtt, tfAtt, 4);
        assertTermEquals("dog", stream, termAtt, tfAtt, 5);
        assertFalse(stream.incrementToken());
        stream.end();
        stream.close();
    }

    void assertTermEquals(String expected, TokenStream stream, CharTermAttribute termAtt, TermFrequencyAttribute tfAtt, int expectedTf)
        throws Exception {
        assertTrue(stream.incrementToken());
        assertEquals(expected, termAtt.toString());
        assertEquals(expectedTf, tfAtt.getTermFrequency());
    }
}

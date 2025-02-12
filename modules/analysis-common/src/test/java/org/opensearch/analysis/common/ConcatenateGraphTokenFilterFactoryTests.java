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
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

public class ConcatenateGraphTokenFilterFactoryTests extends OpenSearchTokenStreamTestCase {
    public void testSimpleTokenizerAndConcatenate() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(),
            new CommonAnalysisModulePlugin()
        );

        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("concatenate_graph");
        String source = "PowerShot Is AweSome";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        assertTokenStreamContents(tokenFilter.create(tokenizer), new String[] { "PowerShot Is AweSome" });
    }

    public void testTokenizerCustomizedSeparator() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                .put("index.analysis.filter.my_concatenate_graph.token_separator", "+")
                .build(),
            new CommonAnalysisModulePlugin()
        );

        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_concatenate_graph");
        String source = "PowerShot Is AweSome";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        assertTokenStreamContents(tokenFilter.create(tokenizer), new String[] { "PowerShot+Is+AweSome" });
    }

    public void testTokenizerEmptySeparator() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                .put("index.analysis.filter.my_concatenate_graph.token_separator", "")
                .build(),
            new CommonAnalysisModulePlugin()
        );

        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_concatenate_graph");
        String source = "PowerShot Is AweSome";
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));

        assertTokenStreamContents(tokenFilter.create(tokenizer), new String[] { "PowerShotIsAweSome" });
    }

    public void testPreservePositionIncrementsDefault() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                .put("index.analysis.filter.my_concatenate_graph.token_separator", "+")
                .build(),
            new CommonAnalysisModulePlugin()
        );

        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_concatenate_graph");

        CannedTokenStream cannedTokenStream = new CannedTokenStream(
            new Token("a", 1, 0, 1),
            new Token("b", 2, 2, 3), // there is a gap, posInc is 2
            new Token("d", 1, 4, 5)
        );

        // the gap between a and b is not preserved
        assertTokenStreamContents(tokenFilter.create(cannedTokenStream), new String[] { "a+b+d" });
    }

    public void testPreservePositionIncrementsTrue() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                .put("index.analysis.filter.my_concatenate_graph.token_separator", "+")
                .put("index.analysis.filter.my_concatenate_graph.preserve_position_increments", "true")
                .build(),
            new CommonAnalysisModulePlugin()
        );

        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_concatenate_graph");

        CannedTokenStream cannedTokenStream = new CannedTokenStream(
            new Token("a", 1, 0, 1),
            new Token("b", 2, 2, 3), // there is a gap, posInc is 2
            new Token("d", 1, 4, 5)
        );

        // the gap between a and b is preserved
        assertTokenStreamContents(tokenFilter.create(cannedTokenStream), new String[] { "a++b+d" });
    }

    public void testGraph() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter_graph")
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_analyzer.filter", "my_word_delimiter, my_concatenate_graph")
                .build(),
            new CommonAnalysisModulePlugin()
        );

        String source = "PowerShot Is AweSome";

        // Expected output from Whitespace Tokenizer is: "PowerShot" --> "Is" --> "Awe" --> "Some"
        // Expected output from word_delimiter_graph is a graph:
        // <start> ---> "Power" --> "Shot" ---> "Is" ---> "Awe" ---> "Some" --- <end>
        // | | | |
        // --> "PowerShot" -------- --> "AweSome" ---------
        // and this filter will traverse through all possible paths to produce concatenated tokens
        String[] expected = new String[] {
            "Power Shot Is Awe Some",
            "Power Shot Is AweSome",
            "PowerShot Is Awe Some",
            "PowerShot Is AweSome" };

        // all tokens will be in the same position
        int[] expectedPosIncrements = new int[] { 1, 0, 0, 0 };
        int[] expectedPosLengths = new int[] { 1, 1, 1, 1 };

        NamedAnalyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        assertAnalyzesToPositions(analyzer, source, expected, expectedPosIncrements, expectedPosLengths);
    }

    public void testInvalidSeparator() {
        expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                    .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                    .put("index.analysis.filter.my_concatenate_graph.token_separator", "11")
                    .build(),
                new CommonAnalysisModulePlugin()
            )
        );
    }

    /**
     * Similar to the {@link #testGraph()} case, there will be 4 paths generated by word_delimiter_graph.
     * By setting max_graph_expansions to 3, we expect an exception.
     */
    public void testMaxGraphExpansion() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", "word_delimiter_graph")
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_concatenate_graph.type", "concatenate_graph")
                .put("index.analysis.filter.my_concatenate_graph.max_graph_expansions", "3")
                .put("index.analysis.analyzer.my_analyzer.type", "custom")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_analyzer.filter", "my_word_delimiter, my_concatenate_graph")
                .build(),
            new CommonAnalysisModulePlugin()
        );

        String source = "PowerShot Is AweSome";

        TokenStream tokenStream = analysis.indexAnalyzers.get("my_analyzer").tokenStream("dummy", source);

        tokenStream.reset();

        expectThrows(TooComplexToDeterminizeException.class, tokenStream::incrementToken);
    }
}

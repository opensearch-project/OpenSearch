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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

/**
 * Base class to test {@link WordDelimiterTokenFilterFactory} and
 * {@link WordDelimiterGraphTokenFilterFactory}.
 */
public abstract class BaseWordDelimiterTokenFilterFactoryTestCase extends OpenSearchTokenStreamTestCase {
    final String type;

    public BaseWordDelimiterTokenFilterFactoryTestCase(String type) {
        this.type = type;
    }

    public void testDefault() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] { "Power", "Shot", "500", "42", "wi", "fi", "wi", "fi", "4000", "j", "2", "se", "O", "Neil" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testCatenateWords() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "false")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] { "PowerShot", "500", "42", "wifi", "wifi", "4000", "j", "2", "se", "ONeil" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testCatenateNumbers() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.generate_number_parts", "false")
                .put("index.analysis.filter.my_word_delimiter.catenate_numbers", "true")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] { "Power", "Shot", "50042", "wi", "fi", "wi", "fi", "4000", "j", "2", "se", "O", "Neil" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testCatenateAll() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "false")
                .put("index.analysis.filter.my_word_delimiter.generate_number_parts", "false")
                .put("index.analysis.filter.my_word_delimiter.catenate_all", "true")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] { "PowerShot", "50042", "wifi", "wifi4000", "j2se", "ONeil" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testSplitOnCaseChange() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.split_on_case_change", "false")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot";
        String[] expected = new String[] { "PowerShot" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testPreserveOriginal() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.preserve_original", "true")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] {
            "PowerShot",
            "Power",
            "Shot",
            "500-42",
            "500",
            "42",
            "wi-fi",
            "wi",
            "fi",
            "wi-fi-4000",
            "wi",
            "fi",
            "4000",
            "j2se",
            "j",
            "2",
            "se",
            "O'Neil's",
            "O",
            "Neil" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testStemEnglishPossessive() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .put("index.analysis.filter.my_word_delimiter.stem_english_possessive", "false")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_word_delimiter");
        String source = "PowerShot 500-42 wi-fi wi-fi-4000 j2se O'Neil's";
        String[] expected = new String[] { "Power", "Shot", "500", "42", "wi", "fi", "wi", "fi", "4000", "j", "2", "se", "O", "Neil", "s" };
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    private void createTokenFilterFactoryWithTypeTable(String[] rules) throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_word_delimiter.type", type)
                .putList("index.analysis.filter.my_word_delimiter.type_table", rules)
                .put("index.analysis.filter.my_word_delimiter.catenate_words", "true")
                .put("index.analysis.filter.my_word_delimiter.generate_word_parts", "true")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        analysis.tokenFilter.get("my_word_delimiter");
    }

    public void testTypeTableParsingError() {
        String[] rules = { "# This is a comment", "# => ALPHANUM", "$ => DIGIT", "\\u200D => ALPHANUM", "abc => ALPHA" };
        RuntimeException ex = expectThrows(RuntimeException.class, () -> createTokenFilterFactoryWithTypeTable(rules));
        assertEquals("Line [5]: Invalid mapping rule: [abc => ALPHA]. Only a single character is allowed.", ex.getMessage());
    }
}

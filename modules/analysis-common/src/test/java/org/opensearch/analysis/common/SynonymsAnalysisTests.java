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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.PreConfiguredTokenFilter;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class SynonymsAnalysisTests extends OpenSearchTestCase {
    private IndexAnalyzers indexAnalyzers;

    public void testSynonymsAnalysis() throws IOException {
        InputStream synonyms = getClass().getResourceAsStream("synonyms.txt");
        InputStream synonymsWordnet = getClass().getResourceAsStream("synonyms_wordnet.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(synonyms, config.resolve("synonyms.txt"));
        Files.copy(synonymsWordnet, config.resolve("synonyms_wordnet.txt"));

        String json = "/org/opensearch/analysis/common/synonyms.json";
        Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), home)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;

        match("synonymAnalyzer", "foobar is the dude abides", "fred is the opensearch man!");
        match("synonymAnalyzer_file", "foobar is the dude abides", "fred is the opensearch man!");
        match("synonymAnalyzerWordnet", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWordnet_file", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWithsettings", "foobar", "fre red");
        match("synonymAnalyzerWithStopAfterSynonym", "foobar is the dude abides , stop", "fred is the opensearch man! ,");
        match("synonymAnalyzerWithStopBeforeSynonym", "foobar is the dude abides , stop", "fred is the opensearch man! ,");
        match("synonymAnalyzerWithStopSynonymAfterSynonym", "foobar is the dude abides", "fred is the man!");
        match("synonymAnalyzerExpand", "foobar is the dude abides", "foobar fred is the dude opensearch abides man!");
        match("synonymAnalyzerExpandWithStopAfterSynonym", "foobar is the dude abides", "fred is the dude abides man!");

    }

    public void testSynonymWordDeleteByAnalyzer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonym.type", "synonym")
            .putList("index.analysis.filter.synonym.synonyms", "foobar => fred", "dude => opensearch", "abides => man!")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "foobar", "opensearch")
            .put("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.filter", "stop_within_synonym", "synonym")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;
            fail("fail! due to synonym word deleted by analyzer");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("Failed to build synonyms"));
        }
    }

    public void testExpandSynonymWordDeleteByAnalyzer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonym_expand.type", "synonym")
            .putList("index.analysis.filter.synonym_expand.synonyms", "foobar, fred", "dude, opensearch", "abides, man!")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "foobar", "opensearch")
            .put("index.analysis.analyzer.synonymAnalyzerExpandWithStopBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerExpandWithStopBeforeSynonym.filter", "stop_within_synonym", "synonym_expand")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        try {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;
            fail("fail! due to synonym word deleted by analyzer");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("Failed to build synonyms"));
        }
    }

    public void testSynonymsWrappedByMultiplexer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "programmer, developer")
            .put("index.analysis.filter.my_english.type", "stemmer")
            .put("index.analysis.filter.my_english.language", "porter2")
            .put("index.analysis.filter.stem_repeat.type", "multiplexer")
            .putList("index.analysis.filter.stem_repeat.filters", "my_english, synonyms")
            .put("index.analysis.analyzer.synonymAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonymAnalyzer.filter", "lowercase", "stem_repeat")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("synonymAnalyzer"),
            "Some developers are odd",
            new String[] { "some", "developers", "develop", "programm", "are", "odd" },
            new int[] { 1, 1, 0, 0, 1, 1 }
        );
    }

    public void testAsciiFoldingFilterForSynonyms() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "hoj, height")
            .put("index.analysis.analyzer.synonymAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonymAnalyzer.filter", "lowercase", "asciifolding", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("synonymAnalyzer"),
            "høj",
            new String[] { "hoj", "height" },
            new int[] { 1, 0 }
        );
    }

    public void testPreconfigured() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "würst, sausage")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase", "asciifolding", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("my_analyzer"),
            "würst",
            new String[] { "wurst", "sausage" },
            new int[] { 1, 0 }
        );
    }

    public void testChainedSynonymFilters() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms1.type", "synonym")
            .putList("index.analysis.filter.synonyms1.synonyms", "term1, term2")
            .put("index.analysis.filter.synonyms2.type", "synonym")
            .putList("index.analysis.filter.synonyms2.synonyms", "term1, term3")
            .put("index.analysis.analyzer.syn.tokenizer", "standard")
            .putList("index.analysis.analyzer.syn.filter", "lowercase", "synonyms1", "synonyms2")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("syn"),
            "term1",
            new String[] { "term1", "term3", "term2" },
            new int[] { 1, 0, 0 }
        );
    }

    public void testShingleFilters() {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "programmer, developer")
            .put("index.analysis.filter.my_shingle.type", "shingle")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "my_shingle", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        expectThrows(IllegalArgumentException.class, () -> {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;
        });

    }

    public void testTokenFiltersBypassSynonymAnalysis() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .putList("word_list", "a")
            .put("hyphenation_patterns_path", "foo")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        Environment environment = TestEnvironment.newEnvironment(settings);
        AnalysisModule analysisModule = new AnalysisModule(environment, Collections.singletonList(new CommonAnalysisModulePlugin()));
        AnalysisRegistry analysisRegistry = analysisModule.getAnalysisRegistry();
        String[] bypassingFactories = new String[] { "dictionary_decompounder" };

        CommonAnalysisModulePlugin plugin = new CommonAnalysisModulePlugin();
        for (String factory : bypassingFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters(analysisModule).get(factory).get(idxSettings, environment, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, environment, "keyword", settings);
            SynonymTokenFilterFactory stff = new SynonymTokenFilterFactory(idxSettings, environment, "synonym", settings, analysisRegistry);
            Analyzer analyzer = stff.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff), null);

            try (TokenStream ts = analyzer.tokenStream("field", "text")) {
                assertThat(ts, instanceOf(KeywordTokenizer.class));
            }
        }

    }

    public void testPreconfiguredTokenFilters() throws IOException {
        Set<String> disallowedFilters = new HashSet<>(
            Arrays.asList(
                "common_grams",
                "edge_ngram",
                "edgeNGram",
                "keyword_repeat",
                "ngram",
                "nGram",
                "shingle",
                "word_delimiter",
                "word_delimiter_graph"
            )
        );

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        CommonAnalysisModulePlugin plugin = new CommonAnalysisModulePlugin();

        for (PreConfiguredTokenFilter tf : plugin.getPreConfiguredTokenFilters()) {
            if (disallowedFilters.contains(tf.getName())) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    "Expected exception for factory " + tf.getName(),
                    () -> {
                        tf.get(idxSettings, null, tf.getName(), settings).getSynonymFilter();
                    }
                );
                assertEquals(tf.getName(), "Token filter [" + tf.getName() + "] cannot be used to parse synonyms", e.getMessage());
            } else {
                tf.get(idxSettings, null, tf.getName(), settings).getSynonymFilter();
            }
        }
    }

    public void testDisallowedTokenFilters() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT))
            .put("path.home", createTempDir().toString())
            .putList("common_words", "a", "b")
            .put("output_unigrams", "true")
            .build();

        Environment environment = TestEnvironment.newEnvironment(settings);
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        AnalysisModule analysisModule = new AnalysisModule(environment, Collections.singletonList(new CommonAnalysisModulePlugin()));
        AnalysisRegistry analysisRegistry = analysisModule.getAnalysisRegistry();
        CommonAnalysisModulePlugin plugin = new CommonAnalysisModulePlugin();

        String[] disallowedFactories = new String[] {
            "multiplexer",
            "cjk_bigram",
            "common_grams",
            "ngram",
            "edge_ngram",
            "word_delimiter",
            "word_delimiter_graph",
            "fingerprint" };

        for (String factory : disallowedFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters(analysisModule).get(factory).get(idxSettings, environment, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, environment, "keyword", settings);
            SynonymTokenFilterFactory stff = new SynonymTokenFilterFactory(idxSettings, environment, "synonym", settings, analysisRegistry);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "Expected IllegalArgumentException for factory " + factory,
                () -> stff.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff), null)
            );

            assertEquals(factory, "Token filter [" + factory + "] cannot be used to parse synonyms", e.getMessage());
        }
    }

    private void match(String analyzerName, String source, String target) throws IOException {
        Analyzer analyzer = indexAnalyzers.get(analyzerName).analyzer();

        TokenStream stream = analyzer.tokenStream("", source);
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

        StringBuilder sb = new StringBuilder();
        while (stream.incrementToken()) {
            sb.append(termAtt.toString()).append(" ");
        }

        MatcherAssert.assertThat(target, equalTo(sb.toString().trim()));
    }

    /**
     * Tests the integration of word delimiter and synonym graph filters with synonym_analyzer based on issue #16263.
     * This test verifies the correct handling of:
     * 1. Hyphenated words with word delimiter (e.g., "note-book" → ["notebook", "note", "book"])
     * 2. Multi-word synonyms (e.g., "mobile phone" → ["smartphone"])
     * 3. Single word synonyms (e.g., "laptop" → ["notebook"])
     *
     * @see <a href="https://github.com/opensearch-project/OpenSearch/issues/16263">Issue #16263</a>
     */
    public void testSynonymAnalyzerWithWordDelimiter() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.custom_word_delimiter.type", "word_delimiter_graph")
            .put("index.analysis.filter.custom_word_delimiter.generate_word_parts", true)
            .put("index.analysis.filter.custom_word_delimiter.catenate_all", true)
            .put("index.analysis.filter.custom_word_delimiter.split_on_numerics", false)
            .put("index.analysis.filter.custom_word_delimiter.split_on_case_change", false)
            .put("index.analysis.filter.custom_pattern_replace_filter.type", "pattern_replace")
            .put("index.analysis.filter.custom_pattern_replace_filter.pattern", "(-)")
            .put("index.analysis.filter.custom_pattern_replace_filter.replacement", " ")
            .put("index.analysis.filter.custom_pattern_replace_filter.all", true)
            .put("index.analysis.filter.custom_synonym_graph_filter.type", "synonym_graph")
            .putList(
                "index.analysis.filter.custom_synonym_graph_filter.synonyms",
                "laptop => notebook",
                "smartphone, mobile phone, cell phone => smartphone",
                "tv, television => television"
            )
            .put("index.analysis.filter.custom_synonym_graph_filter.synonym_analyzer", "standard")
            .put("index.analysis.analyzer.text_en_index.type", "custom")
            .put("index.analysis.analyzer.text_en_index.tokenizer", "whitespace")
            .putList(
                "index.analysis.analyzer.text_en_index.filter",
                "lowercase",
                "custom_word_delimiter",
                "custom_synonym_graph_filter",
                "custom_pattern_replace_filter",
                "flatten_graph"
            )
            .build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisModule module = new AnalysisModule(environment, Collections.singletonList(new CommonAnalysisModulePlugin()));
        IndexAnalyzers analyzers = module.getAnalysisRegistry().build(indexSettings);
        try (TokenStream ts = analyzers.get("text_en_index").tokenStream("", "note-book")) {
            assertTokenStreamContents(
                ts,
                new String[] { "notebook", "note", "book" },
                new int[] { 0, 0, 5 },
                new int[] { 9, 4, 9 },
                new String[] { "word", "word", "word" },
                new int[] { 1, 0, 1 },
                new int[] { 2, 1, 1 }
            );
        }
        try (TokenStream ts = analyzers.get("text_en_index").tokenStream("", "mobile phone")) {
            assertTokenStreamContents(
                ts,
                new String[] { "smartphone" },
                new int[] { 0 },
                new int[] { 12 },
                new String[] { "SYNONYM" },
                new int[] { 1 },
                new int[] { 1 }
            );
        }
        try (TokenStream ts = analyzers.get("text_en_index").tokenStream("", "laptop")) {
            assertTokenStreamContents(ts, new String[] { "notebook" }, new int[] { 0 }, new int[] { 6 });
        }
    }
}

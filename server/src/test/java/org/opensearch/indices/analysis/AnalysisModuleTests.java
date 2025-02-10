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

package org.opensearch.indices.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.Streams;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.Analysis;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.CustomAnalyzer;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.MyFilterTokenFilterFactory;
import org.opensearch.index.analysis.NameOrDefinition;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.PreConfiguredCharFilter;
import org.opensearch.index.analysis.PreConfiguredTokenFilter;
import org.opensearch.index.analysis.PreConfiguredTokenizer;
import org.opensearch.index.analysis.StandardTokenizerFactory;
import org.opensearch.index.analysis.StopTokenFilterFactory;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.hamcrest.MatcherAssert;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

public class AnalysisModuleTests extends OpenSearchTestCase {
    private final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public IndexAnalyzers getIndexAnalyzers(Settings settings) throws IOException {
        return getIndexAnalyzers(getNewRegistry(settings), settings);
    }

    public IndexAnalyzers getIndexAnalyzers(AnalysisRegistry registry, Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        return registry.build(idxSettings);
    }

    public AnalysisRegistry getNewRegistry(Settings settings) {
        try {
            return new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(new AnalysisPlugin() {
                @Override
                public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                    return singletonMap("myfilter", MyFilterTokenFilterFactory::new);
                }

                @Override
                public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
                    return AnalysisPlugin.super.getCharFilters();
                }
            })).getAnalysisRegistry();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Settings loadFromClasspath(String path) throws IOException {
        return Settings.builder()
            .loadFromStream(path, getClass().getResourceAsStream(path), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
    }

    public void testSimpleConfigurationJson() throws IOException {
        Settings settings = loadFromClasspath("/org/opensearch/index/analysis/test1.json");
        testSimpleConfiguration(settings);
    }

    public void testSimpleConfigurationYaml() throws IOException {
        Settings settings = loadFromClasspath("/org/opensearch/index/analysis/test1.yml");
        testSimpleConfiguration(settings);
    }

    private void testSimpleConfiguration(Settings settings) throws IOException {
        IndexAnalyzers indexAnalyzers = getIndexAnalyzers(settings);
        Analyzer analyzer = indexAnalyzers.get("custom1").analyzer();

        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom1 = (CustomAnalyzer) analyzer;
        assertThat(custom1.tokenizerFactory(), instanceOf(StandardTokenizerFactory.class));
        assertThat(custom1.tokenFilters().length, equalTo(2));

        StopTokenFilterFactory stop1 = (StopTokenFilterFactory) custom1.tokenFilters()[0];
        assertThat(stop1.stopWords().size(), equalTo(1));

        // verify position increment gap
        analyzer = indexAnalyzers.get("custom6").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom6 = (CustomAnalyzer) analyzer;
        assertThat(custom6.getPositionIncrementGap("any_string"), equalTo(256));

        // check custom class name (my)
        analyzer = indexAnalyzers.get("custom4").analyzer();
        assertThat(analyzer, instanceOf(CustomAnalyzer.class));
        CustomAnalyzer custom4 = (CustomAnalyzer) analyzer;
        assertThat(custom4.tokenFilters()[0], instanceOf(MyFilterTokenFilterFactory.class));
    }

    public void testWordListPath() throws Exception {
        Path home = createTempDir();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home.toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        String[] words = new String[] { "donau", "dampf", "schiff", "spargel", "creme", "suppe" };

        Path wordListFile = generateWordList(home, words);
        settings = Settings.builder()
            .loadFromSource("index: \n  word_list_path: " + wordListFile.toAbsolutePath(), XContentType.YAML)
            .build();

        Set<?> wordList = Analysis.getWordSet(env, settings, "index.word_list");
        MatcherAssert.assertThat(wordList.size(), equalTo(6));
        // MatcherAssert.assertThat(wordList, hasItems(words));
        Files.delete(wordListFile);
    }

    private Path generateWordList(Path home, String[] words) throws Exception {
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Path wordListFile = config.resolve("wordlist.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(wordListFile, StandardCharsets.UTF_8)) {
            for (String word : words) {
                writer.write(word);
                writer.write('\n');
            }
        }
        return wordListFile;
    }

    public void testUnderscoreInAnalyzerName() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.analyzer._invalid_name.tokenizer", "standard")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, "1")
            .build();
        try {
            getIndexAnalyzers(settings);
            fail("This should fail with IllegalArgumentException because the analyzers name starts with _");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                either(equalTo("analyzer name must not start with '_'. got \"_invalid_name\"")).or(
                    equalTo("analyzer name must not start with '_'. got \"_invalidName\"")
                )
            );
        }
    }

    /**
     * Tests that plugins can register pre-configured char filters that vary in behavior based on OpenSearch version, Lucene version,
     * and that do not vary based on version at all.
     */
    public void testPluginPreConfiguredCharFilters() throws IOException {
        boolean noVersionSupportsMultiTerm = randomBoolean();
        boolean luceneVersionSupportsMultiTerm = randomBoolean();
        boolean opensearchVersionSupportsMultiTerm = randomBoolean();
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            singletonList(new AnalysisPlugin() {
                @Override
                public List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
                    return Arrays.asList(
                        PreConfiguredCharFilter.singleton(
                            "no_version",
                            noVersionSupportsMultiTerm,
                            tokenStream -> new AppendCharFilter(tokenStream, "no_version")
                        ),
                        PreConfiguredCharFilter.luceneVersion(
                            "lucene_version",
                            luceneVersionSupportsMultiTerm,
                            (tokenStream, luceneVersion) -> new AppendCharFilter(tokenStream, luceneVersion.toString())
                        ),
                        PreConfiguredCharFilter.openSearchVersion(
                            "opensearch_version",
                            opensearchVersionSupportsMultiTerm,
                            (tokenStream, esVersion) -> new AppendCharFilter(tokenStream, esVersion.toString())
                        )
                    );
                }

                @Override
                public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
                    // Need mock keyword tokenizer here, because alpha / beta versions are broken up by the dash.
                    return singletonMap(
                        "keyword",
                        (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                            name,
                            () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                        )
                    );
                }
            })
        ).getAnalysisRegistry();

        Version version = VersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            registry,
            Settings.builder()
                .put("index.analysis.analyzer.no_version.tokenizer", "keyword")
                .put("index.analysis.analyzer.no_version.char_filter", "no_version")
                .put("index.analysis.analyzer.lucene_version.tokenizer", "keyword")
                .put("index.analysis.analyzer.lucene_version.char_filter", "lucene_version")
                .put("index.analysis.analyzer.opensearch_version.tokenizer", "keyword")
                .put("index.analysis.analyzer.opensearch_version.char_filter", "opensearch_version")
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build()
        );
        assertTokenStreamContents(analyzers.get("no_version").tokenStream("", "test"), new String[] { "testno_version" });
        assertTokenStreamContents(analyzers.get("lucene_version").tokenStream("", "test"), new String[] { "test" + version.luceneVersion });
        assertTokenStreamContents(analyzers.get("opensearch_version").tokenStream("", "test"), new String[] { "test" + version });

        assertEquals(
            "test" + (noVersionSupportsMultiTerm ? "no_version" : ""),
            analyzers.get("no_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (luceneVersionSupportsMultiTerm ? version.luceneVersion.toString() : ""),
            analyzers.get("lucene_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (opensearchVersionSupportsMultiTerm ? version.toString() : ""),
            analyzers.get("opensearch_version").normalize("", "test").utf8ToString()
        );
    }

    /**
     * Tests that plugins can register pre-configured token filters that vary in behavior based on OpenSearch version, Lucene version,
     * and that do not vary based on version at all.
     */
    public void testPluginPreConfiguredTokenFilters() throws IOException {
        boolean noVersionSupportsMultiTerm = randomBoolean();
        boolean luceneVersionSupportsMultiTerm = randomBoolean();
        boolean opensearchVersionSupportsMultiTerm = randomBoolean();
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            singletonList(new AnalysisPlugin() {
                @Override
                public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
                    return Arrays.asList(
                        PreConfiguredTokenFilter.singleton(
                            "no_version",
                            noVersionSupportsMultiTerm,
                            tokenStream -> new AppendTokenFilter(tokenStream, "no_version")
                        ),
                        PreConfiguredTokenFilter.luceneVersion(
                            "lucene_version",
                            luceneVersionSupportsMultiTerm,
                            (tokenStream, luceneVersion) -> new AppendTokenFilter(tokenStream, luceneVersion.toString())
                        ),
                        PreConfiguredTokenFilter.openSearchVersion(
                            "opensearch_version",
                            opensearchVersionSupportsMultiTerm,
                            (tokenStream, esVersion) -> new AppendTokenFilter(tokenStream, esVersion.toString())
                        )
                    );
                }
            })
        ).getAnalysisRegistry();

        Version version = VersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            registry,
            Settings.builder()
                .put("index.analysis.analyzer.no_version.tokenizer", "standard")
                .put("index.analysis.analyzer.no_version.filter", "no_version")
                .put("index.analysis.analyzer.lucene_version.tokenizer", "standard")
                .put("index.analysis.analyzer.lucene_version.filter", "lucene_version")
                .put("index.analysis.analyzer.opensearch_version.tokenizer", "standard")
                .put("index.analysis.analyzer.opensearch_version.filter", "opensearch_version")
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build()
        );
        assertTokenStreamContents(analyzers.get("no_version").tokenStream("", "test"), new String[] { "testno_version" });
        assertTokenStreamContents(analyzers.get("lucene_version").tokenStream("", "test"), new String[] { "test" + version.luceneVersion });
        assertTokenStreamContents(analyzers.get("opensearch_version").tokenStream("", "test"), new String[] { "test" + version });

        assertEquals(
            "test" + (noVersionSupportsMultiTerm ? "no_version" : ""),
            analyzers.get("no_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (luceneVersionSupportsMultiTerm ? version.luceneVersion.toString() : ""),
            analyzers.get("lucene_version").normalize("", "test").utf8ToString()
        );
        assertEquals(
            "test" + (opensearchVersionSupportsMultiTerm ? version.toString() : ""),
            analyzers.get("opensearch_version").normalize("", "test").utf8ToString()
        );
    }

    /**
     * Tests that plugins can register pre-configured token filters that vary in behavior based on OpenSearch version, Lucene version,
     * and that do not vary based on version at all.
     */
    public void testPluginPreConfiguredTokenizers() throws IOException {

        // Simple tokenizer that always spits out a single token with some preconfigured characters
        final class FixedTokenizer extends Tokenizer {
            private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
            private final char[] chars;
            private boolean read = false;

            protected FixedTokenizer(String chars) {
                this.chars = chars.toCharArray();
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (read) {
                    return false;
                }
                clearAttributes();
                read = true;
                term.resizeBuffer(chars.length);
                System.arraycopy(chars, 0, term.buffer(), 0, chars.length);
                term.setLength(chars.length);
                return true;
            }

            @Override
            public void reset() throws IOException {
                super.reset();
                read = false;
            }
        }
        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            singletonList(new AnalysisPlugin() {
                @Override
                public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
                    return Arrays.asList(
                        PreConfiguredTokenizer.singleton("no_version", () -> new FixedTokenizer("no_version")),
                        PreConfiguredTokenizer.luceneVersion(
                            "lucene_version",
                            luceneVersion -> new FixedTokenizer(luceneVersion.toString())
                        ),
                        PreConfiguredTokenizer.openSearchVersion(
                            "opensearch_version",
                            esVersion -> new FixedTokenizer(esVersion.toString())
                        )
                    );
                }
            })
        ).getAnalysisRegistry();

        Version version = VersionUtils.randomVersion(random());
        IndexAnalyzers analyzers = getIndexAnalyzers(
            registry,
            Settings.builder()
                .put("index.analysis.analyzer.no_version.tokenizer", "no_version")
                .put("index.analysis.analyzer.lucene_version.tokenizer", "lucene_version")
                .put("index.analysis.analyzer.opensearch_version.tokenizer", "opensearch_version")
                .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                .build()
        );
        assertTokenStreamContents(analyzers.get("no_version").tokenStream("", "test"), new String[] { "no_version" });
        assertTokenStreamContents(
            analyzers.get("lucene_version").tokenStream("", "test"),
            new String[] { version.luceneVersion.toString() }
        );
        assertTokenStreamContents(analyzers.get("opensearch_version").tokenStream("", "test"), new String[] { version.toString() });

        // These are current broken by https://github.com/elastic/elasticsearch/issues/24752
        // assertEquals("test" + (noVersionSupportsMultiTerm ? "no_version" : ""),
        // analyzers.get("no_version").normalize("", "test").utf8ToString());
        // assertEquals("test" + (luceneVersionSupportsMultiTerm ? version.luceneVersion.toString() : ""),
        // analyzers.get("lucene_version").normalize("", "test").utf8ToString());
        // assertEquals("test" + (opensearchVersionSupportsMultiTerm ? version.toString() : ""),
        // analyzers.get("opensearch_version").normalize("", "test").utf8ToString());
    }

    public void testRegisterHunspellDictionary() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        InputStream aff = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.aff");
        InputStream dic = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.dic");
        Dictionary dictionary;
        try (Directory tmp = new NIOFSDirectory(environment.tmpDir())) {
            dictionary = new Dictionary(tmp, "hunspell", aff, dic);
        }
        AnalysisModule module = new AnalysisModule(environment, singletonList(new AnalysisPlugin() {
            @Override
            public Map<String, Dictionary> getHunspellDictionaries() {
                return singletonMap("foo", dictionary);
            }
        }));
        assertSame(dictionary, module.getHunspellService().getDictionary("foo"));
    }

    // Simple char filter that appends text to the term
    public static class AppendCharFilter extends CharFilter {

        static Reader append(Reader input, String appendMe) {
            try {
                return new StringReader(Streams.copyToString(input) + appendMe);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public AppendCharFilter(Reader input, String appendMe) {
            super(append(input, appendMe));
        }

        @Override
        protected int correct(int currentOff) {
            return currentOff;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            return input.read(cbuf, off, len);
        }
    }

    // Simple token filter that appends text to the term
    private static class AppendTokenFilter extends TokenFilter {
        public static TokenFilterFactory factoryForSuffix(String suffix) {
            return new TokenFilterFactory() {
                @Override
                public String name() {
                    return suffix;
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return new AppendTokenFilter(tokenStream, suffix);
                }
            };
        }

        private final CharTermAttribute term = addAttribute(CharTermAttribute.class);
        private final char[] appendMe;

        protected AppendTokenFilter(TokenStream input, String appendMe) {
            super(input);
            this.appendMe = appendMe.toCharArray();
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (false == input.incrementToken()) {
                return false;
            }
            term.resizeBuffer(term.length() + appendMe.length);
            System.arraycopy(appendMe, 0, term.buffer(), term.length(), appendMe.length);
            term.setLength(term.length() + appendMe.length);
            return true;
        }
    }

    /**
     * Tests registration and functionality of token filters that require access to the AnalysisModule.
     * This test verifies the token filter registration using the extended getTokenFilters(AnalysisModule) method
     */
    public void testTokenFilterRegistrationWithModuleReference() throws IOException {
        class TestPlugin implements AnalysisPlugin {
            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters(AnalysisModule module) {
                return Map.of(
                    "test_filter",
                    (indexSettings, env, name, settings) -> AppendTokenFilter.factoryForSuffix("_" + module.hashCode())
                );
            }
        }
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .put("index.analysis.analyzer.my_analyzer.filter", "test_filter")
            .build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        AnalysisModule module = new AnalysisModule(environment, singletonList(new TestPlugin()));
        AnalysisRegistry registry = module.getAnalysisRegistry();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder().put(settings).build());
        Map<String, TokenFilterFactory> tokenFilterFactories = registry.buildTokenFilterFactories(indexSettings);
        assertTrue("Token filter 'test_filter' should be registered", tokenFilterFactories.containsKey("test_filter"));
        IndexAnalyzers analyzers = registry.build(indexSettings);
        String testText = "test";
        TokenStream tokenStream = analyzers.get("my_analyzer").tokenStream("", testText);
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        assertTrue("Should have found a token", tokenStream.incrementToken());
        assertEquals("Token should have expected suffix", "test_" + module.hashCode(), charTermAttribute.toString());
        assertFalse("Should not have additional tokens", tokenStream.incrementToken());
        tokenStream.close();
        NamedAnalyzer customAnalyzer = registry.buildCustomAnalyzer(
            indexSettings,
            false,
            new NameOrDefinition("standard"),
            Collections.emptyList(),
            Collections.singletonList(new NameOrDefinition("test_filter"))
        );
        tokenStream = customAnalyzer.tokenStream("", testText);
        charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        assertTrue("Custom analyzer should produce a token", tokenStream.incrementToken());
        assertEquals("Custom analyzer token should have expected suffix", "test_" + module.hashCode(), charTermAttribute.toString());
        assertFalse("Custom analyzer should not produce additional tokens", tokenStream.incrementToken());
        tokenStream.close();
    }
}

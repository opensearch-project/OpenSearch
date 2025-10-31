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

package org.opensearch.index.analysis;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.MockTokenFilter;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperException;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.indices.analysis.PreBuiltAnalyzers;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AnalysisRegistryTests extends OpenSearchTestCase {
    private AnalysisRegistry emptyRegistry;
    private AnalysisRegistry nonEmptyRegistry;

    private static AnalyzerProvider<?> analyzerProvider(final String name) {
        return new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDEX, new EnglishAnalyzer());
    }

    private static AnalysisRegistry emptyAnalysisRegistry(Settings settings) {
        return new AnalysisRegistry(
            TestEnvironment.newEnvironment(settings),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
    }

    /**
     * Creates a reverse filter available for use in testNameClashNormalizer test
     */
    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {
        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("reverse", true, ReverseStringFilter::new));
        }
    }

    private static IndexSettings indexSettingsOfCurrentVersion(Settings.Builder settings) {
        return IndexSettingsModule.newIndexSettings("index", settings.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        emptyRegistry = emptyAnalysisRegistry(settings);
        // Module loaded to register in-built normalizers for testing
        AnalysisModule module = new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(new MockAnalysisPlugin()));
        nonEmptyRegistry = module.getAnalysisRegistry();
    }

    public void testDefaultAnalyzers() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        IndexAnalyzers indexAnalyzers = emptyRegistry.build(idxSettings);
        assertThat(indexAnalyzers.getDefaultIndexAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchQuoteAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
    }

    public void testOverrideDefaultAnalyzer() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexAnalyzers indexAnalyzers = emptyRegistry.build(
            IndexSettingsModule.newIndexSettings("index", settings),
            singletonMap("default", analyzerProvider("default")),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        assertThat(indexAnalyzers.getDefaultIndexAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    public void testOverrideDefaultAnalyzerWithoutAnalysisModeAll() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settings);
        TokenFilterFactory tokenFilter = new AbstractTokenFilterFactory(indexSettings, "my_filter", Settings.EMPTY) {
            @Override
            public AnalysisMode getAnalysisMode() {
                return randomFrom(AnalysisMode.SEARCH_TIME, AnalysisMode.INDEX_TIME);
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }
        };
        TokenizerFactory tokenizer = new AbstractTokenizerFactory(indexSettings, Settings.EMPTY, "my_tokenizer") {
            @Override
            public Tokenizer create() {
                return new StandardTokenizer();
            }
        };
        Analyzer analyzer = new CustomAnalyzer(tokenizer, new CharFilterFactory[0], new TokenFilterFactory[] { tokenFilter });
        MapperException ex = expectThrows(
            MapperException.class,
            () -> emptyRegistry.build(
                IndexSettingsModule.newIndexSettings("index", settings),
                singletonMap("default", new PreBuiltAnalyzerProvider("default", AnalyzerScope.INDEX, analyzer)),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap()
            )
        );
        assertEquals("analyzer [default] contains filters [my_filter] that are not allowed to run in all mode.", ex.getMessage());
    }

    public void testNameClashNormalizer() throws IOException {

        // Test out-of-the-box normalizer works OK.
        IndexAnalyzers indexAnalyzers = nonEmptyRegistry.build(IndexSettingsModule.newIndexSettings("index", Settings.EMPTY));
        assertNotNull(indexAnalyzers.getNormalizer("lowercase"));
        assertThat(indexAnalyzers.getNormalizer("lowercase").normalize("field", "AbC").utf8ToString(), equalTo("abc"));

        // Test that a name clash with a custom normalizer will favour the index's normalizer rather than the out-of-the-box
        // one of the same name. (However this "feature" will be removed with https://github.com/elastic/elasticsearch/issues/22263 )
        Settings settings = Settings.builder()
            // Deliberately bad choice of normalizer name for the job it does.
            .put("index.analysis.normalizer.lowercase.type", "custom")
            .putList("index.analysis.normalizer.lowercase.filter", "reverse")
            .build();

        indexAnalyzers = nonEmptyRegistry.build(IndexSettingsModule.newIndexSettings("index", settings));
        assertNotNull(indexAnalyzers.getNormalizer("lowercase"));
        assertThat(indexAnalyzers.getNormalizer("lowercase").normalize("field", "AbC").utf8ToString(), equalTo("CbA"));
    }

    public void testOverrideDefaultIndexAnalyzerIsUnsupported() {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        AnalyzerProvider<?> defaultIndex = new PreBuiltAnalyzerProvider("default_index", AnalyzerScope.INDEX, new EnglishAnalyzer());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> emptyRegistry.build(
                IndexSettingsModule.newIndexSettings("index", settings),
                singletonMap("default_index", defaultIndex),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap()
            )
        );
        assertTrue(e.getMessage().contains("[index.analysis.analyzer.default_index] is not supported"));
    }

    public void testOverrideDefaultSearchAnalyzer() {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexAnalyzers indexAnalyzers = emptyRegistry.build(
            IndexSettingsModule.newIndexSettings("index", settings),
            singletonMap("default_search", analyzerProvider("default_search")),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        assertThat(indexAnalyzers.getDefaultIndexAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    /**
     * Tests that {@code camelCase} filter names and {@code snake_case} filter names don't collide.
     */
    public void testConfigureCamelCaseTokenFilter() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.testFilter.type", "mock")
            .put("index.analysis.filter.test_filter.type", "mock")
            .put("index.analysis.analyzer.custom_analyzer_with_camel_case.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer_with_camel_case.filter", "lowercase", "testFilter")
            .put("index.analysis.analyzer.custom_analyzer_with_snake_case.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer_with_snake_case.filter", "lowercase", "test_filter")
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        /* The snake_case version of the name should not filter out any stopwords while the
         * camelCase version will filter out English stopwords. */
        AnalysisPlugin plugin = new AnalysisPlugin() {
            class MockFactory extends AbstractTokenFilterFactory {
                MockFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    if (name().equals("test_filter")) {
                        return new MockTokenFilter(tokenStream, MockTokenFilter.EMPTY_STOPSET);
                    }
                    return new MockTokenFilter(tokenStream, MockTokenFilter.ENGLISH_STOPSET);
                }
            }

            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                return singletonMap("mock", MockFactory::new);
            }
        };
        IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(plugin))
            .getAnalysisRegistry()
            .build(idxSettings);

        // This shouldn't contain English stopwords
        try (NamedAnalyzer custom_analyser = indexAnalyzers.get("custom_analyzer_with_camel_case")) {
            assertNotNull(custom_analyser);
            TokenStream tokenStream = custom_analyser.tokenStream("foo", "has a foo");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            assertTrue(tokenStream.incrementToken());
            assertEquals("has", charTermAttribute.toString());
            assertTrue(tokenStream.incrementToken());
            assertEquals("foo", charTermAttribute.toString());
            assertFalse(tokenStream.incrementToken());
        }

        // This *should* contain English stopwords
        try (NamedAnalyzer custom_analyser = indexAnalyzers.get("custom_analyzer_with_snake_case")) {
            assertNotNull(custom_analyser);
            TokenStream tokenStream = custom_analyser.tokenStream("foo", "has a foo");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            assertTrue(tokenStream.incrementToken());
            assertEquals("has", charTermAttribute.toString());
            assertTrue(tokenStream.incrementToken());
            assertEquals("a", charTermAttribute.toString());
            assertTrue(tokenStream.incrementToken());
            assertEquals("foo", charTermAttribute.toString());
            assertFalse(tokenStream.incrementToken());
        }
    }

    public void testBuiltInAnalyzersAreCached() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        IndexAnalyzers indexAnalyzers = emptyAnalysisRegistry(settings).build(idxSettings);
        IndexAnalyzers otherIndexAnalyzers = emptyAnalysisRegistry(settings).build(idxSettings);
        final int numIters = randomIntBetween(5, 20);
        for (int i = 0; i < numIters; i++) {
            PreBuiltAnalyzers preBuiltAnalyzers = RandomPicks.randomFrom(random(), PreBuiltAnalyzers.values());
            assertSame(indexAnalyzers.get(preBuiltAnalyzers.name()), otherIndexAnalyzers.get(preBuiltAnalyzers.name()));
        }
    }

    public void testNoTypeOrTokenizerErrorMessage() throws IOException {
        Version version = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .putList("index.analysis.analyzer.test_analyzer.filter", new String[] { "lowercase", "stop", "shingle" })
            .putList("index.analysis.analyzer.test_analyzer.char_filter", new String[] { "html_strip" })
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> emptyAnalysisRegistry(settings).build(idxSettings));
        assertThat(e.getMessage(), equalTo("analyzer [test_analyzer] must specify either an analyzer type, or a tokenizer"));
    }

    public void testCloseIndexAnalyzersMultipleTimes() throws IOException {
        IndexAnalyzers indexAnalyzers = emptyRegistry.build(indexSettingsOfCurrentVersion(Settings.builder()));
        indexAnalyzers.close();
        indexAnalyzers.close();
    }

    public void testEnsureCloseInvocationProperlyDelegated() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        PreBuiltAnalyzerProviderFactory mock = mock(PreBuiltAnalyzerProviderFactory.class);
        AnalysisRegistry registry = new AnalysisRegistry(
            TestEnvironment.newEnvironment(settings),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            Collections.singletonMap("key", mock)
        );

        registry.close();
        verify(mock).close();
    }

    public void testDeprecationsAndExceptions() throws IOException {

        AnalysisPlugin plugin = new AnalysisPlugin() {

            class MockFactory extends AbstractTokenFilterFactory {
                MockFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    if (indexSettings.getIndexVersionCreated().equals(Version.CURRENT)) {
                        deprecationLogger.deprecate("deprecated_token_filter", "Using deprecated token filter [deprecated]");
                    }
                    return tokenStream;
                }
            }

            class ExceptionFactory extends AbstractTokenFilterFactory {

                ExceptionFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    if (indexSettings.getIndexVersionCreated().equals(Version.CURRENT)) {
                        throw new IllegalArgumentException("Cannot use token filter [exception]");
                    }
                    return tokenStream;
                }
            }

            class UnusedMockFactory extends AbstractTokenFilterFactory {
                UnusedMockFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    deprecationLogger.deprecate("unused_token_filter", "Using deprecated token filter [unused]");
                    return tokenStream;
                }
            }

            class NormalizerFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

                NormalizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    deprecationLogger.deprecate("deprecated_normalizer", "Using deprecated token filter [deprecated_normalizer]");
                    return tokenStream;
                }

            }

            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                Map<String, AnalysisProvider<TokenFilterFactory>> filters = new HashMap<>();
                filters.put("deprecated", MockFactory::new);
                filters.put("unused", UnusedMockFactory::new);
                filters.put("deprecated_normalizer", NormalizerFactory::new);
                filters.put("exception", ExceptionFactory::new);
                return filters;
            }
        };

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.deprecated.type", "deprecated")
            .put("index.analysis.analyzer.custom.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom.filter", "lowercase", "deprecated")
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(plugin)).getAnalysisRegistry().build(idxSettings);

        // We should only get a warning from the token filter that is referenced in settings
        assertWarnings("Using deprecated token filter [deprecated]");

        indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.getPreviousVersion())
            .put("index.analysis.filter.deprecated.type", "deprecated_normalizer")
            .putList("index.analysis.normalizer.custom.filter", "lowercase", "deprecated_normalizer")
            .put("index.analysis.filter.deprecated.type", "deprecated")
            .put("index.analysis.filter.exception.type", "exception")
            .put("index.analysis.analyzer.custom.tokenizer", "standard")
            // exception will not throw because we're not on Version.CURRENT
            .putList("index.analysis.analyzer.custom.filter", "lowercase", "deprecated", "exception")
            .build();
        idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(plugin)).getAnalysisRegistry().build(idxSettings);

        // We should only get a warning from the normalizer, because we're on a version where 'deprecated'
        // works fine
        assertWarnings("Using deprecated token filter [deprecated_normalizer]");

        indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.exception.type", "exception")
            .put("index.analysis.analyzer.custom.tokenizer", "standard")
            // exception will not throw because we're not on Version.LATEST
            .putList("index.analysis.analyzer.custom.filter", "lowercase", "exception")
            .build();
        IndexSettings exceptionSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new AnalysisModule(TestEnvironment.newEnvironment(settings), singletonList(plugin)).getAnalysisRegistry()
                .build(exceptionSettings);
        });

        boolean found = Arrays.stream(e.getSuppressed())
            .map(org.opensearch.ExceptionsHelper::unwrapCause)
            .map(Throwable::getMessage)
            .findFirst()
            .get()
            .contains("Cannot use token filter [exception]");
        assertTrue(found);

    }

    public void testAggregatesAnalyzerBuildFailuresAndContinuesRegistrationLoop() {
        // Build settings with two broken analyzers (different failure modes)
        // and one valid analyzer to prove we keep iterating past the first failure.
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)

            // bad1: unknown token filter name -> will fail when producing filter factory
            .put("index.analysis.analyzer.bad1.type", "custom")
            .put("index.analysis.analyzer.bad1.tokenizer", "standard")
            .putList("index.analysis.analyzer.bad1.filter", "lowercase", "does_not_exist_filter")

            // bad2: unknown tokenizer -> will fail when producing tokenizer factory
            .put("index.analysis.analyzer.bad2.type", "custom")
            .put("index.analysis.analyzer.bad2.tokenizer", "does_not_exist_tokenizer")
            .putList("index.analysis.analyzer.bad2.filter", "lowercase")

            // good: valid analyzer we expect to be *attempted* after bad1
            .put("index.analysis.analyzer.no_split_synonym_analyzer.type", "custom")
            .put("index.analysis.analyzer.no_split_synonym_analyzer.tokenizer", "standard")
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AnalysisModule(TestEnvironment.newEnvironment(settings), Collections.emptyList()).getAnalysisRegistry()
                .build(idxSettings)
        );

        // After the fix, we expect a single aggregated exception that mentions both failing analyzers
        assertThat(e.getMessage(), containsString("bad1"));
        assertThat(e.getMessage(), containsString("bad2"));

        // And we expect two suppressed causes (one per bad analyzer)
        Throwable[] suppressed = e.getSuppressed();
        assertNotNull(suppressed);
        assertEquals(2, suppressed.length);

        // Sanity: each suppressed cause should carry a helpful message
        assertThat(suppressed[0].getMessage(), containsString("bad"));
        assertThat(suppressed[1].getMessage(), containsString("bad"));
    }
}

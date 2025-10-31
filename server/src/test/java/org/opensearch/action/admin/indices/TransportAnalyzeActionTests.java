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

package org.opensearch.action.admin.indices;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.tests.analysis.MockTokenFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.analyze.TransportAnalyzeAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractCharFilterFactory;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NormalizingTokenFilterFactory;
import org.opensearch.index.analysis.PreConfiguredCharFilter;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.indices.analysis.AnalysisModuleTests.AppendCharFilter;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TransportAnalyzeAction}. See the rest tests in the {@code analysis-common} module for places where this code gets a ton
 * more exercise.
 */
public class TransportAnalyzeActionTests extends OpenSearchTestCase {

    private IndexAnalyzers indexAnalyzers;
    private IndexSettings indexSettings;
    private AnalysisRegistry registry;
    private int maxTokenCount;
    private int idxMaxTokenCount;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put("index.analysis.analyzer.custom_analyzer.tokenizer", "standard")
            .put("index.analysis.analyzer.custom_analyzer.filter", "mock")
            .put("index.analysis.normalizer.my_normalizer.type", "custom")
            .put("index.analysis.char_filter.my_append.type", "append")
            .put("index.analysis.char_filter.my_append.suffix", "baz")
            .put("index.analyze.max_token_count", 100)
            .putList("index.analysis.normalizer.my_normalizer.filter", "lowercase")
            .build();
        this.indexSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        Environment environment = TestEnvironment.newEnvironment(settings);
        AnalysisPlugin plugin = new AnalysisPlugin() {
            class MockFactory extends AbstractTokenFilterFactory {

                final CharacterRunAutomaton stopset;

                MockFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                    if (settings.hasValue("stopword")) {
                        this.stopset = new CharacterRunAutomaton(Automata.makeString(settings.get("stopword")));
                    } else {
                        this.stopset = MockTokenFilter.ENGLISH_STOPSET;
                    }
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return new MockTokenFilter(tokenStream, this.stopset);
                }
            }

            class DeprecatedTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

                DeprecatedTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(indexSettings, name, settings);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    deprecationLogger.deprecate("deprecated_token_filter_create", "Using deprecated token filter [deprecated]");
                    return tokenStream;
                }

                @Override
                public TokenStream normalize(TokenStream tokenStream) {
                    deprecationLogger.deprecate("deprecated_token_filter_normalize", "Using deprecated token filter [deprecated]");
                    return tokenStream;
                }
            }

            class AppendCharFilterFactory extends AbstractCharFilterFactory {

                final String suffix;

                AppendCharFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
                    super(indexSettings, name);
                    this.suffix = settings.get("suffix", "bar");
                }

                @Override
                public Reader create(Reader reader) {
                    return new AppendCharFilter(reader, suffix);
                }
            }

            @Override
            public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
                return singletonMap("append", AppendCharFilterFactory::new);
            }

            @Override
            public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
                return singletonMap(
                    "keyword",
                    (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                        name,
                        () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                    )
                );
            }

            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                Map<String, AnalysisProvider<TokenFilterFactory>> filters = new HashMap<>();
                filters.put("mock", MockFactory::new);
                filters.put("deprecated", DeprecatedTokenFilterFactory::new);
                return filters;
            }

            @Override
            public List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
                return singletonList(PreConfiguredCharFilter.singleton("append", false, reader -> new AppendCharFilter(reader, "foo")));
            }
        };
        registry = new AnalysisModule(environment, singletonList(plugin)).getAnalysisRegistry();
        indexAnalyzers = registry.build(this.indexSettings);
        maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.getDefault(settings);
        idxMaxTokenCount = this.indexSettings.getMaxTokenCount();
    }

    private IndexService mockIndexService() {
        IndexService is = mock(IndexService.class);
        when(is.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        when(is.getIndexSettings()).thenReturn(indexSettings);
        return is;
    }

    /**
     * Test behavior when the named analysis component isn't defined on the index. In that case we should build with defaults.
     */
    public void testNoIndexAnalyzers() throws IOException {
        // Refer to an analyzer by its type so we get its default configuration
        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.text("the quick brown fox");
        request.analyzer("standard");
        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(4, tokens.size());

        // We can refer to a pre-configured token filter by its name to get it
        request = new AnalyzeAction.Request();
        request.text("the qu1ck brown fox");
        request.tokenizer("standard");
        request.addCharFilter("append");        // <-- no config, so use preconfigured filter
        analyze = TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(4, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("qu1ck", tokens.get(1).getTerm());
        assertEquals("brown", tokens.get(2).getTerm());
        assertEquals("foxfoo", tokens.get(3).getTerm());

        // If the preconfigured filter doesn't exist, we use a global filter with no settings
        request = new AnalyzeAction.Request();
        request.text("the qu1ck brown fox");
        request.tokenizer("standard");
        request.addTokenFilter("mock");     // <-- not preconfigured, but a global one available
        analyze = TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("qu1ck", tokens.get(0).getTerm());
        assertEquals("brown", tokens.get(1).getTerm());
        assertEquals("fox", tokens.get(2).getTerm());

        Map<String, Object> tokenFilterConfig = new HashMap<>();
        tokenFilterConfig.put("type", "mock");
        tokenFilterConfig.put("stopword", "brown");

        Map<String, Object> charFilterConfig = new HashMap<>();
        charFilterConfig.put("type", "append");

        // We can build a new char filter to get default values
        request = new AnalyzeAction.Request();
        request.text("the qu1ck brown fox");
        request.tokenizer("standard");
        request.addTokenFilter(tokenFilterConfig);
        request.addCharFilter(charFilterConfig);    // <-- basic config, uses defaults
        analyze = TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("qu1ck", tokens.get(1).getTerm());
        assertEquals("foxbar", tokens.get(2).getTerm());

        // We can pass a new configuration
        request = new AnalyzeAction.Request();
        request.text("the qu1ck brown fox");
        request.tokenizer("standard");
        request.addTokenFilter(tokenFilterConfig);
        charFilterConfig.put("suffix", "baz");
        request.addCharFilter(charFilterConfig);
        analyze = TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("qu1ck", tokens.get(1).getTerm());
        assertEquals("foxbaz", tokens.get(2).getTerm());
    }

    public void testFillsAttributes() throws IOException {
        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.analyzer("standard");
        request.text("the 1 brown fox");
        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(4, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals(0, tokens.get(0).getStartOffset());
        assertEquals(3, tokens.get(0).getEndOffset());
        assertEquals(0, tokens.get(0).getPosition());
        assertEquals("<ALPHANUM>", tokens.get(0).getType());

        assertEquals("1", tokens.get(1).getTerm());
        assertEquals(4, tokens.get(1).getStartOffset());
        assertEquals(5, tokens.get(1).getEndOffset());
        assertEquals(1, tokens.get(1).getPosition());
        assertEquals("<NUM>", tokens.get(1).getType());

        assertEquals("brown", tokens.get(2).getTerm());
        assertEquals(6, tokens.get(2).getStartOffset());
        assertEquals(11, tokens.get(2).getEndOffset());
        assertEquals(2, tokens.get(2).getPosition());
        assertEquals("<ALPHANUM>", tokens.get(2).getType());

        assertEquals("fox", tokens.get(3).getTerm());
        assertEquals(12, tokens.get(3).getStartOffset());
        assertEquals(15, tokens.get(3).getEndOffset());
        assertEquals(3, tokens.get(3).getPosition());
        assertEquals("<ALPHANUM>", tokens.get(3).getType());
    }

    public void testWithIndexAnalyzers() throws IOException {
        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.text("the quick brown fox");
        request.analyzer("custom_analyzer");
        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("quick", tokens.get(0).getTerm());
        assertEquals("brown", tokens.get(1).getTerm());
        assertEquals("fox", tokens.get(2).getTerm());

        request.analyzer("standard");
        analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(4, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("quick", tokens.get(1).getTerm());
        assertEquals("brown", tokens.get(2).getTerm());
        assertEquals("fox", tokens.get(3).getTerm());

        // Switch the analyzer out for just a tokenizer
        request.analyzer(null);
        request.tokenizer("standard");
        analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(4, tokens.size());
        assertEquals("the", tokens.get(0).getTerm());
        assertEquals("quick", tokens.get(1).getTerm());
        assertEquals("brown", tokens.get(2).getTerm());
        assertEquals("fox", tokens.get(3).getTerm());

        // Now try applying our token filter
        request.addTokenFilter("mock");
        analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("quick", tokens.get(0).getTerm());
        assertEquals("brown", tokens.get(1).getTerm());
        assertEquals("fox", tokens.get(2).getTerm());

        // Apply the char filter, checking that the correct configuration gets passed on
        request.addCharFilter("my_append");
        analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("quick", tokens.get(0).getTerm());
        assertEquals("brown", tokens.get(1).getTerm());
        assertEquals("foxbaz", tokens.get(2).getTerm());

        // Apply a token filter with parameters
        Map<String, Object> tokenFilterConfig = new HashMap<>();
        tokenFilterConfig.put("type", "mock");
        tokenFilterConfig.put("stopword", "brown");
        request.addTokenFilter(tokenFilterConfig);
        analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        tokens = analyze.getTokens();
        assertEquals(2, tokens.size());
        assertEquals("quick", tokens.get(0).getTerm());
        assertEquals("foxbaz", tokens.get(1).getTerm());

    }

    public void testGetIndexAnalyserWithoutIndexAnalyzers() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeAction.Request().analyzer("custom_analyzer").text("the qu1ck brown fox-dog"),
                registry,
                null,
                maxTokenCount
            )
        );
        assertEquals(e.getMessage(), "failed to find global analyzer [custom_analyzer]");
    }

    public void testGetFieldAnalyzerWithoutIndexAnalyzers() {
        AnalyzeAction.Request req = new AnalyzeAction.Request().field("field").text("text");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            TransportAnalyzeAction.analyze(req, registry, null, maxTokenCount);
        });
        assertEquals(e.getMessage(), "analysis based on a specific field requires an index");
    }

    public void testUnknown() {
        boolean notGlobal = randomBoolean();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeAction.Request().analyzer("foobar").text("the qu1ck brown fox"),
                registry,
                notGlobal ? mockIndexService() : null,
                maxTokenCount
            )
        );
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find analyzer [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global analyzer [foobar]");
        }

        e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeAction.Request().tokenizer("foobar").text("the qu1ck brown fox"),
                registry,
                notGlobal ? mockIndexService() : null,
                maxTokenCount
            )
        );
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find tokenizer under [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global tokenizer under [foobar]");
        }

        e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeAction.Request().tokenizer("standard").addTokenFilter("foobar").text("the qu1ck brown fox"),
                registry,
                notGlobal ? mockIndexService() : null,
                maxTokenCount
            )
        );
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find filter under [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global filter under [foobar]");
        }

        e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeAction.Request().tokenizer("standard")
                    .addTokenFilter("lowercase")
                    .addCharFilter("foobar")
                    .text("the qu1ck brown fox"),
                registry,
                notGlobal ? mockIndexService() : null,
                maxTokenCount
            )
        );
        if (notGlobal) {
            assertEquals(e.getMessage(), "failed to find char_filter under [foobar]");
        } else {
            assertEquals(e.getMessage(), "failed to find global char_filter under [foobar]");
        }

        e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportAnalyzeAction.analyze(
                new AnalyzeAction.Request().normalizer("foobar").text("the qu1ck brown fox"),
                registry,
                mockIndexService(),
                maxTokenCount
            )
        );
        assertEquals(e.getMessage(), "failed to find normalizer under [foobar]");
    }

    public void testNonPreBuildTokenFilter() throws IOException {
        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.tokenizer("standard");
        request.addTokenFilter("stop"); // stop token filter is not prebuilt in AnalysisModule#setupPreConfiguredTokenFilters()
        request.text("the quick brown fox");
        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(3, tokens.size());
        assertEquals("quick", tokens.get(0).getTerm());
        assertEquals("brown", tokens.get(1).getTerm());
        assertEquals("fox", tokens.get(2).getTerm());
    }

    public void testCustomCharFilterWithParameters() throws IOException {
        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.tokenizer("standard");
        Map<String, Object> charFilterConfig = new HashMap<>();
        charFilterConfig.put("type", "append");
        charFilterConfig.put("suffix", "foo");
        request.addCharFilter(charFilterConfig);
        request.text("quick brown");
        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = analyze.getTokens();
        assertEquals(2, tokens.size());
        assertEquals("quick", tokens.get(0).getTerm());
        assertEquals("brownfoo", tokens.get(1).getTerm());
    }

    public void testNormalizerWithIndex() throws IOException {
        AnalyzeAction.Request request = new AnalyzeAction.Request("index");
        request.normalizer("my_normalizer");
        // this should be lowercased and only emit a single token
        request.text("Wi-fi");
        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(request, registry, mockIndexService(), maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = analyze.getTokens();

        assertEquals(1, tokens.size());
        assertEquals("wi-fi", tokens.get(0).getTerm());
    }

    /**
     * This test is equivalent of calling _analyze without a specific index.
     * The default value for the maximum token count is used.
     */
    public void testExceedDefaultMaxTokenLimit() {
        // create a string with No. words more than maxTokenCount
        StringBuilder sbText = new StringBuilder();
        for (int i = 0; i <= maxTokenCount; i++) {
            sbText.append('a');
            sbText.append(' ');
        }
        String text = sbText.toString();

        // request with explain=false to test simpleAnalyze path in TransportAnalyzeAction
        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.text(text);
        request.analyzer("standard");
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TransportAnalyzeAction.analyze(request, registry, null, maxTokenCount)
        );
        assertEquals(
            e.getMessage(),
            "The number of tokens produced by calling _analyze has exceeded the allowed maximum of ["
                + maxTokenCount
                + "]."
                + " This limit can be set by changing the [index.analyze.max_token_count] index level setting."
        );

        // request with explain=true to test detailAnalyze path in TransportAnalyzeAction
        AnalyzeAction.Request request2 = new AnalyzeAction.Request();
        request2.text(text);
        request2.analyzer("standard");
        request2.explain(true);
        IllegalStateException e2 = expectThrows(
            IllegalStateException.class,
            () -> TransportAnalyzeAction.analyze(request2, registry, null, maxTokenCount)
        );
        assertEquals(
            e2.getMessage(),
            "The number of tokens produced by calling _analyze has exceeded the allowed maximum of ["
                + maxTokenCount
                + "]."
                + " This limit can be set by changing the [index.analyze.max_token_count] index level setting."
        );
    }

    /**
     * This test is equivalent of calling _analyze against a specific index.
     * The index specific value for the maximum token count is used.
     */
    public void testExceedSetMaxTokenLimit() {
        // create a string with No. words more than idxMaxTokenCount
        StringBuilder sbText = new StringBuilder();
        for (int i = 0; i <= idxMaxTokenCount; i++) {
            sbText.append('a');
            sbText.append(' ');
        }
        String text = sbText.toString();

        AnalyzeAction.Request request = new AnalyzeAction.Request();
        request.text(text);
        request.analyzer("standard");
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> TransportAnalyzeAction.analyze(request, registry, null, idxMaxTokenCount)
        );
        assertEquals(
            e.getMessage(),
            "The number of tokens produced by calling _analyze has exceeded the allowed maximum of ["
                + idxMaxTokenCount
                + "]."
                + " This limit can be set by changing the [index.analyze.max_token_count] index level setting."
        );
    }

    public void testDeprecationWarnings() throws IOException {
        AnalyzeAction.Request req = new AnalyzeAction.Request();
        req.tokenizer("standard");
        req.addTokenFilter("lowercase");
        req.addTokenFilter("deprecated");
        req.text("test text");

        AnalyzeAction.Response analyze = TransportAnalyzeAction.analyze(req, registry, mockIndexService(), maxTokenCount);
        assertEquals(2, analyze.getTokens().size());
        assertWarnings("Using deprecated token filter [deprecated]");

        // normalizer
        req = new AnalyzeAction.Request();
        req.addTokenFilter("lowercase");
        req.addTokenFilter("deprecated");
        req.text("text");

        analyze = TransportAnalyzeAction.analyze(req, registry, mockIndexService(), maxTokenCount);
        assertEquals(1, analyze.getTokens().size());
    }

    public void testResolveIndicesForNoIndex() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext);

        TransportAnalyzeAction action = new TransportAnalyzeAction(
            Settings.EMPTY,
            mock(ThreadPool.class),
            mock(ClusterService.class),
            mock(TransportService.class),
            mock(IndicesService.class),
            mock(ActionFilters.class),
            indexNameExpressionResolver
        );

        AnalyzeAction.Request request = new AnalyzeAction.Request().text("the quick brown fox");
        assertEquals(ResolvedIndices.of(ResolvedIndices.Local.of()), action.resolveIndices(request));
    }

    public void testResolveIndicesForIndex() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext);
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(
                IndexMetadata.builder("test_index")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
            );

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        TransportAnalyzeAction action = new TransportAnalyzeAction(
            Settings.EMPTY,
            mock(ThreadPool.class),
            clusterService,
            mock(TransportService.class),
            mock(IndicesService.class),
            mock(ActionFilters.class),
            indexNameExpressionResolver
        );

        AnalyzeAction.Request request = new AnalyzeAction.Request("test_index").text("the quick brown fox");
        assertEquals(ResolvedIndices.of(ResolvedIndices.Local.of("test_index")), action.resolveIndices(request));
    }
}

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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.MyFilterTokenFilterFactory;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;

public class CompoundAnalysisTests extends OpenSearchTestCase {

    Settings[] settingsArr;

    @Before
    public void initialize() throws IOException {
        final Path home = createTempDir();
        copyHyphenationPatternsFile(home);
        this.settingsArr = new Settings[] { getJsonSettings(home), getYamlSettings(home) };
    }

    public void testDefaultsCompoundAnalysis() throws Exception {
        for (Settings settings : this.settingsArr) {
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
            AnalysisModule analysisModule = createAnalysisModule(settings);
            TokenFilterFactory filterFactory = analysisModule.getAnalysisRegistry().buildTokenFilterFactories(idxSettings).get("dict_dec");
            MatcherAssert.assertThat(filterFactory, instanceOf(DictionaryCompoundWordTokenFilterFactory.class));
        }
    }

    public void testDictionaryDecompounder() throws Exception {
        for (Settings settings : this.settingsArr) {
            List<String> terms = analyze(settings, "decompoundingAnalyzer", "donaudampfschiff spargelcremesuppe");
            MatcherAssert.assertThat(terms.size(), equalTo(8));
            MatcherAssert.assertThat(
                terms,
                hasItems("donau", "dampf", "schiff", "donaudampfschiff", "spargel", "creme", "suppe", "spargelcremesuppe")
            );
        }
    }

    // Hyphenation Decompounder tests mimic the behavior of lucene tests
    // lucene/analysis/common/src/test/org/apache/lucene/analysis/compound/TestHyphenationCompoundWordTokenFilterFactory.java
    public void testHyphenationDecompounder() throws Exception {
        for (Settings settings : this.settingsArr) {
            List<String> terms = analyze(settings, "hyphenationAnalyzer", "min veninde som er lidt af en læsehest");
            MatcherAssert.assertThat(terms.size(), equalTo(10));
            MatcherAssert.assertThat(terms, hasItems("min", "veninde", "som", "er", "lidt", "af", "en", "læsehest", "læse", "hest"));
        }
    }

    // Hyphenation Decompounder tests mimic the behavior of lucene tests
    // lucene/analysis/common/src/test/org/apache/lucene/analysis/compound/TestHyphenationCompoundWordTokenFilterFactory.java
    public void testHyphenationDecompounderNoSubMatches() throws Exception {
        for (Settings settings : this.settingsArr) {
            List<String> terms = analyze(settings, "hyphenationAnalyzerNoSubMatches", "basketballkurv");
            MatcherAssert.assertThat(terms.size(), equalTo(3));
            MatcherAssert.assertThat(terms, hasItems("basketballkurv", "basketball", "kurv"));
        }
    }

    private List<String> analyze(Settings settings, String analyzerName, String text) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisModule analysisModule = createAnalysisModule(settings);
        IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(idxSettings);
        Analyzer analyzer = indexAnalyzers.get(analyzerName).analyzer();

        TokenStream stream = analyzer.tokenStream("", text);
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

        List<String> terms = new ArrayList<>();
        while (stream.incrementToken()) {
            String tokText = termAtt.toString();
            terms.add(tokText);
        }
        return terms;
    }

    private AnalysisModule createAnalysisModule(Settings settings) throws IOException {
        CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin();
        return new AnalysisModule(TestEnvironment.newEnvironment(settings), Arrays.asList(commonAnalysisPlugin, new AnalysisPlugin() {
            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                return singletonMap("myfilter", MyFilterTokenFilterFactory::new);
            }
        }));
    }

    private void copyHyphenationPatternsFile(Path home) throws IOException {
        InputStream hyphenation_patterns_path = getClass().getResourceAsStream("da_UTF8.xml");
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(hyphenation_patterns_path, config.resolve("da_UTF8.xml"));
    }

    private Settings getJsonSettings(Path home) throws IOException {
        String json = "/org/opensearch/analysis/common/test1.json";
        return Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), home.toString())
            .build();
    }

    private Settings getYamlSettings(Path home) throws IOException {
        String yaml = "/org/opensearch/analysis/common/test1.yml";
        return Settings.builder()
            .loadFromStream(yaml, getClass().getResourceAsStream(yaml), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), home.toString())
            .build();
    }
}

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
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.test.OpenSearchTokenStreamTestCase;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class EdgeNGramTokenizerTests extends OpenSearchTokenStreamTestCase {

    private IndexAnalyzers buildAnalyzers(Version version, String tokenizer) throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put("index.analysis.analyzer.my_analyzer.tokenizer", tokenizer)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        return new AnalysisModule(TestEnvironment.newEnvironment(settings), Collections.singletonList(new CommonAnalysisModulePlugin()))
            .getAnalysisRegistry()
            .build(idxSettings);
    }

    public void testPreConfiguredTokenizer() throws IOException {

        // Afterwards, we return ngrams of length 1 and 2, to match the default factory settings
        {
            try (IndexAnalyzers indexAnalyzers = buildAnalyzers(Version.CURRENT, "edge_ngram")) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t", "te" });
            }
        }

        // Check deprecated name as well, needs version before 3.0 because throws IAE after that
        {
            try (
                IndexAnalyzers indexAnalyzers = buildAnalyzers(
                    VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, VersionUtils.getPreviousVersion(Version.V_3_0_0)),
                    "edgeNGram"
                )
            ) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertAnalyzesTo(analyzer, "test", new String[] { "t", "te" });
            }
        }

        // Check IAE from 3.0 onward
        {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> buildAnalyzers(VersionUtils.randomVersionBetween(random(), Version.V_3_0_0, Version.CURRENT), "edgeNGram")
            );
            assertThat(e, hasToString(containsString("The [edgeNGram] tokenizer name was deprecated pre 1.0.")));
        }
    }

    public void testCustomTokenChars() throws IOException {
        final Index index = new Index("test", "_na_");
        final String name = "engr";
        final Settings indexSettings = newAnalysisSettingsBuilder().put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 2).build();

        final Settings settings = newAnalysisSettingsBuilder().put("min_gram", 2)
            .put("max_gram", 3)
            .putList("token_chars", "letter", "custom")
            .put("custom_token_chars", "_-")
            .build();
        Tokenizer tokenizer = new EdgeNGramTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            name,
            settings
        ).create();
        tokenizer.setReader(new StringReader("Abc -gh _jk =lm"));
        assertTokenStreamContents(tokenizer, new String[] { "Ab", "Abc", "-g", "-gh", "_j", "_jk", "lm" });
    }

}

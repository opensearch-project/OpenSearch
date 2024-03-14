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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTokenStreamTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;

public class PathHierarchyTokenizerFactoryTests extends OpenSearchTokenStreamTestCase {

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

    /**
     * Test that deprecated "PathHierarchy" tokenizer name is still available via {@link CommonAnalysisModulePlugin} starting in 3.x.
     */
    public void testPreConfiguredTokenizer() throws IOException {

        {
            try (
                IndexAnalyzers indexAnalyzers = buildAnalyzers(
                    VersionUtils.randomVersionBetween(random(), Version.V_3_0_0, Version.CURRENT),
                    "PathHierarchy"
                )
            ) {
                NamedAnalyzer analyzer = indexAnalyzers.get("my_analyzer");
                assertNotNull(analyzer);
                assertTokenStreamContents(analyzer.tokenStream("dummy", "/a/b/c"), new String[] { "/a", "/a/b", "/a/b/c" });
                // Once LUCENE-12750 is fixed we can use the following testing method instead.
                // Similar testing approach has been used for deprecation of (Edge)NGrams tokenizers as well.
                // assertAnalyzesTo(analyzer, "/a/b/c", new String[] { "/a", "/a/b", "/a/b/c" });

            }
        }
    }

    public void testDefaults() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            Settings.EMPTY
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "/one", "/one/two", "/one/two/three" });
    }

    public void testReverse() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("reverse", true).build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "/one/two/three", "one/two/three", "two/three", "three" });
    }

    public void testDelimiter() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("delimiter", "-").build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "/one/two/three" });
        tokenizer.setReader(new StringReader("one-two-three"));
        assertTokenStreamContents(tokenizer, new String[] { "one", "one-two", "one-two-three" });
    }

    public void testReplace() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("replacement", "-").build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three"));
        assertTokenStreamContents(tokenizer, new String[] { "-one", "-one-two", "-one-two-three" });
        tokenizer.setReader(new StringReader("one-two-three"));
        assertTokenStreamContents(tokenizer, new String[] { "one-two-three" });
    }

    public void testSkip() throws IOException {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        Settings settings = newAnalysisSettingsBuilder().put("skip", 2).build();
        Tokenizer tokenizer = new PathHierarchyTokenizerFactory(
            IndexSettingsModule.newIndexSettings(index, indexSettings),
            null,
            "path-hierarchy-tokenizer",
            settings
        ).create();
        tokenizer.setReader(new StringReader("/one/two/three/four/five"));
        assertTokenStreamContents(tokenizer, new String[] { "/three", "/three/four", "/three/four/five" });
    }

    public void testDelimiterExceptions() {
        final Index index = new Index("test", "_na_");
        final Settings indexSettings = newAnalysisSettingsBuilder().build();
        {
            String delimiter = RandomPicks.randomFrom(random(), new String[] { "--", "" });
            Settings settings = newAnalysisSettingsBuilder().put("delimiter", delimiter).build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new PathHierarchyTokenizerFactory(
                    IndexSettingsModule.newIndexSettings(index, indexSettings),
                    null,
                    "path-hierarchy-tokenizer",
                    settings
                ).create()
            );
            assertEquals("delimiter must be a one char value", e.getMessage());
        }
        {
            String replacement = RandomPicks.randomFrom(random(), new String[] { "--", "" });
            Settings settings = newAnalysisSettingsBuilder().put("replacement", replacement).build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new PathHierarchyTokenizerFactory(
                    IndexSettingsModule.newIndexSettings(index, indexSettings),
                    null,
                    "path-hierarchy-tokenizer",
                    settings
                ).create()
            );
            assertEquals("replacement must be a one char value", e.getMessage());
        }
    }
}

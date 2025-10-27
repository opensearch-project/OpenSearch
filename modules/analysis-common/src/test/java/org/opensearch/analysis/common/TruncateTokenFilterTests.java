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

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.IOException;

public class TruncateTokenFilterTests extends OpenSearchTokenStreamTestCase {

    public void testFilter() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.truncate.type", "truncate")
            .put("index.analysis.filter.truncate.length", 3)
            .put("index.analysis.analyzer.my_analyzer.type", "custom")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.my_analyzer.filter", "truncate")
            .putList("index.analysis.normalizer.my_normalizer.filter", "truncate")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            settings,
            new CommonAnalysisModulePlugin()
        );
        NamedAnalyzer analyzer = analysis.indexAnalyzers.get("my_analyzer");
        assertNotNull(analyzer);

        assertTokenStreamContents(analyzer.tokenStream("foo", "a bb ccc dddd"), new String[] { "a", "bb", "ccc", "ddd" });
    }

    public void testNormalizer() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.truncate.type", "truncate")
            .put("index.analysis.filter.truncate.length", 3)
            .put("index.analysis.normalizer.my_normalizer.type", "custom")
            .putList("index.analysis.normalizer.my_normalizer.filter", "truncate")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            settings,
            new CommonAnalysisModulePlugin()
        );
        assertNull(analysis.indexAnalyzers.get("my_normalizer"));
        NamedAnalyzer normalizer = analysis.indexAnalyzers.getNormalizer("my_normalizer");
        assertNotNull(normalizer);
        assertEquals("my_normalizer", normalizer.name());
        assertEquals(new BytesRef("a"), normalizer.normalize("foo", "a"));
        assertEquals(new BytesRef("bb"), normalizer.normalize("foo", "bb"));
        assertEquals(new BytesRef("ccc"), normalizer.normalize("foo", "ccc"));
        assertEquals(new BytesRef("ddd"), normalizer.normalize("foo", "dddd"));
    }
}

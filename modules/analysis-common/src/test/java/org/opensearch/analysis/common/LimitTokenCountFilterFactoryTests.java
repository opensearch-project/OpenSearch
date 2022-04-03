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

public class LimitTokenCountFilterFactoryTests extends OpenSearchTokenStreamTestCase {
    public void testDefault() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.limit_default.type", "limit")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        OpenSearchTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
        {
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_default");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    public void testSettings() throws IOException {
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.limit_1.type", "limit")
                .put("index.analysis.filter.limit_1.max_token_count", 3)
                .put("index.analysis.filter.limit_1.consume_all_tokens", true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            OpenSearchTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.limit_1.type", "limit")
                .put("index.analysis.filter.limit_1.max_token_count", 3)
                .put("index.analysis.filter.limit_1.consume_all_tokens", false)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            OpenSearchTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }

        {
            Settings settings = Settings.builder()
                .put("index.analysis.filter.limit_1.type", "limit")
                .put("index.analysis.filter.limit_1.max_token_count", 17)
                .put("index.analysis.filter.limit_1.consume_all_tokens", true)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
            OpenSearchTestCase.TestAnalysis analysis = createTestAnalysisFromSettings(settings);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("limit_1");
            String source = "the quick brown fox";
            String[] expected = new String[] { "the", "quick", "brown", "fox" };
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader(source));
            assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
        }
    }

    private static OpenSearchTestCase.TestAnalysis createTestAnalysisFromSettings(Settings settings) throws IOException {
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
    }

}

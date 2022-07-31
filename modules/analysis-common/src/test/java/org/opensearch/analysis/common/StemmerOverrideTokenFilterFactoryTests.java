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

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Locale;

public class StemmerOverrideTokenFilterFactoryTests extends OpenSearchTokenStreamTestCase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static TokenFilterFactory create(String... rules) throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put("index.analysis.filter.my_stemmer_override.type", "stemmer_override")
                .putList("index.analysis.filter.my_stemmer_override.rules", rules)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build(),
            new CommonAnalysisModulePlugin()
        );

        return analysis.tokenFilter.get("my_stemmer_override");
    }

    public void testRuleError() {
        for (String rule : Arrays.asList(
            "",        // empty
            "a",       // no arrow
            "a=>b=>c", // multiple arrows
            "=>a=>b",  // multiple arrows
            "a=>",     // no override
            "a=>b,c",  // multiple overrides
            "=>a",     // no keys
            "a,=>b"    // empty key
        )) {
            expectThrows(
                RuntimeException.class,
                String.format(Locale.ROOT, "Should fail for invalid rule: '%s'", rule),
                () -> create(rule)
            );
        }
    }

    public void testRulesOk() throws IOException {
        TokenFilterFactory tokenFilterFactory = create("a => 1", "b,c => 2");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("a b c"));
        assertTokenStreamContents(tokenFilterFactory.create(tokenizer), new String[] { "1", "2", "2" });
    }
}

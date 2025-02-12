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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.StringReader;

public class ShingleTokenFilterTests extends OpenSearchTokenStreamTestCase {
    public void testPreConfiguredShingleFilterDisableGraphAttribute() throws Exception {
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.filter.my_ascii_folding.type", "asciifolding")
                .build(),
            new CommonAnalysisModulePlugin()
        );
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("shingle");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("this is a test"));
        TokenStream tokenStream = tokenFilter.create(tokenizer);
        assertTrue(tokenStream.hasAttribute(DisableGraphAttribute.class));
    }
}

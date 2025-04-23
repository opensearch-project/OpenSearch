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
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.StringReader;

public class CJKFilterFactoryTests extends OpenSearchTokenStreamTestCase {
    private static final String RESOURCE = "/org/opensearch/analysis/common/cjk_analysis.json";

    private OpenSearchTestCase.TestAnalysis analysis;

    @Before
    public void setup() throws IOException {
        analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(createTempDir(), RESOURCE, new CommonAnalysisModulePlugin());
    }

    public void testDefault() throws IOException {
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("cjk_bigram");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[] { "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" };
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testNoFlags() throws IOException {
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("cjk_no_flags");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[] { "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" };
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testHanOnly() throws IOException {
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("cjk_han_only");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[] { "多", "く", "の", "学生", "が", "試験", "に", "落", "ち", "た" };
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testHanUnigramOnly() throws IOException {
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("cjk_han_unigram_only");
        String source = "多くの学生が試験に落ちた。";
        String[] expected = new String[] { "多", "く", "の", "学", "学生", "生", "が", "試", "試験", "験", "に", "落", "ち", "た" };
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        assertTokenStreamContents(tokenFilter.create(tokenizer), expected);
    }

    public void testDisableGraph() throws IOException {
        TokenFilterFactory allFlagsFactory = analysis.tokenFilter.get("cjk_all_flags");
        TokenFilterFactory hanOnlyFactory = analysis.tokenFilter.get("cjk_han_only");

        String source = "多くの学生が試験に落ちた。";
        Tokenizer tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        try (TokenStream tokenStream = allFlagsFactory.create(tokenizer)) {
            // This config outputs different size of ngrams so graph analysis is disabled
            assertTrue(tokenStream.hasAttribute(DisableGraphAttribute.class));
        }

        tokenizer = new StandardTokenizer();
        tokenizer.setReader(new StringReader(source));
        try (TokenStream tokenStream = hanOnlyFactory.create(tokenizer)) {
            // This config uses only bigrams so graph analysis is enabled
            assertFalse(tokenStream.hasAttribute(DisableGraphAttribute.class));
        }
    }
}

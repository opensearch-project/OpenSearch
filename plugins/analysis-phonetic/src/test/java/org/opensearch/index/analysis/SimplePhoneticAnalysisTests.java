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

package org.opensearch.index.analysis;

import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.phonetic.DaitchMokotoffSoundexFilter;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.plugin.analysis.AnalysisPhoneticPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.instanceOf;

public class SimplePhoneticAnalysisTests extends OpenSearchTestCase {

    private TestAnalysis analysis;

    @Before
    public void setup() throws IOException {
        String yaml = "/org/opensearch/index/analysis/phonetic-1.yml";
        Settings settings = Settings.builder()
            .loadFromStream(yaml, getClass().getResourceAsStream(yaml), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        this.analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisPhoneticPlugin());
    }

    public void testPhoneticTokenFilterFactory() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("phonetic");
        MatcherAssert.assertThat(filterFactory, instanceOf(PhoneticTokenFilterFactory.class));
    }

    public void testPhoneticTokenFilterBeiderMorseNoLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilter");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("ABADIAS"));
        String[] expected = new String[] {
            "abYdias",
            "abYdios",
            "abadia",
            "abadiaS",
            "abadias",
            "abadio",
            "abadioS",
            "abadios",
            "abodia",
            "abodiaS",
            "abodias",
            "abodio",
            "abodioS",
            "abodios",
            "avadias",
            "avadios",
            "avodias",
            "avodios",
            "obadia",
            "obadiaS",
            "obadias",
            "obadio",
            "obadioS",
            "obadios",
            "obodia",
            "obodiaS",
            "obodias",
            "obodioS" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterBeiderMorseWithLanguage() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("beidermorsefilterfrench");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("Rimbault"));
        String[] expected = new String[] {
            "rimbD",
            "rimbDlt",
            "rimba",
            "rimbalt",
            "rimbo",
            "rimbolt",
            "rimbu",
            "rimbult",
            "rmbD",
            "rmbDlt",
            "rmba",
            "rmbalt",
            "rmbo",
            "rmbolt",
            "rmbu",
            "rmbult" };
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

    public void testPhoneticTokenFilterDaitchMotokoff() throws IOException {
        TokenFilterFactory filterFactory = analysis.tokenFilter.get("daitch_mokotoff");
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader("chauptman"));
        String[] expected = new String[] { "473660", "573660" };
        assertThat(filterFactory.create(tokenizer), instanceOf(DaitchMokotoffSoundexFilter.class));
        BaseTokenStreamTestCase.assertTokenStreamContents(filterFactory.create(tokenizer), expected);
    }

}

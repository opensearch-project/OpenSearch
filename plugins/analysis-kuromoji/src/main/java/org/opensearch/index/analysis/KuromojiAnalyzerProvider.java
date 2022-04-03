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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

import java.util.Set;

public class KuromojiAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseAnalyzer> {

    private final JapaneseAnalyzer analyzer;

    public KuromojiAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        final Set<?> stopWords = Analysis.parseStopWords(env, settings, JapaneseAnalyzer.getDefaultStopSet());
        final JapaneseTokenizer.Mode mode = KuromojiTokenizerFactory.getMode(settings);
        final UserDictionary userDictionary = KuromojiTokenizerFactory.getUserDictionary(env, settings);
        analyzer = new JapaneseAnalyzer(userDictionary, mode, CharArraySet.copy(stopWords), JapaneseAnalyzer.getDefaultStopTags());
    }

    @Override
    public JapaneseAnalyzer get() {
        return this.analyzer;
    }

}

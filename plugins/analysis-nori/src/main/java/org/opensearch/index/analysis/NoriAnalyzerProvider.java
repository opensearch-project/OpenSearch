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

import java.util.List;
import java.util.Set;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.ko.KoreanPartOfSpeechStopFilter;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.apache.lucene.analysis.ko.POS;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

import static org.opensearch.index.analysis.NoriPartOfSpeechStopFilterFactory.resolvePOSList;

public class NoriAnalyzerProvider extends AbstractIndexAnalyzerProvider<KoreanAnalyzer> {
    private final KoreanAnalyzer analyzer;

    public NoriAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        final KoreanTokenizer.DecompoundMode mode = NoriTokenizerFactory.getMode(settings);
        final UserDictionary userDictionary = NoriTokenizerFactory.getUserDictionary(env, settings);
        final List<String> tagList = Analysis.getWordList(env, settings, "stoptags");
        final Set<POS.Tag> stopTags = tagList != null ? resolvePOSList(tagList) : KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
        analyzer = new KoreanAnalyzer(userDictionary, mode, stopTags, false);
    }

    @Override
    public KoreanAnalyzer get() {
        return analyzer;
    }

}

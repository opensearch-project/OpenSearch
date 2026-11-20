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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.opensearch.index.analysis.Analysis;

/**
 * Provider for the {@code thai} analyzer.
 *
 * <p>Lucene 10.6 changed what its Thai analyzer, default stopwords and tokenizer produce for some text. To avoid a reindex,
 * indices created before {@link Version#V_3_10_0} keep the previous behavior ({@link LegacyThaiAnalyzer},
 * {@link LegacyThaiTokenizer}, {@link Analysis#legacyThaiStopWords()}); newer indices use Lucene's
 * {@link ThaiAnalyzer} and its default stopwords.
 */
public class ThaiAnalyzerProvider extends AbstractIndexAnalyzerProvider<StopwordAnalyzerBase> {

    private final StopwordAnalyzerBase analyzer;

    ThaiAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        Version indexCreatedVersion = indexSettings.getIndexVersionCreated();
        analyzer = create(indexCreatedVersion, Analysis.parseStopWords(env, settings, defaultStopWords(indexCreatedVersion)));
    }

    /**
     * Whether an index created with {@code indexCreatedVersion} uses the Thai components as changed in Lucene 10.6.
     */
    static boolean usesLucene106Thai(Version indexCreatedVersion) {
        return indexCreatedVersion.onOrAfter(Version.V_3_10_0);
    }

    static CharArraySet defaultStopWords(Version indexCreatedVersion) {
        return usesLucene106Thai(indexCreatedVersion) ? ThaiAnalyzer.getDefaultStopSet() : Analysis.legacyThaiStopWords();
    }

    static StopwordAnalyzerBase create(Version indexCreatedVersion, CharArraySet stopwords) {
        return usesLucene106Thai(indexCreatedVersion) ? new ThaiAnalyzer(stopwords) : new LegacyThaiAnalyzer(stopwords);
    }

    @Override
    public StopwordAnalyzerBase get() {
        return this.analyzer;
    }
}

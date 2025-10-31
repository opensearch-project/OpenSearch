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

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Map;

import static org.opensearch.index.analysis.AnalyzerComponents.createComponents;

/**
 * A custom analyzer that is built out of a single {@link org.apache.lucene.analysis.Tokenizer} and a list
 * of {@link org.apache.lucene.analysis.TokenFilter}s.
 *
 * @opensearch.internal
 */
public class CustomAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Settings analyzerSettings;

    private Analyzer customAnalyzer;

    public CustomAnalyzerProvider(IndexSettings indexSettings, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.analyzerSettings = settings;
    }

    void build(
        final Map<String, TokenizerFactory> tokenizers,
        final Map<String, CharFilterFactory> charFilters,
        final Map<String, TokenFilterFactory> tokenFilters,
        final Map<String, Analyzer> analyzersBuiltSoFar
    ) {
        customAnalyzer = create(name(), analyzerSettings, tokenizers, charFilters, tokenFilters, analyzersBuiltSoFar);
    }

    /**
     * Factory method that either returns a plain {@link ReloadableCustomAnalyzer} if the components used for creation are supporting index
     * and search time use, or a {@link ReloadableCustomAnalyzer} if the components are intended for search time use only.
     */
    private static Analyzer create(
        String name,
        Settings analyzerSettings,
        Map<String, TokenizerFactory> tokenizers,
        Map<String, CharFilterFactory> charFilters,
        Map<String, TokenFilterFactory> tokenFilters,
        Map<String, Analyzer> analyzersBuiltSoFar
    ) {
        int positionIncrementGap = TextFieldMapper.Defaults.POSITION_INCREMENT_GAP;
        positionIncrementGap = analyzerSettings.getAsInt("position_increment_gap", positionIncrementGap);
        int offsetGap = analyzerSettings.getAsInt("offset_gap", -1);
        AnalyzerComponents components = createComponents(
            name,
            analyzerSettings,
            tokenizers,
            charFilters,
            tokenFilters,
            analyzersBuiltSoFar
        );
        if (components.analysisMode().equals(AnalysisMode.SEARCH_TIME)) {
            return new ReloadableCustomAnalyzer(components, positionIncrementGap, offsetGap);
        } else {
            return new CustomAnalyzer(
                components.getTokenizerFactory(),
                components.getCharFilters(),
                components.getTokenFilters(),
                positionIncrementGap,
                offsetGap
            );
        }
    }

    @Override
    public Analyzer get() {
        return this.customAnalyzer;
    }
}

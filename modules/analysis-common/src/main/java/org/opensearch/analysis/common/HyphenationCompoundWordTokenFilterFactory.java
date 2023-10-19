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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.Analysis;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.xml.sax.InputSource;

/**
 * Uses the {@link org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter} to decompound tokens based on hyphenation rules.
 *
 * @see org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter
 */
public class HyphenationCompoundWordTokenFilterFactory extends AbstractCompoundWordTokenFilterFactory {

    private final boolean noSubMatches;
    private final boolean noOverlappingMatches;
    private final HyphenationTree hyphenationTree;

    HyphenationCompoundWordTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, env, name, settings);

        noSubMatches = settings.getAsBoolean("no_sub_matches", false);
        noOverlappingMatches = settings.getAsBoolean("no_overlapping_matches", false);

        String hyphenationPatternsPath = settings.get("hyphenation_patterns_path", null);
        if (hyphenationPatternsPath == null) {
            throw new IllegalArgumentException("hyphenation_patterns_path is a required setting.");
        }

        Path hyphenationPatternsFile = Analysis.resolveAnalyzerPath(env, hyphenationPatternsPath);

        try {
            InputStream in = Files.newInputStream(hyphenationPatternsFile);
            hyphenationTree = HyphenationCompoundWordTokenFilter.getHyphenationTree(new InputSource(in));
        } catch (Exception e) {
            LogManager.getLogger(HyphenationCompoundWordTokenFilterFactory.class)
                .error("Exception while reading hyphenation_patterns_path ", e);
            throw new IllegalArgumentException("Exception while reading hyphenation_patterns_path.");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HyphenationCompoundWordTokenFilter(
            tokenStream,
            hyphenationTree,
            wordList,
            minWordSize,
            minSubwordSize,
            maxSubwordSize,
            onlyLongestMatch,
            noSubMatches,
            noOverlappingMatches
        );
    }
}

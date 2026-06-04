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
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.Analysis;
import org.opensearch.index.analysis.TokenFilterFactory;

/**
 * Contains the common configuration settings between subclasses of this class.
 */
public abstract class AbstractCompoundWordTokenFilterFactory extends AbstractTokenFilterFactory {

    protected final int minWordSize;
    protected final int minSubwordSize;
    protected final int maxSubwordSize;
    protected final boolean onlyLongestMatch;
    protected final CharArraySet wordList;

    protected AbstractCompoundWordTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        minWordSize = settings.getAsInt("min_word_size", CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE);
        minSubwordSize = settings.getAsInt("min_subword_size", CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE);
        maxSubwordSize = settings.getAsInt("max_subword_size", CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE);
        onlyLongestMatch = settings.getAsBoolean("only_longest_match", false);
        wordList = Analysis.getWordSet(env, settings, "word_list");
        if (wordList == null) {
            throw new IllegalArgumentException("word_list must be provided for [" + name + "], either as a path to a file, or directly");
        }
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        return IDENTITY_FILTER;     // don't decompound synonym file
    }
}

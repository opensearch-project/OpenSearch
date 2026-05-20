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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.util.CSVUtil;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KuromojiTokenizerFactory extends AbstractTokenizerFactory {

    private static final Logger LOGGER = LogManager.getLogger(KuromojiTokenizerFactory.class);
    private static final String USER_DICT_PATH_OPTION = "user_dictionary";
    private static final String USER_DICT_RULES_OPTION = "user_dictionary_rules";
    private static final String NBEST_COST = "nbest_cost";
    private static final String NBEST_EXAMPLES = "nbest_examples";
    private static final String DISCARD_COMPOUND_TOKEN = "discard_compound_token";

    private final UserDictionary userDictionary;
    private final Mode mode;
    private final String nBestExamples;
    private final int nBestCost;

    private boolean discardPunctuation;
    private boolean discardCompoundToken;

    public KuromojiTokenizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, settings, name);
        mode = getMode(settings);
        userDictionary = getUserDictionary(env, settings);
        discardPunctuation = settings.getAsBoolean("discard_punctuation", true);
        nBestCost = settings.getAsInt(NBEST_COST, -1);
        nBestExamples = settings.get(NBEST_EXAMPLES);
        discardCompoundToken = settings.getAsBoolean(DISCARD_COMPOUND_TOKEN, false);
    }

    private static String parse(String rule, Set<String> dup) {
        String[] values = CSVUtil.parse(rule);
        if (values.length == 0) {
            throw new IllegalArgumentException("Malformed csv in user dictionary.");
        }
        if (dup.add(values[0]) == false) {
            throw new IllegalArgumentException("Found duplicate term [" + values[0] + "] in user dictionary.");
        }
        return rule;
    }

    public static UserDictionary getUserDictionary(Environment env, Settings settings) {
        if (settings.get(USER_DICT_PATH_OPTION) != null && settings.get(USER_DICT_RULES_OPTION) != null) {
            throw new IllegalArgumentException(
                "It is not allowed to use [" + USER_DICT_PATH_OPTION + "] in conjunction" + " with [" + USER_DICT_RULES_OPTION + "]"
            );
        }
        try {
            Set<String> dup = new HashSet<>();
            List<String> ruleList = Analysis.parseWordList(
                env,
                settings,
                USER_DICT_PATH_OPTION,
                USER_DICT_RULES_OPTION,
                s -> parse(s, dup)
            );
            if (ruleList == null || ruleList.isEmpty()) {
                return null;
            }

            StringBuilder sb = new StringBuilder();
            for (String line : ruleList) {
                sb.append(line).append(System.lineSeparator());
            }
            return UserDictionary.open(new StringReader(sb.toString()));
        } catch (IOException e) {
            LOGGER.error("Failed to load kuromoji user dictionary", e);
            throw new OpenSearchException("Failed to load kuromoji user dictionary");
        }
    }

    public static JapaneseTokenizer.Mode getMode(Settings settings) {
        JapaneseTokenizer.Mode mode = JapaneseTokenizer.DEFAULT_MODE;
        String modeSetting = settings.get("mode", null);
        if (modeSetting != null) {
            if ("search".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseTokenizer.Mode.SEARCH;
            } else if ("normal".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseTokenizer.Mode.NORMAL;
            } else if ("extended".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseTokenizer.Mode.EXTENDED;
            }
        }
        return mode;
    }

    @Override
    public Tokenizer create() {
        JapaneseTokenizer t = new JapaneseTokenizer(userDictionary, discardPunctuation, discardCompoundToken, mode);
        int nBestCost = this.nBestCost;
        if (nBestExamples != null) {
            nBestCost = Math.max(nBestCost, t.calcNBestCost(nBestExamples));
        }
        t.setNBestCost(nBestCost);
        return t;
    }

}

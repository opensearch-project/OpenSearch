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
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

public class NoriTokenizerFactory extends AbstractTokenizerFactory {
    private static final Logger LOGGER = LogManager.getLogger(NoriTokenizerFactory.class);
    private static final String USER_DICT_PATH_OPTION = "user_dictionary";
    private static final String USER_DICT_RULES_OPTION = "user_dictionary_rules";

    private final UserDictionary userDictionary;
    private final KoreanTokenizer.DecompoundMode decompoundMode;
    private final boolean discardPunctuation;

    public NoriTokenizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, settings, name);
        decompoundMode = getMode(settings);
        userDictionary = getUserDictionary(env, settings);
        discardPunctuation = settings.getAsBoolean("discard_punctuation", true);
    }

    public static UserDictionary getUserDictionary(Environment env, Settings settings) {
        if (settings.get(USER_DICT_PATH_OPTION) != null && settings.get(USER_DICT_RULES_OPTION) != null) {
            throw new IllegalArgumentException(
                "It is not allowed to use [" + USER_DICT_PATH_OPTION + "] in conjunction" + " with [" + USER_DICT_RULES_OPTION + "]"
            );
        }
        List<String> ruleList = Analysis.parseWordList(env, settings, USER_DICT_PATH_OPTION, USER_DICT_RULES_OPTION, s -> s);
        StringBuilder sb = new StringBuilder();
        if (ruleList == null || ruleList.isEmpty()) {
            return null;
        }
        for (String line : ruleList) {
            sb.append(line).append(System.lineSeparator());
        }
        try (Reader rulesReader = new StringReader(sb.toString())) {
            return UserDictionary.open(rulesReader);
        } catch (IOException e) {
            LOGGER.error("Failed to load nori user dictionary", e);
            throw new OpenSearchException("Failed to load nori user dictionary");
        }
    }

    public static KoreanTokenizer.DecompoundMode getMode(Settings settings) {
        KoreanTokenizer.DecompoundMode mode = KoreanTokenizer.DEFAULT_DECOMPOUND;
        String modeSetting = settings.get("decompound_mode", null);
        if (modeSetting != null) {
            mode = KoreanTokenizer.DecompoundMode.valueOf(modeSetting.toUpperCase(Locale.ENGLISH));
        }
        return mode;
    }

    @Override
    public Tokenizer create() {
        return new KoreanTokenizer(
            KoreanTokenizer.DEFAULT_TOKEN_ATTRIBUTE_FACTORY,
            userDictionary,
            decompoundMode,
            false,
            discardPunctuation
        );
    }

}

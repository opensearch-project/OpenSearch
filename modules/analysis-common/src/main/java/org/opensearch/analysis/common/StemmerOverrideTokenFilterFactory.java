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
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.Analysis;
import org.opensearch.index.analysis.MappingRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StemmerOverrideTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final String MAPPING_SEPARATOR = "=>";
    private final StemmerOverrideMap overrideMap;

    StemmerOverrideTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);

        List<MappingRule<List<String>, String>> rules = Analysis.parseWordList(env, settings, "rules", this::parse);
        if (rules == null) {
            throw new IllegalArgumentException("stemmer override filter requires either `rules` or `rules_path` to be configured");
        }

        StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(false);
        for (MappingRule<List<String>, String> rule : rules) {
            for (String key : rule.getLeft()) {
                builder.add(key, rule.getRight());
            }
        }
        overrideMap = builder.build();

    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new StemmerOverrideFilter(tokenStream, overrideMap);
    }

    private MappingRule<List<String>, String> parse(String rule) {
        String[] sides = rule.split(MAPPING_SEPARATOR, -1);
        if (sides.length != 2) {
            throw new RuntimeException("Invalid keyword override rule: " + rule);
        }

        String[] keys = sides[0].split(",", -1);
        String override = sides[1].trim();
        if (override.isEmpty() || override.indexOf(',') != -1) {
            throw new RuntimeException("Invalid keyword override rule: " + rule);
        }

        List<String> trimmedKeys = new ArrayList<>();
        for (String key : keys) {
            String trimmedKey = key.trim();
            if (trimmedKey.isEmpty()) {
                throw new RuntimeException("Invalid keyword override rule: " + rule);
            }
            trimmedKeys.add(trimmedKey);
        }
        return new MappingRule<>(trimmedKeys, override);
    }
}

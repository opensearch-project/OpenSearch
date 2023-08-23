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

import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractCharFilterFactory;
import org.opensearch.index.analysis.Analysis;
import org.opensearch.index.analysis.MappingRule;
import org.opensearch.index.analysis.NormalizingCharFilterFactory;

import java.io.Reader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingCharFilterFactory extends AbstractCharFilterFactory implements NormalizingCharFilterFactory {

    private final NormalizeCharMap normMap;

    MappingCharFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name);

        List<MappingRule<String, String>> rules = Analysis.parseWordList(env, settings, "mappings", this::parse, false);
        if (rules == null) {
            throw new IllegalArgumentException("mapping requires either `mappings` or `mappings_path` to be configured");
        }

        NormalizeCharMap.Builder normMapBuilder = new NormalizeCharMap.Builder();
        rules.forEach(rule -> normMapBuilder.add(rule.getLeft(), rule.getRight()));
        normMap = normMapBuilder.build();
    }

    @Override
    public Reader create(Reader tokenStream) {
        return new MappingCharFilter(normMap, tokenStream);
    }

    // source => target
    private static Pattern rulePattern = Pattern.compile("(.*)\\s*=>\\s*(.*)\\s*$");

    private MappingRule<String, String> parse(String rule) {
        Matcher m = rulePattern.matcher(rule);
        if (!m.find()) throw new RuntimeException("Invalid mapping rule : [" + rule + "]");
        String lhs = parseString(m.group(1).trim());
        String rhs = parseString(m.group(2).trim());
        if (lhs == null || rhs == null) throw new RuntimeException("Invalid mapping rule: [" + rule + "]. Illegal mapping.");
        return new MappingRule<>(lhs, rhs);
    }

    char[] out = new char[256];

    private String parseString(String s) {
        int readPos = 0;
        int len = s.length();
        int writePos = 0;
        while (readPos < len) {
            char c = s.charAt(readPos++);
            if (c == '\\') {
                if (readPos >= len) throw new RuntimeException("Invalid escaped char in [" + s + "]");
                c = s.charAt(readPos++);
                switch (c) {
                    case '\\':
                        c = '\\';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'u':
                        if (readPos + 3 >= len) throw new RuntimeException("Invalid escaped char in [" + s + "]");
                        c = (char) Integer.parseInt(s.substring(readPos, readPos + 4), 16);
                        readPos += 4;
                        break;
                }
            }
            out[writePos++] = c;
        }
        return new String(out, 0, writePos);
    }

}

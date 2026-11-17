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
import org.apache.lucene.analysis.minhash.MinHashFilter;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * TokenFilterFactoryAdapter for {@link MinHashFilterFactory}
 *
 */
public class MinHashTokenFilterFactory extends AbstractTokenFilterFactory {

    private final MinHashFilterFactory minHashFilterFactory;

    // Lucene eagerly allocates hash_count * bucket_count sets per filter instance and emits up to
    // hash_count * bucket_count * hash_set_size tokens, so the product must be bounded.
    static final long MAX_HASH_PRODUCT = 16_384;

    MinHashTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        long hashCount = settings.getAsInt("hash_count", MinHashFilter.DEFAULT_HASH_COUNT);
        long bucketCount = settings.getAsInt("bucket_count", MinHashFilter.DEFAULT_BUCKET_COUNT);
        long hashSetSize = settings.getAsInt("hash_set_size", MinHashFilter.DEFAULT_HASH_SET_SIZE);
        long product = hashCount * bucketCount * hashSetSize;
        if (product > MAX_HASH_PRODUCT) {
            throw new IllegalArgumentException(
                "[min_hash] hash_count * bucket_count * hash_set_size must be less than or equal to: ["
                    + MAX_HASH_PRODUCT
                    + "] but was ["
                    + product
                    + "]"
            );
        }
        minHashFilterFactory = new MinHashFilterFactory(convertSettings(settings));
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return minHashFilterFactory.create(tokenStream);
    }

    private Map<String, String> convertSettings(Settings settings) {
        Map<String, String> settingMap = new HashMap<>();
        if (settings.hasValue("hash_count")) {
            settingMap.put("hashCount", settings.get("hash_count"));
        }
        if (settings.hasValue("bucket_count")) {
            settingMap.put("bucketCount", settings.get("bucket_count"));
        }
        if (settings.hasValue("hash_set_size")) {
            settingMap.put("hashSetSize", settings.get("hash_set_size"));
        }
        if (settings.hasValue("with_rotation")) {
            settingMap.put("withRotation", settings.get("with_rotation"));
        }
        return settingMap;
    }
}

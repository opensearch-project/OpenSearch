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

    /**
     * Upper bound on each of {@code hash_count}, {@code bucket_count} and {@code hash_set_size}, and on their
     * product. The filter eagerly allocates one {@code TreeSet} per {@code (hash_count * bucket_count)} bucket
     * when it is created, independently of the analyzed text. Each empty bucket retains roughly 76 bytes
     * (measured on JDK 21; it varies with the JDK, object-header flags and heap size, but stays in that order
     * of magnitude), so {@code hash_count=4000, bucket_count=4000} forces about 1.1 GiB of allocation and a
     * single {@code _analyze} request can exhaust the node heap. Capping the product at 2^20 bounds that
     * eager allocation to roughly 80 MB per filter.
     */
    static final int MAX_HASH_COUNT = 1024;
    static final int MAX_BUCKET_COUNT = 1024;
    static final int MAX_HASH_SET_SIZE = 1024;
    static final int MAX_TOTAL_ALLOCATIONS = 1024 * 1024;

    private final MinHashFilterFactory minHashFilterFactory;

    MinHashTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        validateSettings(settings);
        minHashFilterFactory = new MinHashFilterFactory(convertSettings(settings));
    }

    private static void validateSettings(Settings settings) {
        int hashCount = boundedSetting(settings, "hash_count", MinHashFilter.DEFAULT_HASH_COUNT, MAX_HASH_COUNT);
        int bucketCount = boundedSetting(settings, "bucket_count", MinHashFilter.DEFAULT_BUCKET_COUNT, MAX_BUCKET_COUNT);
        int hashSetSize = boundedSetting(settings, "hash_set_size", MinHashFilter.DEFAULT_HASH_SET_SIZE, MAX_HASH_SET_SIZE);

        long totalAllocations = (long) hashCount * bucketCount * hashSetSize;
        if (totalAllocations > MAX_TOTAL_ALLOCATIONS) {
            throw new IllegalArgumentException(
                "The product of [hash_count], [bucket_count] and [hash_set_size] in a [min_hash] token filter must not "
                    + "exceed ["
                    + MAX_TOTAL_ALLOCATIONS
                    + "] but was ["
                    + totalAllocations
                    + "]"
            );
        }
    }

    private static int boundedSetting(Settings settings, String key, int defaultValue, int max) {
        int value = settings.getAsInt(key, defaultValue);
        if (value < 1) {
            throw new IllegalArgumentException("[" + key + "] in a [min_hash] token filter must be at least 1 but was [" + value + "]");
        }
        if (value > max) {
            throw new IllegalArgumentException(
                "[" + key + "] in a [min_hash] token filter must not exceed [" + max + "] but was [" + value + "]"
            );
        }
        return value;
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

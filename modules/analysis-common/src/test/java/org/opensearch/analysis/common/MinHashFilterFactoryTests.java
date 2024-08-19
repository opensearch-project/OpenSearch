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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

public class MinHashFilterFactoryTests extends OpenSearchTokenStreamTestCase {
    public void testDefault() throws IOException {
        int default_hash_count = 1;
        int default_bucket_size = 512;
        int default_hash_set_size = 1;
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("min_hash");
        String source = "the quick brown fox";
        Tokenizer tokenizer = getTokenizer(source);

        // with_rotation is true by default, and hash_set_size is 1, so even though the source doesn't
        // have enough tokens to fill all the buckets, we still expect 512 tokens.
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), default_hash_count * default_bucket_size * default_hash_set_size);
    }

    public void testSettings() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "2")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put("index.analysis.filter.test_min_hash.with_rotation", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        OpenSearchTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("test_min_hash");
        String source = "sushi";
        Tokenizer tokenizer = getTokenizer(source);

        // despite the fact that bucket_count is 2 and hash_set_size is 1,
        // because with_rotation is false, we only expect 1 token here.
        assertStreamHasNumberOfTokens(tokenFilter.create(tokenizer), 1);
    }

    public void testBucketCountSetting() throws IOException {
        // Correct case with "bucket_count"
        Settings settingsWithBucketCount = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "3")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "1")
            .put("index.analysis.filter.test_min_hash.with_rotation", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        OpenSearchTestCase.TestAnalysis analysisWithBucketCount = getTestAnalysisFromSettings(settingsWithBucketCount);

        TokenFilterFactory tokenFilterWithBucketCount = analysisWithBucketCount.tokenFilter.get("test_min_hash");
        String sourceWithBucketCount = "salmon avocado roll uramaki";
        Tokenizer tokenizerWithBucketCount = getTokenizer(sourceWithBucketCount);
        // Expect 3 tokens due to bucket_count being set to 3
        assertStreamHasNumberOfTokens(tokenFilterWithBucketCount.create(tokenizerWithBucketCount), 3);
    }

    public void testHashSetSizeSetting() throws IOException {
        // Correct case with "hash_set_size"
        Settings settingsWithHashSetSize = Settings.builder()
            .put("index.analysis.filter.test_min_hash.type", "min_hash")
            .put("index.analysis.filter.test_min_hash.hash_count", "1")
            .put("index.analysis.filter.test_min_hash.bucket_count", "1")
            .put("index.analysis.filter.test_min_hash.hash_set_size", "2")
            .put("index.analysis.filter.test_min_hash.with_rotation", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        OpenSearchTestCase.TestAnalysis analysisWithHashSetSize = getTestAnalysisFromSettings(settingsWithHashSetSize);

        TokenFilterFactory tokenFilterWithHashSetSize = analysisWithHashSetSize.tokenFilter.get("test_min_hash");
        String sourceWithHashSetSize = "salmon avocado roll uramaki";
        Tokenizer tokenizerWithHashSetSize = getTokenizer(sourceWithHashSetSize);
        // Expect 2 tokens due to hash_set_size being set to 2 and bucket_count being 1
        assertStreamHasNumberOfTokens(tokenFilterWithHashSetSize.create(tokenizerWithHashSetSize), 2);
    }

    private static OpenSearchTestCase.TestAnalysis getTestAnalysisFromSettings(Settings settingsWithBucketCount) throws IOException {
        return AnalysisTestsHelper.createTestAnalysisFromSettings(settingsWithBucketCount, new CommonAnalysisPlugin());
    }

    private static Tokenizer getTokenizer(String sourceWithBucketCount) {
        Tokenizer tokenizerWithBucketCount = new WhitespaceTokenizer();
        tokenizerWithBucketCount.setReader(new StringReader(sourceWithBucketCount));
        return tokenizerWithBucketCount;
    }
}

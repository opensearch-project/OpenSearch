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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.analyze;

import org.opensearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

import java.util.Map;

/**
 * Transport request builder for analyzing text
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AnalyzeRequestBuilder extends SingleShardOperationRequestBuilder<
    AnalyzeAction.Request,
    AnalyzeAction.Response,
    AnalyzeRequestBuilder> {

    public AnalyzeRequestBuilder(OpenSearchClient client, AnalyzeAction action) {
        super(client, action, new AnalyzeAction.Request());
    }

    public AnalyzeRequestBuilder(OpenSearchClient client, AnalyzeAction action, String index, String... text) {
        super(client, action, new AnalyzeAction.Request(index).text(text));
    }

    /**
     * Sets the analyzer name to use in order to analyze the text.
     *
     * @param analyzer The analyzer name.
     */
    public AnalyzeRequestBuilder setAnalyzer(String analyzer) {
        request.analyzer(analyzer);
        return this;
    }

    /**
     * Sets the field that its analyzer will be used to analyze the text. Note, requires an index
     * to be set.
     */
    public AnalyzeRequestBuilder setField(String field) {
        request.field(field);
        return this;
    }

    /**
     * Instead of setting the analyzer, sets the tokenizer as name that will be used as part of a custom
     * analyzer.
     */
    public AnalyzeRequestBuilder setTokenizer(String tokenizer) {
        request.tokenizer(tokenizer);
        return this;
    }

    /**
     * Instead of setting the analyzer, sets the tokenizer using custom settings that will be used as part of a custom
     * analyzer.
     */
    public AnalyzeRequestBuilder setTokenizer(Map<String, ?> tokenizer) {
        request.tokenizer(tokenizer);
        return this;
    }

    /**
     * Add token filter setting that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder addTokenFilter(Map<String, ?> tokenFilter) {
        request.addTokenFilter(tokenFilter);
        return this;
    }

    /**
     * Add a name of token filter that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder addTokenFilter(String tokenFilter) {
        request.addTokenFilter(tokenFilter);
        return this;
    }

    /**
     * Add char filter setting that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder addCharFilter(Map<String, ?> charFilter) {
        request.addCharFilter(charFilter);
        return this;
    }

    /**
     * Add a name of char filter that will be used before the tokenizer.
     */
    public AnalyzeRequestBuilder addCharFilter(String tokenFilter) {
        request.addCharFilter(tokenFilter);
        return this;
    }

    /**
     * Sets explain
     */
    public AnalyzeRequestBuilder setExplain(boolean explain) {
        request.explain(explain);
        return this;
    }

    /**
     * Sets attributes that will include results
     */
    public AnalyzeRequestBuilder setAttributes(String... attributes) {
        request.attributes(attributes);
        return this;
    }

    /**
     * Sets texts to analyze
     */
    public AnalyzeRequestBuilder setText(String... texts) {
        request.text(texts);
        return this;
    }

    /**
     * Instead of setting the analyzer and tokenizer, sets the normalizer as name
     */
    public AnalyzeRequestBuilder setNormalizer(String normalizer) {
        request.normalizer(normalizer);
        return this;
    }

}

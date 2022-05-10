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

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.Version;
import org.opensearch.indices.analysis.PreBuiltCacheFactory;
import org.opensearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides pre-configured, shared {@link Tokenizer}s.
 *
 * @opensearch.internal
 */
public final class PreConfiguredTokenizer extends PreConfiguredAnalysisComponent<TokenizerFactory> {
    /**
     * Create a pre-configured tokenizer that may not vary at all.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer singleton(String name, Supplier<Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.ONE, version -> create.get());
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the Lucene version.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer luceneVersion(String name, Function<org.apache.lucene.util.Version, Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.LUCENE, version -> create.apply(version.luceneVersion));
    }

    /**
     * Create a pre-configured tokenizer that may vary based on the OpenSearch version.
     *
     * @param name the name of the tokenizer in the api
     * @param create builds the tokenizer
     */
    public static PreConfiguredTokenizer openSearchVersion(String name, Function<org.opensearch.Version, Tokenizer> create) {
        return new PreConfiguredTokenizer(name, CachingStrategy.OPENSEARCH, create);
    }

    private final Function<Version, Tokenizer> create;

    private PreConfiguredTokenizer(String name, PreBuiltCacheFactory.CachingStrategy cache, Function<Version, Tokenizer> create) {
        super(name, cache);
        this.create = create;
    }

    @Override
    protected TokenizerFactory create(Version version) {
        return TokenizerFactory.newFactory(name, () -> create.apply(version));
    }
}

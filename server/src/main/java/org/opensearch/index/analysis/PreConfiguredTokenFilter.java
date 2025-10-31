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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.indices.analysis.PreBuiltCacheFactory;
import org.opensearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides pre-configured, shared {@link TokenFilter}s.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class PreConfiguredTokenFilter extends PreConfiguredAnalysisComponent<TokenFilterFactory> {
    /**
     * Create a pre-configured token filter that may not vary at all.
     */
    public static PreConfiguredTokenFilter singleton(
        String name,
        boolean useFilterForMultitermQueries,
        Function<TokenStream, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            true,
            CachingStrategy.ONE,
            (tokenStream, version) -> create.apply(tokenStream)
        );
    }

    /**
     * Create a pre-configured token filter that may not vary at all.
     */
    public static PreConfiguredTokenFilter singleton(
        String name,
        boolean useFilterForMultitermQueries,
        boolean allowForSynonymParsing,
        Function<TokenStream, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            allowForSynonymParsing,
            CachingStrategy.ONE,
            (tokenStream, version) -> create.apply(tokenStream)
        );
    }

    /**
     * Create a pre-configured token filter that may vary based on the Lucene version.
     */
    public static PreConfiguredTokenFilter luceneVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<TokenStream, org.apache.lucene.util.Version, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            true,
            CachingStrategy.LUCENE,
            (tokenStream, version) -> create.apply(tokenStream, version.luceneVersion)
        );
    }

    /**
     * Create a pre-configured token filter that may vary based on the OpenSearch version.
     */
    public static PreConfiguredTokenFilter openSearchVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<TokenStream, org.opensearch.Version, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(name, useFilterForMultitermQueries, true, CachingStrategy.OPENSEARCH, create);
    }

    /**
     * Create a pre-configured token filter that may vary based on the OpenSearch version.
     */
    public static PreConfiguredTokenFilter openSearchVersion(
        String name,
        boolean useFilterForMultitermQueries,
        boolean useFilterForParsingSynonyms,
        BiFunction<TokenStream, Version, TokenStream> create
    ) {
        return new PreConfiguredTokenFilter(
            name,
            useFilterForMultitermQueries,
            useFilterForParsingSynonyms,
            CachingStrategy.OPENSEARCH,
            create
        );
    }

    private final boolean useFilterForMultitermQueries;
    private final boolean allowForSynonymParsing;
    private final BiFunction<TokenStream, Version, TokenStream> create;

    private PreConfiguredTokenFilter(
        String name,
        boolean useFilterForMultitermQueries,
        boolean allowForSynonymParsing,
        PreBuiltCacheFactory.CachingStrategy cache,
        BiFunction<TokenStream, Version, TokenStream> create
    ) {
        super(name, cache);
        this.useFilterForMultitermQueries = useFilterForMultitermQueries;
        this.allowForSynonymParsing = allowForSynonymParsing;
        this.create = create;
    }

    /**
     * Can this {@link TokenFilter} be used in multi-term queries?
     */
    public boolean shouldUseFilterForMultitermQueries() {
        return useFilterForMultitermQueries;
    }

    @Override
    protected TokenFilterFactory create(Version version) {
        if (useFilterForMultitermQueries) {
            return new NormalizingTokenFilterFactory() {

                @Override
                public TokenStream normalize(TokenStream tokenStream) {
                    return create.apply(tokenStream, version);
                }

                @Override
                public String name() {
                    return getName();
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return create.apply(tokenStream, version);
                }

                @Override
                public TokenFilterFactory getSynonymFilter() {
                    if (allowForSynonymParsing) {
                        return this;
                    }
                    throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
                }
            };
        }
        return new TokenFilterFactory() {
            @Override
            public String name() {
                return getName();
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return create.apply(tokenStream, version);
            }

            @Override
            public TokenFilterFactory getSynonymFilter() {
                if (allowForSynonymParsing) {
                    return this;
                }
                throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
            }
        };
    }
}

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

package org.opensearch.index.query.support;

import org.apache.lucene.search.MultiTermQuery;
import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.index.query.QueryShardContext;

/**
 * Utility class for Query Parsers
 *
 * @opensearch.internal
 */
public final class QueryParsers {

    public static final ParseField CONSTANT_SCORE = new ParseField("constant_score");
    public static final ParseField SCORING_BOOLEAN = new ParseField("scoring_boolean");
    public static final ParseField CONSTANT_SCORE_BOOLEAN = new ParseField("constant_score_boolean");
    public static final ParseField TOP_TERMS = new ParseField("top_terms_");
    public static final ParseField TOP_TERMS_BOOST = new ParseField("top_terms_boost_");
    public static final ParseField TOP_TERMS_BLENDED_FREQS = new ParseField("top_terms_blended_freqs_");

    public static final ParseField INDEX_OR_DOC_VALUES = new ParseField("index_or_doc_values");
    public static final ParseField INDEX_ONLY = new ParseField("index_only");
    public static final ParseField DOC_VALUES_ONLY = new ParseField("doc_values_only");

    private QueryParsers() {

    }

    public static void setRewriteMethod(MultiTermQuery query, @Nullable MultiTermQuery.RewriteMethod rewriteMethod) {
        if (rewriteMethod == null) {
            return;
        }
        query.setRewriteMethod(rewriteMethod);
    }

    public static MultiTermQuery.RewriteMethod parseRewriteMethod(@Nullable String rewriteMethod, DeprecationHandler deprecationHandler) {
        return parseRewriteMethod(rewriteMethod, MultiTermQuery.CONSTANT_SCORE_REWRITE, deprecationHandler);
    }

    public static QueryShardContext.RewriteOverride parseRewriteOverride(
        @Nullable String rewrite_override,
        @Nullable QueryShardContext.RewriteOverride defaultRewriteOverride,
        DeprecationHandler deprecationHandler
    ) {
        if (rewrite_override == null) {
            return defaultRewriteOverride;
        }
        if (INDEX_OR_DOC_VALUES.match(rewrite_override, deprecationHandler)) {
            return QueryShardContext.RewriteOverride.INDEX_OR_DOC_VALUES;
        }
        if (INDEX_ONLY.match(rewrite_override, deprecationHandler)) {
            return QueryShardContext.RewriteOverride.INDEX_ONLY;
        }
        if (DOC_VALUES_ONLY.match(rewrite_override, deprecationHandler)) {
            return QueryShardContext.RewriteOverride.DOC_VALUES_ONLY;
        }
        throw new IllegalArgumentException("Failed to parse rewrite_override [" + rewrite_override + "]");

    }

    public static MultiTermQuery.RewriteMethod parseRewriteMethod(
        @Nullable String rewriteMethod,
        @Nullable MultiTermQuery.RewriteMethod defaultRewriteMethod,
        DeprecationHandler deprecationHandler
    ) {
        if (rewriteMethod == null) {
            return defaultRewriteMethod;
        }
        if (CONSTANT_SCORE.match(rewriteMethod, deprecationHandler)) {
            return MultiTermQuery.CONSTANT_SCORE_REWRITE;
        }
        if (SCORING_BOOLEAN.match(rewriteMethod, deprecationHandler)) {
            return MultiTermQuery.SCORING_BOOLEAN_REWRITE;
        }
        if (CONSTANT_SCORE_BOOLEAN.match(rewriteMethod, deprecationHandler)) {
            return MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE;
        }

        int firstDigit = -1;
        for (int i = 0; i < rewriteMethod.length(); ++i) {
            if (Character.isDigit(rewriteMethod.charAt(i))) {
                firstDigit = i;
                break;
            }
        }

        if (firstDigit >= 0) {
            final int size = Integer.parseInt(rewriteMethod.substring(firstDigit));
            String rewriteMethodName = rewriteMethod.substring(0, firstDigit);

            if (TOP_TERMS.match(rewriteMethodName, deprecationHandler)) {
                return new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(size);
            }
            if (TOP_TERMS_BOOST.match(rewriteMethodName, deprecationHandler)) {
                return new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(size);
            }
            if (TOP_TERMS_BLENDED_FREQS.match(rewriteMethodName, deprecationHandler)) {
                return new MultiTermQuery.TopTermsBlendedFreqScoringRewrite(size);
            }
        }

        throw new IllegalArgumentException("Failed to parse rewrite_method [" + rewriteMethod + "]");
    }
}

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

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;

import java.io.IOException;
import java.util.Map;

/**
 * Internally used for custom scoring
 *
 * @opensearch.internal
 */
public final class CustomQueryScorer extends QueryScorer {

    public CustomQueryScorer(Query query, IndexReader reader, String field, String defaultField) {
        super(query, reader, field, defaultField);
    }

    public CustomQueryScorer(Query query, IndexReader reader, String field) {
        super(query, reader, field);
    }

    public CustomQueryScorer(Query query, String field, String defaultField) {
        super(query, field, defaultField);
    }

    public CustomQueryScorer(Query query, String field) {
        super(query, field);
    }

    public CustomQueryScorer(Query query) {
        super(query);
    }

    public CustomQueryScorer(WeightedSpanTerm[] weightedTerms) {
        super(weightedTerms);
    }

    @Override
    protected WeightedSpanTermExtractor newTermExtractor(String defaultField) {
        return defaultField == null ? new CustomWeightedSpanTermExtractor() : new CustomWeightedSpanTermExtractor(defaultField);
    }

    private static class CustomWeightedSpanTermExtractor extends WeightedSpanTermExtractor {

        CustomWeightedSpanTermExtractor() {
            super();
        }

        CustomWeightedSpanTermExtractor(String defaultField) {
            super(defaultField);
        }

        @Override
        protected void extractUnknownQuery(Query query, Map<String, WeightedSpanTerm> terms) throws IOException {
            if (terms.isEmpty()) {
                extractWeightedTerms(terms, query, 1F);
            }
        }

        protected void extract(Query query, float boost, Map<String, WeightedSpanTerm> terms) throws IOException {
            if (isChildOrParentQuery(query.getClass())) {
                // skip has_child or has_parent queries, see: https://github.com/elastic/elasticsearch/issues/14999
                return;
            } else if (query instanceof FunctionScoreQuery) {
                super.extract(((FunctionScoreQuery) query).getSubQuery(), boost, terms);
            } else if (query instanceof OpenSearchToParentBlockJoinQuery) {
                super.extract(((OpenSearchToParentBlockJoinQuery) query).getChildQuery(), boost, terms);
            } else if (query instanceof IndexOrDocValuesQuery) {
                super.extract(((IndexOrDocValuesQuery) query).getIndexQuery(), boost, terms);
            } else {
                super.extract(query, boost, terms);
            }
        }

        /**
         * Workaround to detect parent/child query
         */
        private static final String PARENT_CHILD_QUERY_NAME = "HasChildQueryBuilder$LateParsingQuery";

        private static boolean isChildOrParentQuery(Class<?> clazz) {
            return clazz.getName().endsWith(PARENT_CHILD_QUERY_NAME);
        }
    }
}

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

package org.opensearch.join.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.MaxScoreCollector;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.join.mapper.ParentIdFieldMapper;
import org.opensearch.join.mapper.ParentJoinFieldMapper;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.fetch.subphase.InnerHitsContext.intersect;

class ParentChildInnerHitContextBuilder extends InnerHitContextBuilder {
    private final String typeName;
    private final boolean fetchChildInnerHits;

    ParentChildInnerHitContextBuilder(
        String typeName,
        boolean fetchChildInnerHits,
        QueryBuilder query,
        InnerHitBuilder innerHitBuilder,
        Map<String, InnerHitContextBuilder> children
    ) {
        super(query, innerHitBuilder, children);
        this.typeName = typeName;
        this.fetchChildInnerHits = fetchChildInnerHits;
    }

    @Override
    protected void doBuild(SearchContext context, InnerHitsContext innerHitsContext) throws IOException {
        QueryShardContext queryShardContext = context.getQueryShardContext();
        ParentJoinFieldMapper joinFieldMapper = ParentJoinFieldMapper.getMapper(context.mapperService());
        if (joinFieldMapper != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : typeName;
            JoinFieldInnerHitSubContext joinFieldInnerHits = new JoinFieldInnerHitSubContext(
                name,
                context,
                typeName,
                fetchChildInnerHits,
                joinFieldMapper
            );
            setupInnerHitsContext(queryShardContext, joinFieldInnerHits);
            innerHitsContext.addInnerHitDefinition(joinFieldInnerHits);
        } else {
            if (innerHitBuilder.isIgnoreUnmapped() == false) {
                throw new IllegalStateException("no join field has been configured");
            }
        }
    }

    static final class JoinFieldInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        private final String typeName;
        private final boolean fetchChildInnerHits;
        private final ParentJoinFieldMapper joinFieldMapper;

        JoinFieldInnerHitSubContext(
            String name,
            SearchContext context,
            String typeName,
            boolean fetchChildInnerHits,
            ParentJoinFieldMapper joinFieldMapper
        ) {
            super(name, context);
            this.typeName = typeName;
            this.fetchChildInnerHits = fetchChildInnerHits;
            this.joinFieldMapper = joinFieldMapper;
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) throws IOException {
            Weight innerHitQueryWeight = getInnerHitQueryWeight();
            String joinName = getSortedDocValue(joinFieldMapper.name(), context, hit.docId());
            if (joinName == null) {
                return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
            }

            QueryShardContext qsc = context.getQueryShardContext();
            ParentIdFieldMapper parentIdFieldMapper = joinFieldMapper.getParentIdFieldMapper(typeName, fetchChildInnerHits == false);
            if (parentIdFieldMapper == null) {
                return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
            }

            Query q;
            if (fetchChildInnerHits) {
                Query hitQuery = parentIdFieldMapper.fieldType().termQuery(hit.getId(), qsc);
                q = new BooleanQuery.Builder()
                    // Only include child documents that have the current hit as parent:
                    .add(hitQuery, BooleanClause.Occur.FILTER)
                    // and only include child documents of a single relation:
                    .add(joinFieldMapper.fieldType().termQuery(typeName, qsc), BooleanClause.Occur.FILTER)
                    .build();
            } else {
                String parentId = getSortedDocValue(parentIdFieldMapper.name(), context, hit.docId());
                if (parentId == null) {
                    return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
                }
                q = context.mapperService().fieldType(IdFieldMapper.NAME).termQuery(parentId, qsc);
            }

            Weight weight = context.searcher().createWeight(context.searcher().rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1f);
            if (size() == 0) {
                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                    intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                }
                return new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(totalHitCountCollector.getTotalHits(), TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                    Float.NaN
                );
            } else {
                int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                TopDocsCollector<?> topDocsCollector;
                MaxScoreCollector maxScoreCollector = null;
                if (sort() != null) {
                    topDocsCollector = new TopFieldCollectorManager(sort().sort, topN, null, Integer.MAX_VALUE, false).newCollector();
                    if (trackScores()) {
                        maxScoreCollector = new MaxScoreCollector();
                    }
                } else {
                    topDocsCollector = new TopScoreDocCollectorManager(topN, null, Integer.MAX_VALUE).newCollector();
                    maxScoreCollector = new MaxScoreCollector();
                }
                for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                    intersect(weight, innerHitQueryWeight, MultiCollector.wrap(topDocsCollector, maxScoreCollector), ctx);
                }
                TopDocs topDocs = topDocsCollector.topDocs(from(), size());
                float maxScore = Float.NaN;
                if (maxScoreCollector != null) {
                    maxScore = maxScoreCollector.getMaxScore();
                }
                return new TopDocsAndMaxScore(topDocs, maxScore);
            }
        }

        private String getSortedDocValue(String field, SearchContext context, int docId) {
            try {
                List<LeafReaderContext> ctxs = context.searcher().getIndexReader().leaves();
                LeafReaderContext ctx = ctxs.get(ReaderUtil.subIndex(docId, ctxs));
                SortedDocValues docValues = ctx.reader().getSortedDocValues(field);
                int segmentDocId = docId - ctx.docBase;
                if (docValues == null || docValues.advanceExact(segmentDocId) == false) {
                    return null;
                }
                int ord = docValues.ordValue();
                BytesRef joinName = docValues.lookupOrd(ord);
                return joinName.utf8ToString();
            } catch (IOException e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }

    }

}

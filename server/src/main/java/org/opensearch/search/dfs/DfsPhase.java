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

package org.opensearch.search.dfs;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermStatistics;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.RescoreContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Dfs phase of a search request, used to make scoring 100% accurate by collecting additional info from each shard before the query phase.
 * The additional information is used to better compare the scores coming from all the shards, which depend on local factors (e.g. idf)
 *
 * @opensearch.internal
 */
public class DfsPhase {

    public void execute(SearchContext context) {
        try {
            Map<String, CollectionStatistics> fieldStatistics = new HashMap<>();
            Map<Term, TermStatistics> stats = new HashMap<>();
            IndexSearcher searcher = new IndexSearcher(context.searcher().getIndexReader()) {
                @Override
                public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
                    if (context.isCancelled()) {
                        throw new TaskCancelledException("cancelled task with reason: " + context.getTask().getReasonCancelled());
                    }
                    TermStatistics ts = super.termStatistics(term, docFreq, totalTermFreq);
                    if (ts != null) {
                        stats.put(term, ts);
                    }
                    return ts;
                }

                @Override
                public CollectionStatistics collectionStatistics(String field) throws IOException {
                    if (context.isCancelled()) {
                        throw new TaskCancelledException("cancelled task with reason: " + context.getTask().getReasonCancelled());
                    }
                    CollectionStatistics cs = super.collectionStatistics(field);
                    if (cs != null) {
                        fieldStatistics.put(field, cs);
                    }
                    return cs;
                }
            };

            searcher.createWeight(context.searcher().rewrite(context.query()), ScoreMode.COMPLETE, 1);
            for (RescoreContext rescoreContext : context.rescore()) {
                for (ParsedQuery parsedQuery : rescoreContext.getParsedQueries()) {
                    searcher.createWeight(context.searcher().rewrite(parsedQuery.query()), ScoreMode.COMPLETE, 1);
                }
            }

            Term[] terms = stats.keySet().toArray(new Term[0]);
            TermStatistics[] termStatistics = new TermStatistics[terms.length];
            for (int i = 0; i < terms.length; i++) {
                termStatistics[i] = stats.get(terms[i]);
            }

            context.dfsResult()
                .termsStatistics(terms, termStatistics)
                .fieldStatistics(fieldStatistics)
                .maxDoc(context.searcher().getIndexReader().maxDoc());
        } catch (Exception e) {
            throw new DfsPhaseExecutionException(context.shardTarget(), "Exception during dfs phase", e);
        }
    }

}

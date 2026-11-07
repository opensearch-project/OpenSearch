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

package org.opensearch.search.rescore;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RescoreProcessor of a search request, used to run potentially expensive scoring models against the top matching documents.
 *
 * @opensearch.internal
 */
public class RescoreProcessor {

    public void process(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs().topDocs;
        if (topDocs.scoreDocs.length == 0) {
            return;
        }
        assert context.sort() == null : "rescore requires the sort by score";
        try {
            // The collector kept the full rescore window, but the coordinator uses only the top from + size hits of each shard.
            TopDocsAndMaxScore rescored = rescore(topDocs, context.searcher(), context.rescore(), context.from() + context.size());
            context.queryResult().topDocs(rescored, context.queryResult().sortValueFormats());
        } catch (IOException e) {
            throw new OpenSearchException("Rescore Phase Failed", e);
        }
    }

    /**
     * Runs each rescorer on all of the given docs, then keeps only the top {@code numDocs} and their rescored doc ids.
     */
    public static TopDocsAndMaxScore rescore(TopDocs topDocs, IndexSearcher searcher, List<RescoreContext> rescore, int numDocs)
        throws IOException {
        for (RescoreContext ctx : rescore) {
            topDocs = ctx.rescorer().rescore(topDocs, searcher, ctx);
            // It is the responsibility of the rescorer to sort the resulted top docs,
            // here we only assert that this condition is met.
            assert topDocsSortedByScore(topDocs) : "topdocs should be sorted after rescore";
        }
        // Before the cut, because the cut can remove all docs.
        float maxScore = topDocs.scoreDocs.length > 0 ? topDocs.scoreDocs[0].score : Float.NaN;
        TopDocs top = topN(topDocs, numDocs);
        // The rescored doc ids go to the coordinator and back, and the fetch phase loads only the top docs.
        for (RescoreContext ctx : rescore) {
            Set<Integer> rescoredDocs = ctx.getRescoredDocs();
            if (rescoredDocs != null) {
                Set<Integer> topRescoredDocs = new HashSet<>();
                for (ScoreDoc doc : top.scoreDocs) {
                    if (rescoredDocs.contains(doc.doc)) {
                        topRescoredDocs.add(doc.doc);
                    }
                }
                ctx.setRescoredDocs(topRescoredDocs);
            }
        }
        return new TopDocsAndMaxScore(top, maxScore);
    }

    /**
     * Returns a new {@link TopDocs} with the top {@code n} docs of {@code in}, or {@code in} if it has {@code n} docs or fewer.
     */
    static TopDocs topN(TopDocs in, int n) {
        if (in.scoreDocs.length <= n) {
            return in;
        }
        return new TopDocs(in.totalHits, Arrays.copyOf(in.scoreDocs, n));
    }

    /**
     * Returns true if the provided docs are sorted by score.
     */
    private static boolean topDocsSortedByScore(TopDocs topDocs) {
        if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length < 2) {
            return true;
        }
        float lastScore = topDocs.scoreDocs[0].score;
        for (int i = 1; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc doc = topDocs.scoreDocs[i];
            if (Float.compare(doc.score, lastScore) > 0) {
                return false;
            }
            lastScore = doc.score;
        }
        return true;
    }
}

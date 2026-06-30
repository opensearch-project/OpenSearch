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

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;

/**
 * Fetches the score of a query match during search phase
 *
 * @opensearch.internal
 */
public class FetchScorePhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) throws IOException {
        if (context.fetchScores() == false) {
            return null;
        }
        final IndexSearcher searcher = context.searcher();
        final Weight weight = searcher.createWeight(searcher.rewrite(context.query()), ScoreMode.COMPLETE, 1);
        return new FetchSubPhaseProcessor() {

            Scorer scorer;

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                ScorerSupplier scorerSupplier = weight.scorerSupplier(readerContext);
                if (scorerSupplier == null) {
                    throw new IllegalStateException("Can't compute score on document as it doesn't match the query");
                }
                scorer = scorerSupplier.get(1L); // random-access
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                if (scorer == null || scorer.iterator().advance(hitContext.docId()) != hitContext.docId()) {
                    throw new IllegalStateException("Can't compute score on document " + hitContext + " as it doesn't match the query");
                }
                hitContext.hit().score(scorer.score());
            }
        };
    }
}

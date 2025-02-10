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

package org.opensearch.search.slice;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.opensearch.common.util.BitMixer;

import java.io.IOException;

/**
 * A {@link SliceQuery} that uses the numeric doc values of a field to do the slicing.
 * <p>
 * <b>NOTE</b>: With deterministic field values this query can be used across different readers safely.
 * If updates are accepted on the field you must ensure that the same reader is used for all `slice` queries.
 *
 * @opensearch.internal
 */
public final class DocValuesSliceQuery extends SliceQuery {
    public DocValuesSliceQuery(String field, int id, int max) {
        super(field, id, max);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), getField());
                final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

                    @Override
                    public boolean matches() throws IOException {
                        if (values.advanceExact(approximation.docID())) {
                            for (int i = 0; i < values.docValueCount(); i++) {
                                if (contains(BitMixer.mix(values.nextValue()))) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            return contains(0);
                        }
                    }

                    @Override
                    public float matchCost() {
                        // BitMixer.mix seems to be about 10 ops
                        return 10;
                    }
                };
                final Scorer scorer = new ConstantScoreScorer(score(), scoreMode, twoPhase);
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, getField());
            }

        };
    }
}

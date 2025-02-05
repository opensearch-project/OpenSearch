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

package org.opensearch.search.suggest.phrase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.io.IOException;

/**
 * naive backoff scorer
 *
 * @opensearch.internal
 */
class StupidBackoffScorer extends WordScorer {
    private final double discount;

    StupidBackoffScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator, double discount)
        throws IOException {
        super(reader, terms, field, realWordLikelihood, separator);
        this.discount = discount;
    }

    double discount() {
        return this.discount;
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        join(separator, spare, w_1.term, word.term);
        final long count = frequency(spare.get());
        if (count < 1) {
            return discount * scoreUnigram(word);
        }
        return count / (w_1.termStats.totalTermFreq() + 0.00000000001d);
    }

    @Override
    protected double scoreTrigram(Candidate w, Candidate w_1, Candidate w_2) throws IOException {
        // First see if there are bigrams. If there aren't then skip looking up the trigram. This saves lookups
        // when the bigrams and trigrams are rare and we need both anyway.
        join(separator, spare, w_1.term, w.term);
        long bigramCount = frequency(spare.get());
        if (bigramCount < 1) {
            return discount * scoreUnigram(w);
        }
        join(separator, spare, w_2.term, w_1.term, w.term);
        long trigramCount = frequency(spare.get());
        if (trigramCount < 1) {
            return discount * (bigramCount / (w_1.termStats.totalTermFreq() + 0.00000000001d));
        }
        return trigramCount / (bigramCount + 0.00000000001d);
    }

}

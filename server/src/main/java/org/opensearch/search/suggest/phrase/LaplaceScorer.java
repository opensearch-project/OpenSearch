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
 * Scorer implementation based on a laplace computation
 *
 * @opensearch.internal
 */
final class LaplaceScorer extends WordScorer {
    private double alpha;

    LaplaceScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator, double alpha)
        throws IOException {
        super(reader, terms, field, realWordLikelihood, separator);
        this.alpha = alpha;
    }

    double alpha() {
        return this.alpha;
    }

    @Override
    protected double scoreUnigram(Candidate word) throws IOException {
        return (alpha + frequency(word.term)) / (vocabluarySize + alpha * numTerms);
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        join(separator, spare, w_1.term, word.term);
        return (alpha + frequency(spare.get())) / (w_1.termStats.totalTermFreq() + alpha * numTerms);
    }

    @Override
    protected double scoreTrigram(Candidate word, Candidate w_1, Candidate w_2) throws IOException {
        join(separator, spare, w_2.term, w_1.term, word.term);
        long trigramCount = frequency(spare.get());
        join(separator, spare, w_1.term, word.term);
        return (alpha + trigramCount) / (frequency(spare.get()) + alpha * numTerms);
    }

}

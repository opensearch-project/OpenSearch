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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A minimum score collector.
 *
 * @opensearch.internal
 */
public class MinimumScoreCollector extends FilterCollector {

    private final float minimumScore;

    public MinimumScoreCollector(Collector collector, float minimumScore) {
        super(collector);
        this.minimumScore = minimumScore;
    }

    public Collector getCollector() {
        return in;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return ScoreCachingWrappingScorer.wrap(new FilterLeafCollector(super.getLeafCollector(context)) {
            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
                in.setScorer(scorer);
            }

            @Override
            public void collect(int doc) throws IOException {
                if (scorer.score() >= minimumScore) {
                    in.collect(doc);
                }
            }
        });
    }

    @Override
    public void setWeight(Weight weight) {
        // Not redirecting to delegate collector to maintain same behaviour when this extended SimpleCollector.
    }

    @Override
    public ScoreMode scoreMode() {
        return in.scoreMode() == ScoreMode.TOP_SCORES ? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE;
    }
}

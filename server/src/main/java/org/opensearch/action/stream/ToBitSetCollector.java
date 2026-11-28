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
package org.opensearch.action.stream;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

public class ToBitSetCollector implements Collector {
    private final FixedBitSet[] sets;

    public ToBitSetCollector(FixedBitSet[] sets) {
        this.sets = sets;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final FixedBitSet set = new FixedBitSet(context.reader().maxDoc());
        this.sets[context.ord] = set;

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // No scoring needed
            }

            @Override
            public void collect(int docId) {
                set.set(docId);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

}

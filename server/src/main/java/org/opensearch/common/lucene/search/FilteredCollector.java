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

package org.opensearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.search.profile.query.ProfileWeight;

import java.io.IOException;

/**
 * A filtered collector.
 *
 * @opensearch.internal
 */
public class FilteredCollector implements Collector {

    private final Collector collector;
    private final Weight filter;

    public FilteredCollector(Collector collector, Weight filter) {
        this.collector = collector;
        this.filter = filter;
    }

    public Collector getCollector() {
        return collector;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (filter instanceof ProfileWeight) {
            ((ProfileWeight) filter).associateCollectorToLeaves(context, collector);
        }
        final ScorerSupplier filterScorerSupplier = filter.scorerSupplier(context);
        final LeafCollector in = collector.getLeafCollector(context);
        final Bits bits = Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);

        return new FilterLeafCollector(in) {
            @Override
            public void collect(int doc) throws IOException {
                if (bits.get(doc)) {
                    in.collect(doc);
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return collector.scoreMode();
    }
}

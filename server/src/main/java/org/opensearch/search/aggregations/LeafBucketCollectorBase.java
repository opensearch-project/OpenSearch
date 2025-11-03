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

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Scorable;
import org.opensearch.common.lucene.ScorerAware;

import java.io.IOException;

/**
 * A {@link LeafBucketCollector} that delegates all calls to the sub leaf
 * aggregator and sets the scorer on its source of values if it implements
 * {@link ScorerAware}.
 *
 * @opensearch.internal
 */
public class LeafBucketCollectorBase extends LeafBucketCollector {

    private final LeafBucketCollector sub;
    private final ScorerAware values;

    /**
     * @param sub    The leaf collector for sub aggregations.
     * @param values The values. {@link ScorerAware#setScorer} will be called automatically on them if they implement {@link ScorerAware}.
     */
    public LeafBucketCollectorBase(LeafBucketCollector sub, Object values) {
        this.sub = sub;
        if (values instanceof ScorerAware scorerAware) {
            this.values = scorerAware;
        } else {
            this.values = null;
        }
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        sub.setScorer(s);
        if (values != null) {
            values.setScorer(s);
        }
    }

    @Override
    public void collect(int doc, long bucket) throws IOException {
        sub.collect(doc, bucket);
    }

}

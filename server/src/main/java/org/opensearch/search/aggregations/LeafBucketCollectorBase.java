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

import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.lucene.ScorerAware;

import java.io.IOException;

/**
 * A {@link LeafBucketCollector} that delegates all calls to the sub leaf
 * aggregator and sets the scorer on its source of values if it implements
 * {@link ScorerAware}.
 *
 * <p>This class buffers doc IDs from {@link #collect(int)} (top-level bucket 0 path)
 * and flushes them in batches via {@link #collect(int[], int, long)}, enabling
 * aggregator implementations to perform bulk doc value retrieval. Aggregators that
 * need immediate per-doc feedback should use {@link LeafBucketCollector} directly.
 *
 * @opensearch.internal
 */
public class LeafBucketCollectorBase extends LeafBucketCollector {

    private static final int BUFFER_SIZE = 256;

    private final LeafBucketCollector sub;
    private final ScorerAware values;

    /** Whether collect(int) buffering is enabled */
    private final boolean bufferingEnabled;

    /** Buffer for batching doc IDs from collect(int) calls */
    private final int[] docBuffer = new int[BUFFER_SIZE];

    /** Number of doc IDs currently buffered */
    private int docCount;

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
        // Enable buffering only when we have doc values (which support random access)
        // and the values source doesn't need scores (ScorerAware).
        // Aggregators with null values (e.g., ScriptedMetricAggregator) or scorer-dependent
        // values are not safe for buffered replay.
        this.bufferingEnabled = (values != null && this.values == null);
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

    /**
     * Buffers doc IDs for top-level (bucket 0) collection and flushes via
     * {@link #collect(int[], int, long)} when the buffer is full. This enables
     * batch doc value retrieval for the per-doc Lucene scorer path.
     *
     * <p>When buffering is disabled (no doc values, or scorer-aware values),
     * falls back to direct collection.
     */
    @Override
    public void collect(int doc) throws IOException {
        if (bufferingEnabled) {
            docBuffer[docCount++] = doc;
            if (docCount == docBuffer.length) {
                docCount = 0;
                collect(docBuffer, docBuffer.length, 0);
            }
        } else {
            collect(doc, 0);
        }
    }

    /**
     * Flush any remaining buffered doc IDs from {@link #collect(int)} calls.
     */
    @Override
    public void finish() throws IOException {
        if (docCount > 0) {
            try {
                int count = docCount;
                docCount = 0;
                collect(docBuffer, count, 0);
            } catch (CollectionTerminatedException e) {
                // Safe to ignore — some aggregators may throw during finish
            }
        }
    }

}

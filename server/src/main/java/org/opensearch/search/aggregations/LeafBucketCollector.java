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

import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Per-leaf bucket collector.
 *
 * @opensearch.internal
 */
public abstract class LeafBucketCollector implements LeafCollector {

    public static final LeafBucketCollector NO_OP_COLLECTOR = new LeafBucketCollector() {
        @Override
        public void setScorer(Scorable arg0) throws IOException {
            // no-op
        }

        @Override
        public void collect(int doc, long bucket) {
            // no-op
        }
    };

    public static LeafBucketCollector wrap(Iterable<LeafBucketCollector> collectors) {
        final Stream<LeafBucketCollector> actualCollectors = StreamSupport.stream(collectors.spliterator(), false)
            .filter(c -> c != NO_OP_COLLECTOR);
        final LeafBucketCollector[] colls = actualCollectors.toArray(size -> new LeafBucketCollector[size]);
        switch (colls.length) {
            case 0:
                return NO_OP_COLLECTOR;
            case 1:
                return colls[0];
            default:
                return new LeafBucketCollector() {

                    @Override
                    public void setScorer(Scorable s) throws IOException {
                        for (LeafBucketCollector c : colls) {
                            c.setScorer(s);
                        }
                    }

                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        for (LeafBucketCollector c : colls) {
                            c.collect(doc, bucket);
                        }
                    }

                };
        }
    }

    /**
     * Collect the given {@code doc} in the bucket owned by
     * {@code owningBucketOrd}.
     * <p>
     * The implementation of this method metric aggregations is generally
     * something along the lines of
     * <pre>{@code
     * array[owningBucketOrd] += loadValueFromDoc(doc)
     * }</pre>
     * <p>Bucket aggregations have more trouble because their job is to
     * <strong>make</strong> new ordinals. So their implementation generally
     * looks kind of like
     * <pre>{@code
     * long myBucketOrd = mapOwningBucketAndValueToMyOrd(owningBucketOrd, loadValueFromDoc(doc));
     * collectBucket(doc, myBucketOrd);
     * }</pre>
     * <p>
     * Some bucket aggregations "know" how many ordinals each owning ordinal
     * needs so they can map "densely". The {@code range} aggregation, for
     * example, can perform this mapping with something like:
     * <pre>{@code
     * return rangeCount * owningBucketOrd + matchingRange(value);
     * }</pre>
     * Other aggregations don't know how many buckets will fall into any
     * particular owning bucket. The {@code terms} aggregation, for example,
     * uses {@link LongKeyedBucketOrds} which amounts to a hash lookup.
     */
    public abstract void collect(int doc, long owningBucketOrd) throws IOException;

    @Override
    public void collect(int doc) throws IOException {
        collect(doc, 0);
    }

    /**
     * Bulk-collect doc IDs within owningBucketOrd.
     *
     * <p>Note: The provided {@link DocIdStream} may be reused across calls and should be consumed
     * immediately.
     *
     * <p>Note: The provided {@link DocIdStream} typically holds all the docIds for the corresponding
     * owningBucketOrd. This method may be called multiple times per segment (but once per owningBucketOrd).
     *
     * <p>While the {@link DocIdStream} for each owningBucketOrd is sorted by docIds, it is NOT GUARANTEED
     * that doc IDs arrive in order across invocations for different owningBucketOrd.
     *
     * <p>It is NOT LEGAL for callers to mix calls to {@link #collect(DocIdStream, long)} and {@link
     * #collect(int, long)}.
     *
     * <p>The default implementation calls {@code stream.forEach(doc -> collect(doc, owningBucketOrd))}.
     */
    @ExperimentalApi
    public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
        stream.forEach(doc -> collect(doc, owningBucketOrd));
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // no-op by default
    }
}

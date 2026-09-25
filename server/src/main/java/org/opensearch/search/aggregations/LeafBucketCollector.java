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

import org.apache.lucene.index.DocValuesSkipper;
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
 * <p>This collector buffers doc IDs internally and flushes them in batches via
 * {@link #collect(int[], int, long)}. All entry points ({@link #collect(int)},
 * {@link #collect(DocIdStream, long)}, {@link #collectRange(int, int, long)}) funnel
 * through this batch path, enabling aggregator implementations to perform bulk doc value
 * retrieval using Lucene 10.4 APIs like {@code NumericDocValues#longValues}.
 *
 * @opensearch.internal
 */
public abstract class LeafBucketCollector implements LeafCollector {

    /** Default buffer size for batching doc IDs */
    private static final int DEFAULT_BUFFER_SIZE = 256;

    public static final LeafBucketCollector NO_OP_COLLECTOR = new LeafBucketCollector() {
        @Override
        public void setScorer(Scorable arg0) throws IOException {
            // no-op
        }

        @Override
        public void collect(int doc, long bucket) {
            // no-op
        }

        @Override
        public void collect(int[] docs, int count, long bucket) {
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

    /** Buffer for batching doc IDs before flushing to {@link #collect(int[], int, long)} */
    private final int[] docBuffer = new int[DEFAULT_BUFFER_SIZE];

    /** Number of doc IDs currently buffered */
    private int docCount;

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

    /**
     * Bulk-collect an array of doc IDs into the bucket owned by {@code owningBucketOrd}.
     *
     * <p>The {@code docs} array contains {@code count} doc IDs in ascending order.
     * Aggregator implementations should override this method to perform batch doc value
     * retrieval using APIs like {@code NumericDocValues#longValues(int, int[], long[], long)}.
     *
     * <p>The default implementation falls back to per-doc {@link #collect(int, long)} calls.
     */
    @ExperimentalApi
    public void collect(int[] docs, int count, long owningBucketOrd) throws IOException {
        for (int i = 0; i < count; i++) {
            collect(docs[i], owningBucketOrd);
        }
    }

    /**
     * Collect a single doc ID into bucket 0 (top-level aggregation).
     *
     * <p>The default implementation delegates to {@link #collect(int, long)} with bucket 0.
     */
    @Override
    public void collect(int doc) throws IOException {
        collect(doc, 0);
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
        collect(stream, 0);
    }

    /**
     * Collect a range of doc IDs, between {@code min} inclusive and {@code max} exclusive. {@code
     * max} is guaranteed to be greater than {@code min}.
     *
     * <p>Extending this method is typically useful to take advantage of pre-aggregated data exposed
     * in a {@link DocValuesSkipper}.
     *
     * <p>The default implementation delegates to {@link #collectRange(int, int, long)} with bucket 0.
     *
     * @see #collect(int,long)
     */
    @Override
    public void collectRange(int min, int max) throws IOException {
        collectRange(min, max, 0);
    }

    /**
     * Collect a range of doc IDs, between {@code min} inclusive and {@code max} exclusive, into the
     * bucket owned by {@code owningBucketOrd}. {@code max} is guaranteed to be greater than {@code min}.
     *
     * <p>The default implementation buffers doc IDs and flushes via {@link #collect(int[], int, long)},
     * enabling batch doc value retrieval in aggregator implementations.
     *
     * @see #collect(int[], int, long)
     */
    @ExperimentalApi
    public void collectRange(int min, int max, long owningBucketOrd) throws IOException {
        // Fill buffer from the range and flush in batches
        for (int docId = min; docId < max; docId++) {
            docBuffer[docCount++] = docId;
            if (docCount == docBuffer.length) {
                docCount = 0;
                collect(docBuffer, docBuffer.length, owningBucketOrd);
            }
        }
        flushBuffer(owningBucketOrd);
    }

    /**
     * Bulk-collect doc IDs within {@code owningBucketOrd}.
     *
     * <p>The default implementation uses {@link DocIdStream#intoArray} to batch doc IDs
     * and flushes via {@link #collect(int[], int, long)}, enabling batch doc value retrieval.
     *
     * <p>Note: The provided {@link DocIdStream} may be reused across calls and should be consumed immediately.
     *
     * <p>Note: The provided DocIdStream typically only holds a small subset of query matches. This method may be called multiple times
     * per segment. Like collect(int), it is guaranteed that doc IDs get collected in order, ie. doc IDs are collected in order within a
     * DocIdStream, and if called twice, all doc IDs from the second DocIdStream will be greater than all doc IDs from the first
     * DocIdStream.
     *
     * <p>It is legal for callers to mix calls to {@link #collect(DocIdStream, long)} and {@link #collect(int, long)}.
     */
    @ExperimentalApi
    public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
        // Use Lucene 10.4 intoArray to batch doc IDs directly into our buffer,
        // then flush via collect(int[], int, long) for batch doc value retrieval.
        for (int count = stream.intoArray(docBuffer); count != 0; count = stream.intoArray(docBuffer)) {
            collect(docBuffer, count, owningBucketOrd);
        }
    }

    /**
     * Flush any remaining buffered doc IDs.
     */
    private void flushBuffer(long owningBucketOrd) throws IOException {
        if (docCount > 0) {
            int count = docCount;
            docCount = 0;
            collect(docBuffer, count, owningBucketOrd);
        }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // no-op by default
    }
}

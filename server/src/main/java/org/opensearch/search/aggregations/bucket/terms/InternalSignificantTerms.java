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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the significant terms aggregation.
 *
 * @opensearch.internal
 */
public abstract class InternalSignificantTerms<A extends InternalSignificantTerms<A, B>, B extends InternalSignificantTerms.Bucket<B>>
    extends InternalMultiBucketAggregation<A, B>
    implements
        SignificantTerms {

    public static final String SCORE = "score";
    public static final String BG_COUNT = "bg_count";

    /**
     * Bucket for a significant terms agg
     *
     * @opensearch.internal
     */
    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    public abstract static class Bucket<B extends Bucket<B>> extends InternalMultiBucketAggregation.InternalBucket
        implements
            SignificantTerms.Bucket {
        /**
         * Reads a bucket. Should be a constructor reference.
         *
         * @opensearch.internal
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) throws IOException;
        }

        long subsetDf;
        long subsetSize;
        long supersetDf;
        long supersetSize;
        long bucketOrd;
        double score;
        protected InternalAggregations aggregations;
        final transient DocValueFormat format;

        protected Bucket(
            long subsetDf,
            long subsetSize,
            long supersetDf,
            long supersetSize,
            InternalAggregations aggregations,
            DocValueFormat format
        ) {
            this.subsetSize = subsetSize;
            this.supersetSize = supersetSize;
            this.subsetDf = subsetDf;
            this.supersetDf = supersetDf;
            this.aggregations = aggregations;
            this.format = format;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) {
            this.subsetSize = subsetSize;
            this.supersetSize = supersetSize;
            this.format = format;
        }

        @Override
        public long getSubsetDf() {
            return subsetDf;
        }

        @Override
        public long getSupersetDf() {
            return supersetDf;
        }

        @Override
        public long getSupersetSize() {
            return supersetSize;
        }

        @Override
        public long getSubsetSize() {
            return subsetSize;
        }

        // TODO we should refactor to remove this, since buckets should be immutable after they are generated.
        // This can lead to confusing bugs if the bucket is re-created (via createBucket() or similar) without
        // the score
        void updateScore(SignificanceHeuristic significanceHeuristic) {
            score = significanceHeuristic.getScore(subsetDf, subsetSize, supersetDf, supersetSize);
        }

        @Override
        public long getDocCount() {
            return subsetDf;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public double getSignificanceScore() {
            return score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Bucket<?> that = (Bucket<?>) o;
            return bucketOrd == that.bucketOrd
                && Double.compare(that.score, score) == 0
                && Objects.equals(aggregations, that.aggregations)
                && Objects.equals(format, that.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), bucketOrd, aggregations, score, format);
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            builder.field(SCORE, score);
            builder.field(BG_COUNT, supersetDf);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;
    }

    protected final int requiredSize;
    protected final long minDocCount;
    protected final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    protected InternalSignificantTerms(
        String name,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.requiredSize = bucketCountThresholds.getRequiredSize();
        this.minDocCount = bucketCountThresholds.getMinDocCount();
        this.bucketCountThresholds = bucketCountThresholds;
    }

    /**
     * Read from a stream.
     */
    protected InternalSignificantTerms(StreamInput in) throws IOException {
        super(in);
        requiredSize = readSize(in);
        minDocCount = in.readVLong();
        // shardMinDocCount and shardSize are not used on the coordinator, so they are not deserialized. We use
        // CoordinatorBucketCountThresholds which will throw an exception if they are accessed.
        bucketCountThresholds = new TermsAggregator.CoordinatorBucketCountThresholds(minDocCount, -1, requiredSize, -1);
    }

    protected final void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public abstract List<B> getBuckets();

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LocalBucketCountThresholds localBucketCountThresholds = reduceContext.asLocalBucketCountThresholds(bucketCountThresholds);
        long globalSubsetSize = 0;
        long globalSupersetSize = 0;
        // Compute the overall result set size and the corpus size using the
        // top-level Aggregations from each shard
        for (InternalAggregation aggregation : aggregations) {
            reduceContext.checkCancelled();
            @SuppressWarnings("unchecked")
            InternalSignificantTerms<A, B> terms = (InternalSignificantTerms<A, B>) aggregation;
            globalSubsetSize += terms.getSubsetSize();
            // supersetSize is a shard level count, if we sum it across slices we would produce num_slices_with_bucket * supersetSize where
            // num_slices_with_bucket is the number of segment slices that have collected a bucket for the key
            if (reduceContext.isSliceLevel()) {
                globalSupersetSize = terms.getSupersetSize();
            } else {
                globalSupersetSize += terms.getSupersetSize();
            }
        }
        Map<String, List<B>> buckets = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalSignificantTerms<A, B> terms = (InternalSignificantTerms<A, B>) aggregation;
            for (B bucket : terms.getBuckets()) {
                List<B> existingBuckets = buckets.get(bucket.getKeyAsString());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.getKeyAsString(), existingBuckets);
                }
                // Adjust the buckets with the global stats representing the
                // total size of the pots from which the stats are drawn
                existingBuckets.add(
                    createBucket(
                        bucket.getSubsetDf(),
                        globalSubsetSize,
                        bucket.getSupersetDf(),
                        globalSupersetSize,
                        bucket.aggregations,
                        bucket
                    )
                );
            }
        }
        SignificanceHeuristic heuristic = getSignificanceHeuristic().rewrite(reduceContext);
        boolean isCoordinatorPartialReduce = reduceContext.isFinalReduce() == false && reduceContext.isSliceLevel() == false;
        // Do not apply size threshold on coordinator partial reduce
        final int size = !isCoordinatorPartialReduce
            ? Math.min(localBucketCountThresholds.getRequiredSize(), buckets.size())
            : buckets.size();
        BucketSignificancePriorityQueue<B> ordered = new BucketSignificancePriorityQueue<>(size);
        for (Map.Entry<String, List<B>> entry : buckets.entrySet()) {
            List<B> sameTermBuckets = entry.getValue();
            final B b = reduceBucket(sameTermBuckets, reduceContext);
            b.updateScore(heuristic);
            // For concurrent search case we do not apply bucket count thresholds in buildAggregation and instead is done here during
            // reduce. However, the bucket score is only evaluated at the final coordinator reduce.
            boolean meetsThresholds = (b.subsetDf >= localBucketCountThresholds.getMinDocCount())
                && (((b.score > 0) || reduceContext.isSliceLevel()));
            if (isCoordinatorPartialReduce || meetsThresholds) {
                B removed = ordered.insertWithOverflow(b);
                if (removed == null) {
                    reduceContext.consumeBucketsAndMaybeBreak(1);
                } else {
                    reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
                }
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(b));
            }
        }
        B[] list = createBucketsArray(ordered.size());
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        return create(globalSubsetSize, globalSupersetSize, Arrays.asList(list));
    }

    @Override
    protected B reduceBucket(List<B> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        long subsetDf = 0;
        long supersetDf = 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (B bucket : buckets) {
            subsetDf += bucket.subsetDf;
            // supersetDf is a shard level count, if we sum it across slices we would produce num_slices_with_bucket * supersetSize where
            // num_slices_with_bucket is the number of segment slices that have collected a bucket for the key
            if (context.isSliceLevel()) {
                supersetDf = bucket.supersetDf;
            } else {
                supersetDf += bucket.supersetDf;
            }
            aggregationsList.add(bucket.aggregations);
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(subsetDf, buckets.get(0).subsetSize, supersetDf, buckets.get(0).supersetSize, aggs, buckets.get(0));
    }

    abstract B createBucket(
        long subsetDf,
        long subsetSize,
        long supersetDf,
        long supersetSize,
        InternalAggregations aggregations,
        B prototype
    );

    protected abstract A create(long subsetSize, long supersetSize, List<B> buckets);

    /**
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);

    protected abstract long getSubsetSize();

    protected abstract long getSupersetSize();

    protected abstract SignificanceHeuristic getSignificanceHeuristic();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minDocCount, requiredSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalSignificantTerms<?, ?> that = (InternalSignificantTerms<?, ?>) obj;
        return Objects.equals(minDocCount, that.minDocCount) && Objects.equals(requiredSize, that.requiredSize);
    }
}

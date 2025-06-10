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

import org.opensearch.OpenSearchException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder.Aggregation;
import org.opensearch.search.aggregations.InternalOrder.CompoundOrder;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregator;
import org.opensearch.search.aggregations.support.AggregationPath;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base aggregator class to aggregate documents by terms
 *
 * @opensearch.internal
 */
public abstract class TermsAggregator extends DeferableBucketAggregator {

    /**
     * Bucket count thresholds
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class BucketCountThresholds implements Writeable, ToXContentFragment {
        private long minDocCount;
        private long shardMinDocCount;
        private int requiredSize;
        private int shardSize;

        public BucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            this.minDocCount = minDocCount;
            this.shardMinDocCount = shardMinDocCount;
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        /**
         * Read from a stream.
         */
        public BucketCountThresholds(StreamInput in) throws IOException {
            requiredSize = in.readInt();
            shardSize = in.readInt();
            minDocCount = in.readLong();
            shardMinDocCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(requiredSize);
            out.writeInt(shardSize);
            out.writeLong(minDocCount);
            out.writeLong(shardMinDocCount);
        }

        public BucketCountThresholds(BucketCountThresholds bucketCountThresholds) {
            this(
                bucketCountThresholds.minDocCount,
                bucketCountThresholds.shardMinDocCount,
                bucketCountThresholds.requiredSize,
                bucketCountThresholds.shardSize
            );
        }

        public void ensureValidity() {

            // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return
            // <size>
            if (shardSize < requiredSize) {
                setShardSize(requiredSize);
            }

            // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match
            // the min_doc_count criteria
            if (shardMinDocCount > minDocCount) {
                setShardMinDocCount(minDocCount);
            }

            if (requiredSize <= 0 || shardSize <= 0) {
                throw new OpenSearchException("parameters [required_size] and [shard_size] must be >0 in terms aggregation.");
            }

            if (minDocCount < 0 || shardMinDocCount < 0) {
                throw new OpenSearchException("parameter [min_doc_count] and [shardMinDocCount] must be >=0 in terms aggregation.");
            }
        }

        public long getShardMinDocCount() {
            return shardMinDocCount;
        }

        public void setShardMinDocCount(long shardMinDocCount) {
            this.shardMinDocCount = shardMinDocCount;
        }

        public long getMinDocCount() {
            return minDocCount;
        }

        public void setMinDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
        }

        public int getRequiredSize() {
            return requiredSize;
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = requiredSize;
        }

        public int getShardSize() {
            return shardSize;
        }

        public void setShardSize(int shardSize) {
            this.shardSize = shardSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME.getPreferredName(), requiredSize);
            if (shardSize != -1) {
                builder.field(TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME.getPreferredName(), shardSize);
            }
            builder.field(TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), minDocCount);
            builder.field(TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), shardMinDocCount);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredSize, shardSize, minDocCount, shardMinDocCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BucketCountThresholds other = (BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize)
                && Objects.equals(shardSize, other.shardSize)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(shardMinDocCount, other.shardMinDocCount);
        }
    }

    /**
     * BucketCountThresholds type that throws an exception when shardMinDocCount or shardSize are accessed. This is used for
     * deserialization on the coordinator during reduce as shardMinDocCount and shardSize should not be accessed this way on the
     * coordinator.
     *
     * @opensearch.internal
     */
    public static class CoordinatorBucketCountThresholds extends BucketCountThresholds {

        public CoordinatorBucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            super(minDocCount, shardMinDocCount, requiredSize, shardSize);
        }

        @Override
        public long getShardMinDocCount() {
            throw new AggregationExecutionException("shard_min_doc_count should not be accessed via CoordinatorBucketCountThresholds");
        }

        @Override
        public int getShardSize() {
            throw new AggregationExecutionException("shard_size should not be accessed via CoordinatorBucketCountThresholds");
        }
    }

    protected final DocValueFormat format;
    protected final BucketCountThresholds bucketCountThresholds;
    protected final BucketOrder order;
    protected final Comparator<InternalTerms.Bucket<?>> partiallyBuiltBucketComparator;
    protected final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    protected final SubAggCollectionMode collectMode;

    public TermsAggregator(
        String name,
        AggregatorFactories factories,
        SearchContext context,
        Aggregator parent,
        BucketCountThresholds bucketCountThresholds,
        BucketOrder order,
        DocValueFormat format,
        SubAggCollectionMode collectMode,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order;
        partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        this.format = format;
        if (subAggsNeedScore() && descendsFromNestedAggregator(parent)) {
            /*
              Force the execution to depth_first because we need to access the score of
              nested documents in a sub-aggregation and we are not able to generate this score
              while replaying deferred documents.
             */
            this.collectMode = SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = collectMode;
        }
        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof Aggregation) {
            AggregationPath path = ((Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
        } else if (order instanceof CompoundOrder) {
            CompoundOrder compoundOrder = (CompoundOrder) order;
            for (BucketOrder orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof Aggregation) {
                    AggregationPath path = ((Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
                }
            }
        }
    }

    static boolean descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    private boolean subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        if (context.getQueryShardContext().getStarTreeQueryContext() == null) {
            return collectMode == SubAggCollectionMode.BREADTH_FIRST && !aggsUsedForSorting.contains(aggregator);
        } else {
            // when pre-computing using star-tree - return false (don't defer) for BREADTH_FIRST case
            return collectMode != SubAggCollectionMode.BREADTH_FIRST;
        }
    }
}

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

package org.opensearch.search.aggregations.bucket.filter;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of filters agg
 *
 * @opensearch.internal
 */
public class InternalFilters extends InternalMultiBucketAggregation<InternalFilters, InternalFilters.InternalBucket> implements Filters {
    /**
     * Internal bucket for an internal filters agg
     *
     * @opensearch.internal
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements Filters.Bucket {

        private final boolean keyed;
        private final String key;
        private long docCount;
        InternalAggregations aggregations;

        public InternalBucket(String key, long docCount, InternalAggregations aggregations, boolean keyed) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.keyed = keyed;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in, boolean keyed) throws IOException {
            this.keyed = keyed;
            key = in.readOptionalString();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (keyed) {
                builder.startObject(key);
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            InternalBucket that = (InternalBucket) other;
            return Objects.equals(key, that.key)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(docCount, that.docCount)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, keyed, docCount, aggregations);
        }
    }

    private final List<InternalBucket> buckets;
    private final boolean keyed;
    // bucketMap gets lazily initialized from buckets in getBucketByKey()
    private transient Map<String, InternalBucket> bucketMap;

    public InternalFilters(String name, List<InternalBucket> buckets, boolean keyed, Map<String, Object> metadata) {
        super(name, metadata);
        this.buckets = buckets;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    public InternalFilters(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        int size = in.readVInt();
        List<InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            buckets.add(new InternalBucket(in, keyed));
        }
        this.buckets = buckets;
        this.bucketMap = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeVInt(buckets.size());
        for (InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return FiltersAggregationBuilder.NAME;
    }

    @Override
    public InternalFilters create(List<InternalBucket> buckets) {
        return new InternalFilters(name, buckets, keyed, metadata);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.key, prototype.docCount, aggregations, prototype.keyed);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public InternalBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (InternalBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<List<InternalBucket>> bucketsList = null;
        for (InternalAggregation aggregation : aggregations) {
            reduceContext.checkCancelled();
            InternalFilters filters = (InternalFilters) aggregation;
            if (bucketsList == null) {
                bucketsList = new ArrayList<>(filters.buckets.size());
                for (InternalBucket bucket : filters.buckets) {
                    List<InternalBucket> sameRangeList = new ArrayList<>(aggregations.size());
                    sameRangeList.add(bucket);
                    bucketsList.add(sameRangeList);
                }
            } else {
                int i = 0;
                for (InternalBucket bucket : filters.buckets) {
                    bucketsList.get(i++).add(bucket);
                }
            }
        }

        reduceContext.consumeBucketsAndMaybeBreak(bucketsList.size());
        InternalFilters reduced = new InternalFilters(name, new ArrayList<>(bucketsList.size()), keyed, getMetadata());
        for (List<InternalBucket> sameRangeList : bucketsList) {
            reduced.buckets.add(reduceBucket(sameRangeList, reduceContext));
        }
        return reduced;
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        InternalBucket reduced = null;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (InternalBucket bucket : buckets) {
            if (reduced == null) {
                reduced = new InternalBucket(bucket.key, bucket.docCount, bucket.aggregations, bucket.keyed);
            } else {
                reduced.docCount += bucket.docCount;
            }
            aggregationsList.add(bucket.aggregations);
        }
        reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
        return reduced;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, keyed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalFilters that = (InternalFilters) obj;
        return Objects.equals(buckets, that.buckets) && Objects.equals(keyed, that.keyed);
    }

}

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

package org.opensearch.geo.search.aggregations.bucket.geogrid;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.util.LongObjectPagedHashMap;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Represents a grid of cells where each cell's location is determined by a specific geo hashing algorithm.
 * All geo-grid hash-encoding in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 *
 * @opensearch.api
 */
public abstract class BaseGeoGrid<B extends BaseGeoGridBucket> extends InternalMultiBucketAggregation<BaseGeoGrid, BaseGeoGridBucket>
    implements
        GeoGrid {

    protected final int requiredSize;
    protected final List<BaseGeoGridBucket> buckets;

    protected BaseGeoGrid(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata) {
        super(name, metadata);
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    protected abstract Reader<B> getBucketReader();

    /**
     * Read from a stream.
     */
    public BaseGeoGrid(StreamInput in) throws IOException {
        super(in);
        requiredSize = readSize(in);
        buckets = (List<BaseGeoGridBucket>) in.readList(getBucketReader());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeList(buckets);
    }

    protected abstract BaseGeoGrid create(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata);

    @Override
    public List<BaseGeoGridBucket> getBuckets() {
        return unmodifiableList(buckets);
    }

    @Override
    public BaseGeoGrid reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<BaseGeoGridBucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            BaseGeoGrid grid = (BaseGeoGrid) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }
            for (Object obj : grid.buckets) {
                BaseGeoGridBucket bucket = (BaseGeoGridBucket) obj;
                List<BaseGeoGridBucket> existingBuckets = buckets.get(bucket.hashAsLong());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.hashAsLong(), existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final int size = Math.toIntExact(reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size()));
        BucketPriorityQueue<BaseGeoGridBucket> ordered = new BucketPriorityQueue<>(size);
        for (LongObjectPagedHashMap.Cursor<List<BaseGeoGridBucket>> cursor : buckets) {
            List<BaseGeoGridBucket> sameCellBuckets = cursor.value;
            ordered.insertWithOverflow(reduceBucket(sameCellBuckets, reduceContext));
        }
        buckets.close();
        BaseGeoGridBucket[] list = new BaseGeoGridBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        reduceContext.consumeBucketsAndMaybeBreak(list.length);
        return create(getName(), requiredSize, Arrays.asList(list), getMetadata());
    }

    @Override
    protected BaseGeoGridBucket reduceBucket(List<BaseGeoGridBucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (BaseGeoGridBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregationsList.add(bucket.aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(buckets.get(0).hashAsLong, docCount, aggs);
    }

    protected abstract B createBucket(long hashAsLong, long docCount, InternalAggregations aggregations);

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (BaseGeoGridBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    // package protected for testing
    int getRequiredSize() {
        return requiredSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), requiredSize, buckets);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        BaseGeoGrid other = (BaseGeoGrid) obj;
        return Objects.equals(requiredSize, other.requiredSize) && Objects.equals(buckets, other.buckets);
    }

}

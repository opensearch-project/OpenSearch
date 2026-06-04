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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.Objects;

/**
 * Base implementation of geogrid aggs
 *
 * @opensearch.api
 */
public abstract class BaseGeoGridBucket<B extends BaseGeoGridBucket> extends InternalMultiBucketAggregation.InternalBucket
    implements
        GeoGrid.Bucket,
        Comparable<BaseGeoGridBucket> {

    protected long hashAsLong;
    protected long docCount;
    protected InternalAggregations aggregations;

    long bucketOrd;

    public BaseGeoGridBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        this.docCount = docCount;
        this.aggregations = aggregations;
        this.hashAsLong = hashAsLong;
    }

    /**
     * Read from a stream.
     */
    public BaseGeoGridBucket(StreamInput in) throws IOException {
        hashAsLong = in.readLong();
        docCount = in.readVLong();
        aggregations = InternalAggregations.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hashAsLong);
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    public long hashAsLong() {
        return hashAsLong;
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
    public int compareTo(BaseGeoGridBucket other) {
        if (this.hashAsLong > other.hashAsLong) {
            return 1;
        }
        if (this.hashAsLong < other.hashAsLong) {
            return -1;
        }
        return 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Aggregation.CommonFields.KEY.getPreferredName(), getKeyAsString());
        builder.field(Aggregation.CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseGeoGridBucket bucket = (BaseGeoGridBucket) o;
        return hashAsLong == bucket.hashAsLong && docCount == bucket.docCount && Objects.equals(aggregations, bucket.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashAsLong, docCount, aggregations);
    }

}

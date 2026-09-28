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

package org.opensearch.index.get;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for a search get
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetStats implements Writeable, ToXContentFragment {

    private long existsCount;
    private long existsTimeInMillis;
    private long missingCount;
    private long missingTimeInMillis;
    private long current;

    public GetStats() {}

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new GetStats object.
     * @param builder The builder instance containing all the values.
     */
    private GetStats(Builder builder) {
        this.existsCount = builder.existsCount;
        this.existsTimeInMillis = builder.existsTimeInMillis;
        this.missingCount = builder.missingCount;
        this.missingTimeInMillis = builder.missingTimeInMillis;
        this.current = builder.current;
    }

    public GetStats(StreamInput in) throws IOException {
        existsCount = in.readVLong();
        existsTimeInMillis = in.readVLong();
        missingCount = in.readVLong();
        missingTimeInMillis = in.readVLong();
        current = in.readVLong();
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public GetStats(long existsCount, long existsTimeInMillis, long missingCount, long missingTimeInMillis, long current) {
        this.existsCount = existsCount;
        this.existsTimeInMillis = existsTimeInMillis;
        this.missingCount = missingCount;
        this.missingTimeInMillis = missingTimeInMillis;
        this.current = current;
    }

    public void add(GetStats stats) {
        if (stats == null) {
            return;
        }
        current += stats.current;
        addTotals(stats);
    }

    public void addTotals(GetStats stats) {
        if (stats == null) {
            return;
        }
        existsCount += stats.existsCount;
        existsTimeInMillis += stats.existsTimeInMillis;
        missingCount += stats.missingCount;
        missingTimeInMillis += stats.missingTimeInMillis;
        current += stats.current;
    }

    public long getCount() {
        return existsCount + missingCount;
    }

    public long getTimeInMillis() {
        return existsTimeInMillis + missingTimeInMillis;
    }

    public TimeValue getTime() {
        return new TimeValue(getTimeInMillis());
    }

    public long getExistsCount() {
        return this.existsCount;
    }

    public long getExistsTimeInMillis() {
        return this.existsTimeInMillis;
    }

    public TimeValue getExistsTime() {
        return new TimeValue(existsTimeInMillis);
    }

    public long getMissingCount() {
        return this.missingCount;
    }

    public long getMissingTimeInMillis() {
        return this.missingTimeInMillis;
    }

    public TimeValue getMissingTime() {
        return new TimeValue(missingTimeInMillis);
    }

    public long current() {
        return this.current;
    }

    /**
     * Builder for the {@link GetStats} class.
     * Provides a fluent API for constructing a GetStats object.
     */
    public static class Builder {
        private long existsCount = 0;
        private long existsTimeInMillis = 0;
        private long missingCount = 0;
        private long missingTimeInMillis = 0;
        private long current = 0;

        public Builder() {}

        public Builder existsCount(long count) {
            this.existsCount = count;
            return this;
        }

        public Builder existsTimeInMillis(long time) {
            this.existsTimeInMillis = time;
            return this;
        }

        public Builder missingCount(long count) {
            this.missingCount = count;
            return this;
        }

        public Builder missingTimeInMillis(long time) {
            this.missingTimeInMillis = time;
            return this;
        }

        public Builder current(long current) {
            this.current = current;
            return this;
        }

        /**
         * Creates a {@link GetStats} object from the builder's current state.
         *
         * @return A new GetStats instance.
         */
        public GetStats build() {
            return new GetStats(this);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.GET);
        builder.field(Fields.TOTAL, getCount());
        builder.field(Fields.GET_TIME, Objects.toString(getTime()));
        builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, getTime());
        builder.field(Fields.EXISTS_TOTAL, existsCount);
        builder.humanReadableField(Fields.EXISTS_TIME_IN_MILLIS, Fields.EXISTS_TIME, getExistsTime());
        builder.field(Fields.MISSING_TOTAL, missingCount);
        builder.humanReadableField(Fields.MISSING_TIME_IN_MILLIS, Fields.MISSING_TIME, getMissingTime());
        builder.field(Fields.CURRENT, current);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for get statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String GET = "get";
        static final String TOTAL = "total";
        /**
         * Deprecated field name for time. Use {@link #TIME} instead.
         */
        @Deprecated(forRemoval = true)
        static final String GET_TIME = "getTime";
        static final String TIME = "time";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String EXISTS_TOTAL = "exists_total";
        static final String EXISTS_TIME = "exists_time";
        static final String EXISTS_TIME_IN_MILLIS = "exists_time_in_millis";
        static final String MISSING_TOTAL = "missing_total";
        static final String MISSING_TIME = "missing_time";
        static final String MISSING_TIME_IN_MILLIS = "missing_time_in_millis";
        static final String CURRENT = "current";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(existsCount);
        out.writeVLong(existsTimeInMillis);
        out.writeVLong(missingCount);
        out.writeVLong(missingTimeInMillis);
        out.writeVLong(current);
    }
}

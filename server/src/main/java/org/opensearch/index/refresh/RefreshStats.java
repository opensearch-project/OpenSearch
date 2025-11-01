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

package org.opensearch.index.refresh;

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
 * Encapsulates stats for index refresh
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RefreshStats implements Writeable, ToXContentFragment {

    private long total;

    private long totalTimeInMillis;

    private long externalTotal;

    private long externalTotalTimeInMillis;

    /**
     * Number of waiting refresh listeners.
     */
    private int listeners;

    public RefreshStats() {}

    public RefreshStats(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        externalTotal = in.readVLong();
        externalTotalTimeInMillis = in.readVLong();
        listeners = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(externalTotal);
        out.writeVLong(externalTotalTimeInMillis);
        out.writeVInt(listeners);
    }

    /**
     * This constructor will be deprecated starting in version 3.3.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public RefreshStats(long total, long totalTimeInMillis, long externalTotal, long externalTotalTimeInMillis, int listeners) {
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
        this.externalTotal = externalTotal;
        this.externalTotalTimeInMillis = externalTotalTimeInMillis;
        this.listeners = listeners;
    }

    public void add(RefreshStats refreshStats) {
        addTotals(refreshStats);
    }

    public void addTotals(RefreshStats refreshStats) {
        if (refreshStats == null) {
            return;
        }
        this.total += refreshStats.total;
        this.totalTimeInMillis += refreshStats.totalTimeInMillis;
        this.externalTotal += refreshStats.externalTotal;
        this.externalTotalTimeInMillis += refreshStats.externalTotalTimeInMillis;
        this.listeners += refreshStats.listeners;
    }

    /**
     * The total number of refresh executed.
     */
    public long getTotal() {
        return this.total;
    }

    /*
     * The total number of external refresh executed.
     */
    public long getExternalTotal() {
        return this.externalTotal;
    }

    /**
     * The total time spent executing refreshes (in milliseconds).
     */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time spent executing external refreshes (in milliseconds).
     */
    public long getExternalTotalTimeInMillis() {
        return this.externalTotalTimeInMillis;
    }

    /**
     * The total time refreshes have been executed.
     */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    /**
     * The total time external refreshes have been executed.
     */
    public TimeValue getExternalTotalTime() {
        return new TimeValue(externalTotalTimeInMillis);
    }

    /**
     * The number of waiting refresh listeners.
     */
    public int getListeners() {
        return listeners;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("refresh");
        builder.field("total", total);
        builder.humanReadableField("total_time_in_millis", "total_time", getTotalTime());
        builder.field("external_total", externalTotal);
        builder.humanReadableField("external_total_time_in_millis", "external_total_time", getExternalTotalTime());
        builder.field("listeners", listeners);
        builder.endObject();
        return builder;
    }

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new RefreshStats object.
     * @param builder The builder instance containing all the values.
     */
    private RefreshStats(Builder builder) {
        this.total = builder.total;
        this.totalTimeInMillis = builder.totalTimeInMillis;
        this.externalTotal = builder.externalTotal;
        this.externalTotalTimeInMillis = builder.externalTotalTimeInMillis;
        this.listeners = builder.listeners;
    }

    /**
     * Builder for the {@link RefreshStats} class.
     * Provides a fluent API for constructing instances.
     */
    public static class Builder {
        private long total = 0;
        private long totalTimeInMillis = 0;
        private long externalTotal = 0;
        private long externalTotalTimeInMillis = 0;
        private int listeners = 0;

        public Builder() {}

        public Builder total(long total) {
            this.total = total;
            return this;
        }

        public Builder totalTimeInMillis(long time) {
            this.totalTimeInMillis = time;
            return this;
        }

        public Builder externalTotal(long total) {
            this.externalTotal = total;
            return this;
        }

        public Builder externalTotalTimeInMillis(long time) {
            this.externalTotalTimeInMillis = time;
            return this;
        }

        public Builder listeners(int listeners) {
            this.listeners = listeners;
            return this;
        }

        /**
         * Creates a {@link RefreshStats} object from the builder's current state.
         * @return A new RefreshStats instance.
         */
        public RefreshStats build() {
            return new RefreshStats(this);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != RefreshStats.class) {
            return false;
        }
        RefreshStats rhs = (RefreshStats) obj;
        return total == rhs.total
            && totalTimeInMillis == rhs.totalTimeInMillis
            && externalTotal == rhs.externalTotal
            && externalTotalTimeInMillis == rhs.externalTotalTimeInMillis
            && listeners == rhs.listeners;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, totalTimeInMillis, externalTotal, externalTotalTimeInMillis, listeners);
    }
}

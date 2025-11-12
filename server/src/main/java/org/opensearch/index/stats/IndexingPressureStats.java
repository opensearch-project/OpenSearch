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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.stats;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Base indexing pressure statistics
 *
 * @opensearch.api
 */
@PublicApi(since = "1.3.0")
public class IndexingPressureStats implements Writeable, ToXContentFragment {

    private final long totalCombinedCoordinatingAndPrimaryBytes;
    private final long totalCoordinatingBytes;
    private final long totalPrimaryBytes;
    private final long totalReplicaBytes;

    private final long currentCombinedCoordinatingAndPrimaryBytes;
    private final long currentCoordinatingBytes;
    private final long currentPrimaryBytes;
    private final long currentReplicaBytes;
    private final long coordinatingRejections;
    private final long primaryRejections;
    private final long replicaRejections;
    private final long memoryLimit;

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new IndexingPressureStats object.
     * @param builder The builder instance containing all the values.
     */
    private IndexingPressureStats(Builder builder) {
        this.totalCombinedCoordinatingAndPrimaryBytes = builder.totalCombinedCoordinatingAndPrimaryBytes;
        this.totalCoordinatingBytes = builder.totalCoordinatingBytes;
        this.totalPrimaryBytes = builder.totalPrimaryBytes;
        this.totalReplicaBytes = builder.totalReplicaBytes;
        this.currentCombinedCoordinatingAndPrimaryBytes = builder.currentCombinedCoordinatingAndPrimaryBytes;
        this.currentCoordinatingBytes = builder.currentCoordinatingBytes;
        this.currentPrimaryBytes = builder.currentPrimaryBytes;
        this.currentReplicaBytes = builder.currentReplicaBytes;
        this.coordinatingRejections = builder.coordinatingRejections;
        this.primaryRejections = builder.primaryRejections;
        this.replicaRejections = builder.replicaRejections;
        this.memoryLimit = builder.memoryLimit;
    }

    public IndexingPressureStats(StreamInput in) throws IOException {
        totalCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        totalCoordinatingBytes = in.readVLong();
        totalPrimaryBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();

        currentCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        currentCoordinatingBytes = in.readVLong();
        currentPrimaryBytes = in.readVLong();
        currentReplicaBytes = in.readVLong();

        coordinatingRejections = in.readVLong();
        primaryRejections = in.readVLong();
        replicaRejections = in.readVLong();
        memoryLimit = in.readVLong();
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public IndexingPressureStats(
        long totalCombinedCoordinatingAndPrimaryBytes,
        long totalCoordinatingBytes,
        long totalPrimaryBytes,
        long totalReplicaBytes,
        long currentCombinedCoordinatingAndPrimaryBytes,
        long currentCoordinatingBytes,
        long currentPrimaryBytes,
        long currentReplicaBytes,
        long coordinatingRejections,
        long primaryRejections,
        long replicaRejections,
        long memoryLimit
    ) {
        this.totalCombinedCoordinatingAndPrimaryBytes = totalCombinedCoordinatingAndPrimaryBytes;
        this.totalCoordinatingBytes = totalCoordinatingBytes;
        this.totalPrimaryBytes = totalPrimaryBytes;
        this.totalReplicaBytes = totalReplicaBytes;
        this.currentCombinedCoordinatingAndPrimaryBytes = currentCombinedCoordinatingAndPrimaryBytes;
        this.currentCoordinatingBytes = currentCoordinatingBytes;
        this.currentPrimaryBytes = currentPrimaryBytes;
        this.currentReplicaBytes = currentReplicaBytes;
        this.coordinatingRejections = coordinatingRejections;
        this.primaryRejections = primaryRejections;
        this.replicaRejections = replicaRejections;
        this.memoryLimit = memoryLimit;
    }

    /**
     * Builder for the {@link IndexingPressureStats} class.
     * Provides a fluent API for constructing a IndexingPressureStats object.
     */
    public static class Builder {
        private long totalCombinedCoordinatingAndPrimaryBytes = 0;
        private long totalCoordinatingBytes = 0;
        private long totalPrimaryBytes = 0;
        private long totalReplicaBytes = 0;
        private long currentCombinedCoordinatingAndPrimaryBytes = 0;
        private long currentCoordinatingBytes = 0;
        private long currentPrimaryBytes = 0;
        private long currentReplicaBytes = 0;
        private long coordinatingRejections = 0;
        private long primaryRejections = 0;
        private long replicaRejections = 0;
        private long memoryLimit = 0;

        public Builder() {}

        public Builder totalCombinedCoordinatingAndPrimaryBytes(long bytes) {
            this.totalCombinedCoordinatingAndPrimaryBytes = bytes;
            return this;
        }

        public Builder totalCoordinatingBytes(long bytes) {
            this.totalCoordinatingBytes = bytes;
            return this;
        }

        public Builder totalPrimaryBytes(long bytes) {
            this.totalPrimaryBytes = bytes;
            return this;
        }

        public Builder totalReplicaBytes(long bytes) {
            this.totalReplicaBytes = bytes;
            return this;
        }

        public Builder currentCombinedCoordinatingAndPrimaryBytes(long bytes) {
            this.currentCombinedCoordinatingAndPrimaryBytes = bytes;
            return this;
        }

        public Builder currentCoordinatingBytes(long bytes) {
            this.currentCoordinatingBytes = bytes;
            return this;
        }

        public Builder currentPrimaryBytes(long bytes) {
            this.currentPrimaryBytes = bytes;
            return this;
        }

        public Builder currentReplicaBytes(long bytes) {
            this.currentReplicaBytes = bytes;
            return this;
        }

        public Builder coordinatingRejections(long rejections) {
            this.coordinatingRejections = rejections;
            return this;
        }

        public Builder primaryRejections(long rejections) {
            this.primaryRejections = rejections;
            return this;
        }

        public Builder replicaRejections(long rejections) {
            this.replicaRejections = rejections;
            return this;
        }

        public Builder memoryLimit(long limit) {
            this.memoryLimit = limit;
            return this;
        }

        /**
         * Creates a {@link IndexingPressureStats} object from the builder's current state.
         * @return A new IndexingPressureStats instance.
         */
        public IndexingPressureStats build() {
            return new IndexingPressureStats(this);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(totalCoordinatingBytes);
        out.writeVLong(totalPrimaryBytes);
        out.writeVLong(totalReplicaBytes);

        out.writeVLong(currentCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(currentCoordinatingBytes);
        out.writeVLong(currentPrimaryBytes);
        out.writeVLong(currentReplicaBytes);

        out.writeVLong(coordinatingRejections);
        out.writeVLong(primaryRejections);
        out.writeVLong(replicaRejections);
        out.writeVLong(memoryLimit);
    }

    public long getTotalCombinedCoordinatingAndPrimaryBytes() {
        return totalCombinedCoordinatingAndPrimaryBytes;
    }

    public long getTotalCoordinatingBytes() {
        return totalCoordinatingBytes;
    }

    public long getTotalPrimaryBytes() {
        return totalPrimaryBytes;
    }

    public long getTotalReplicaBytes() {
        return totalReplicaBytes;
    }

    public long getCurrentCombinedCoordinatingAndPrimaryBytes() {
        return currentCombinedCoordinatingAndPrimaryBytes;
    }

    public long getCurrentCoordinatingBytes() {
        return currentCoordinatingBytes;
    }

    public long getCurrentPrimaryBytes() {
        return currentPrimaryBytes;
    }

    public long getCurrentReplicaBytes() {
        return currentReplicaBytes;
    }

    public long getCoordinatingRejections() {
        return coordinatingRejections;
    }

    public long getPrimaryRejections() {
        return primaryRejections;
    }

    public long getReplicaRejections() {
        return replicaRejections;
    }

    private static final String COMBINED = "combined_coordinating_and_primary";
    private static final String COMBINED_IN_BYTES = "combined_coordinating_and_primary_in_bytes";
    private static final String COORDINATING = "coordinating";
    private static final String COORDINATING_IN_BYTES = "coordinating_in_bytes";
    private static final String PRIMARY = "primary";
    private static final String PRIMARY_IN_BYTES = "primary_in_bytes";
    private static final String REPLICA = "replica";
    private static final String REPLICA_IN_BYTES = "replica_in_bytes";
    private static final String ALL = "all";
    private static final String ALL_IN_BYTES = "all_in_bytes";
    private static final String COORDINATING_REJECTIONS = "coordinating_rejections";
    private static final String PRIMARY_REJECTIONS = "primary_rejections";
    private static final String REPLICA_REJECTIONS = "replica_rejections";
    private static final String LIMIT = "limit";
    private static final String LIMIT_IN_BYTES = "limit_in_bytes";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.startObject("memory");
        builder.startObject("current");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, new ByteSizeValue(currentCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(currentCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(currentPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(currentReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, new ByteSizeValue(currentReplicaBytes + currentCombinedCoordinatingAndPrimaryBytes));
        builder.endObject();
        builder.startObject("total");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, new ByteSizeValue(totalCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(totalCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(totalPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(totalReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, new ByteSizeValue(totalReplicaBytes + totalCombinedCoordinatingAndPrimaryBytes));
        builder.field(COORDINATING_REJECTIONS, coordinatingRejections);
        builder.field(PRIMARY_REJECTIONS, primaryRejections);
        builder.field(REPLICA_REJECTIONS, replicaRejections);
        builder.endObject();
        builder.humanReadableField(LIMIT_IN_BYTES, LIMIT, new ByteSizeValue(memoryLimit));
        builder.endObject();
        return builder.endObject();
    }
}

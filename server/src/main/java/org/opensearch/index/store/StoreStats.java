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

package org.opensearch.index.store;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Statistics about an OpenSearch Store
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class StoreStats implements Writeable, ToXContentFragment {

    /**
     * Sentinel value for cases where the shard does not yet know its reserved size so we must fall back to an estimate, for instance
     * prior to receiving the list of files in a peer recovery.
     */
    public static final long UNKNOWN_RESERVED_BYTES = -1L;
    private long sizeInBytes;
    private long reservedSize;

    public StoreStats() {

    }

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new StoreStats object.
     * @param builder The builder instance containing all the values.
     */
    private StoreStats(Builder builder) {
        this.sizeInBytes = builder.sizeInBytes;
        this.reservedSize = builder.reservedSize;
    }

    public StoreStats(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        reservedSize = in.readZLong();
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
      */
    @Deprecated
    public StoreStats(long sizeInBytes, long reservedSize) {
        assert reservedSize == UNKNOWN_RESERVED_BYTES || reservedSize >= 0 : reservedSize;
        this.sizeInBytes = sizeInBytes;
        this.reservedSize = reservedSize;
    }

    public void add(StoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;
        reservedSize = ignoreIfUnknown(reservedSize) + ignoreIfUnknown(stats.reservedSize);
    }

    private static long ignoreIfUnknown(long reservedSize) {
        return reservedSize == UNKNOWN_RESERVED_BYTES ? 0L : reservedSize;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    /**
     * A prediction of how much larger this store will eventually grow. For instance, if we are currently doing a peer recovery or restoring
     * a snapshot into this store then we can account for the rest of the recovery using this field. A value of {@code -1B} indicates that
     * the reserved size is unknown.
     */
    public ByteSizeValue getReservedSize() {
        return new ByteSizeValue(reservedSize);
    }

    /**
     * Builder for the {@link StoreStats} class.
     * Provides a fluent API for constructing a StoreStats object.
     */
    public static class Builder {
        private long sizeInBytes = 0;
        private long reservedSize = 0;

        public Builder() {}

        /**
         * @param bytes the size of the store in bytes
         */
        public Builder sizeInBytes(long bytes) {
            this.sizeInBytes = bytes;
            return this;
        }

        /**
         * @param size a prediction of how much larger the store is expected to grow, or {@link StoreStats#UNKNOWN_RESERVED_BYTES}.
         */
        public Builder reservedSize(long size) {
            assert size == UNKNOWN_RESERVED_BYTES || size >= 0 : size;
            this.reservedSize = size;
            return this;
        }

        /**
         * Creates a {@link StoreStats} object from the builder's current state.
         * @return A new StoreStats instance.
         */
        public StoreStats build() {
            return new StoreStats(this);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeZLong(reservedSize);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STORE);
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, size());
        builder.humanReadableField(Fields.RESERVED_IN_BYTES, Fields.RESERVED, getReservedSize());
        builder.endObject();
        return builder;
    }

    /**
     * Fields for store statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String STORE = "store";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String RESERVED = "reserved";
        static final String RESERVED_IN_BYTES = "reserved_in_bytes";
    }
}

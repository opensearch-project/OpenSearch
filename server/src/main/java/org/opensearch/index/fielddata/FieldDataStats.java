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

package org.opensearch.index.fielddata;

import org.opensearch.Version;
import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.FieldStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates heap usage for field data
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FieldDataStats implements Writeable, ToXContentFragment {

    private static final String FIELDDATA = "fielddata";
    private static final String MEMORY_SIZE = "memory_size";
    public static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
    private static final String EVICTIONS = "evictions";
    private static final String FIELDS = "fields";
    public static final String ITEM_COUNT = "item_count";
    private long memorySize;
    private long evictions;
    private long itemCount;

    @Nullable
    private FieldStats fieldStats;

    // Static values for constructing FieldStats objects
    static final List<String> ORDERED_STAT_NAMES = List.of(MEMORY_SIZE_IN_BYTES, ITEM_COUNT);
    static final Map<String, Boolean> IS_MEMORY = Map.of(MEMORY_SIZE_IN_BYTES, true, ITEM_COUNT, false);
    static final Map<String, String> READABLE_KEYS = Map.of(MEMORY_SIZE_IN_BYTES, MEMORY_SIZE);

    public FieldDataStats() {

    }

    public FieldDataStats(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
            itemCount = in.readVLong();
            fieldStats = in.readOptionalWriteable(FieldStats::new);
        } else {
            FieldMemoryStats deserializedFieldMemoryStats = in.readOptionalWriteable(FieldMemoryStats::new);
            if (deserializedFieldMemoryStats != null) {
                // Field memory stats will be populated from FieldMemoryStats, item stats will be 0
                fieldStats = FieldStats.fromSingleStat(
                    ORDERED_STAT_NAMES,
                    IS_MEMORY,
                    READABLE_KEYS,
                    deserializedFieldMemoryStats,
                    MEMORY_SIZE_IN_BYTES
                );
            } else {
                fieldStats = null;
            }
        }
    }

    /**
     * @deprecated Use the constructor with FieldStats instead.
     * @param memorySize the total memory size
     * @param evictions the number of evictions
     * @param fields the memory size for each field
     */
    @Deprecated
    public FieldDataStats(long memorySize, long evictions, @Nullable FieldMemoryStats fields) {
        // TODO: Remove usage of this ctor in tests?
        this(
            memorySize,
            evictions,
            0L,
            fields == null ? null : FieldStats.fromSingleStat(ORDERED_STAT_NAMES, IS_MEMORY, READABLE_KEYS, fields, MEMORY_SIZE_IN_BYTES)
        );
    }

    public FieldDataStats(long memorySize, long evictions, long itemCount, @Nullable FieldStats fieldStats) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.itemCount = itemCount;
        this.fieldStats = fieldStats;
    }

    public void add(FieldDataStats stats) {
        if (stats == null) {
            return;
        }
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        this.itemCount += stats.itemCount;
        if (stats.fieldStats != null) {
            if (this.fieldStats == null) {
                fieldStats = stats.fieldStats.copy();
            } else {
                fieldStats.add(stats.fieldStats);
            }
        }
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    public long getItemCount() {
        return this.itemCount;
    }

    /**
     * Get the field memory sizes.
     * Cannot change the method name as this class was marked PublicApi.
     * @return The memory field sizes
     */
    @Nullable
    public FieldMemoryStats getFields() {
        if (fieldStats == null) return null;
        return fieldStats.toFieldMemoryStats(MEMORY_SIZE_IN_BYTES);
    }

    /*@Nullable
    public FieldMemoryStats getFieldItemCounts() {
        return fieldItemCounts;
    }*/

    @Nullable
    public FieldStats getFieldStats() {
        return fieldStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            out.writeVLong(itemCount);
            out.writeOptionalWriteable(fieldStats);
        } else {
            out.writeOptionalWriteable(fieldStats.toFieldMemoryStats(MEMORY_SIZE_IN_BYTES));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELDDATA);
        builder.humanReadableField(MEMORY_SIZE_IN_BYTES, MEMORY_SIZE, getMemorySize());
        builder.field(EVICTIONS, getEvictions());
        builder.field(ITEM_COUNT, getItemCount());
        if (fieldStats != null) {
            fieldStats.toXContent(builder, FIELDS);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldDataStats that = (FieldDataStats) o;
        return memorySize == that.memorySize
            && evictions == that.evictions
            && itemCount == that.itemCount
            && Objects.equals(fieldStats, that.fieldStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySize, evictions, itemCount, fieldStats);
    }
}

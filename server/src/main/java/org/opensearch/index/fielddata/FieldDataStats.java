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
import org.opensearch.common.FieldCountStats;
import org.opensearch.common.FieldMemoryStats;
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
import java.util.Objects;
import java.util.Set;

/**
 * Encapsulates heap usage for field data
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FieldDataStats implements Writeable, ToXContentFragment {

    private static final String FIELDDATA = "fielddata";
    private static final String MEMORY_SIZE = "memory_size";
    private static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
    private static final String EVICTIONS = "evictions";
    private static final String FIELDS = "fields";
    private static final String ITEM_COUNT = "item_count";
    private long memorySize;
    private long evictions;
    private long itemCount;
    @Nullable
    private FieldMemoryStats fieldMemorySizes;
    @Nullable
    private FieldCountStats fieldItemCounts;

    private static final List<String> ORDERED_FIELD_LEVEL_API_RAW_KEYS = List.of(MEMORY_SIZE_IN_BYTES, ITEM_COUNT);
    private static final List<String> ORDERED_FIELD_LEVEL_API_READABLE_KEYS = List.of(MEMORY_SIZE, ITEM_COUNT);

    public FieldDataStats() {

    }

    public FieldDataStats(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        fieldMemorySizes = in.readOptionalWriteable(FieldMemoryStats::new);
        if (in.getVersion().onOrAfter(Version.V_3_4_0)) {
            itemCount = in.readVLong();
            fieldItemCounts = in.readOptionalWriteable(FieldCountStats::new);
        }
    }

    /**
     * @deprecated Use the constructor that includes item counts instead.
     * @param memorySize the total memory size
     * @param evictions the number of evictions
     * @param fields the memory size for each field
     */
    @Deprecated
    public FieldDataStats(long memorySize, long evictions, @Nullable FieldMemoryStats fields) {
        this(memorySize, evictions, fields, 0, null);
    }

    public FieldDataStats(
        long memorySize,
        long evictions,
        @Nullable FieldMemoryStats fieldMemorySizes,
        long itemCount,
        @Nullable FieldCountStats fieldItemCounts
    ) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fieldMemorySizes = fieldMemorySizes;
        this.itemCount = itemCount;
        this.fieldItemCounts = fieldItemCounts;
        if (fieldMemorySizes != null && fieldItemCounts != null) {
            assert fieldMemorySizes.getStats().keySet().equals(fieldItemCounts.getStats().keySet());
        }
    }

    public void add(FieldDataStats stats) {
        if (stats == null) {
            return;
        }
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        this.itemCount += stats.itemCount;
        if (stats.fieldMemorySizes != null) {
            if (fieldMemorySizes == null) {
                fieldMemorySizes = stats.fieldMemorySizes.copy();
            } else {
                fieldMemorySizes.add(stats.fieldMemorySizes);
            }
        }
        if (stats.fieldItemCounts != null) {
            if (fieldItemCounts == null) {
                fieldItemCounts = stats.fieldItemCounts.copy();
            } else {
                fieldItemCounts.add(stats.fieldItemCounts);
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
        return fieldMemorySizes;
    }

    @Nullable
    public FieldMemoryStats getFieldItemCounts() {
        return fieldItemCounts;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        out.writeOptionalWriteable(fieldMemorySizes);
        if (out.getVersion().onOrAfter(Version.V_3_4_0)) {
            out.writeVLong(itemCount);
            out.writeOptionalWriteable(fieldItemCounts);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELDDATA);
        builder.humanReadableField(MEMORY_SIZE_IN_BYTES, MEMORY_SIZE, getMemorySize());
        builder.field(EVICTIONS, getEvictions());
        builder.field(ITEM_COUNT, getItemCount());
        Set<String> fields = null;
        // We've already asserted the two key sets are the same if both are not null
        if (fieldMemorySizes != null) {
            fields = fieldMemorySizes.getStats().keySet();
        } else if (fieldItemCounts != null) {
            fields = fieldItemCounts.getStats().keySet();
        }
        if (fields != null) {
            FieldMemoryStats.toXContent(
                builder,
                fields,
                FIELDS,
                List.of(fieldMemorySizes, fieldItemCounts),
                ORDERED_FIELD_LEVEL_API_RAW_KEYS,
                ORDERED_FIELD_LEVEL_API_READABLE_KEYS
            );
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
            && Objects.equals(fieldMemorySizes, that.fieldMemorySizes)
            && itemCount == that.itemCount
            && Objects.equals(fieldItemCounts, that.fieldItemCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySize, evictions, fieldMemorySizes, itemCount, fieldItemCounts);
    }
}

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

import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates heap usage for field data
 *
 * @opensearch.internal
 */
public class FieldDataStats implements Writeable, ToXContentFragment {

    private static final String FIELDDATA = "fielddata";
    private static final String MEMORY_SIZE = "memory_size";
    private static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
    private static final String EVICTIONS = "evictions";
    private static final String FIELDS = "fields";
    private long memorySize;
    private long evictions;
    @Nullable
    private FieldMemoryStats fields;

    public FieldDataStats() {

    }

    public FieldDataStats(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        fields = in.readOptionalWriteable(FieldMemoryStats::new);
    }

    public FieldDataStats(long memorySize, long evictions, @Nullable FieldMemoryStats fields) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fields = fields;
    }

    public void add(FieldDataStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        if (stats.fields != null) {
            if (fields == null) {
                fields = stats.fields.copy();
            } else {
                fields.add(stats.fields);
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

    @Nullable
    public FieldMemoryStats getFields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        out.writeOptionalWriteable(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELDDATA);
        builder.humanReadableField(MEMORY_SIZE_IN_BYTES, MEMORY_SIZE, getMemorySize());
        builder.field(EVICTIONS, getEvictions());
        if (fields != null) {
            fields.toXContent(builder, FIELDS, MEMORY_SIZE_IN_BYTES, MEMORY_SIZE);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldDataStats that = (FieldDataStats) o;
        return memorySize == that.memorySize && evictions == that.evictions && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySize, evictions, fields);
    }
}

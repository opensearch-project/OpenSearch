/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.fielddata;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.ProtobufFieldMemoryStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
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
public class ProtobufFieldDataStats implements ProtobufWriteable, ToXContentFragment {

    private static final String FIELDDATA = "fielddata";
    private static final String MEMORY_SIZE = "memory_size";
    private static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
    private static final String EVICTIONS = "evictions";
    private static final String FIELDS = "fields";
    private long memorySize;
    private long evictions;
    @Nullable
    private ProtobufFieldMemoryStats fields;

    public ProtobufFieldDataStats() {

    }

    public ProtobufFieldDataStats(CodedInputStream in) throws IOException {
        memorySize = in.readInt64();
        evictions = in.readInt64();
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        fields = protobufStreamInput.readOptionalWriteable(ProtobufFieldMemoryStats::new);
    }

    public ProtobufFieldDataStats(long memorySize, long evictions, @Nullable ProtobufFieldMemoryStats fields) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fields = fields;
    }

    public void add(ProtobufFieldDataStats stats) {
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
    public ProtobufFieldMemoryStats getFields() {
        return fields;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(memorySize);
        out.writeInt64NoTag(evictions);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(fields);
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
        ProtobufFieldDataStats that = (ProtobufFieldDataStats) o;
        return memorySize == that.memorySize && evictions == that.evictions && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySize, evictions, fields);
    }
}

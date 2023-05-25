/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.search.suggest.completion;

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

/**
 * Stats for completion suggester
*
* @opensearch.internal
*/
public class ProtobufCompletionStats implements ProtobufWriteable, ToXContentFragment {

    private static final String COMPLETION = "completion";
    private static final String SIZE_IN_BYTES = "size_in_bytes";
    private static final String SIZE = "size";
    private static final String FIELDS = "fields";

    private long sizeInBytes;
    @Nullable
    private ProtobufFieldMemoryStats fields;

    public ProtobufCompletionStats() {}

    public ProtobufCompletionStats(CodedInputStream in) throws IOException {
        sizeInBytes = in.readInt64();
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        fields = protobufStreamInput.readOptionalWriteable(ProtobufFieldMemoryStats::new);
    }

    public ProtobufCompletionStats(long size, @Nullable ProtobufFieldMemoryStats fields) {
        this.sizeInBytes = size;
        this.fields = fields;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue getSize() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ProtobufFieldMemoryStats getFields() {
        return fields;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(sizeInBytes);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(fields);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(COMPLETION);
        builder.humanReadableField(SIZE_IN_BYTES, SIZE, getSize());
        if (fields != null) {
            fields.toXContent(builder, FIELDS, SIZE_IN_BYTES, SIZE);
        }
        builder.endObject();
        return builder;
    }

    public void add(ProtobufCompletionStats completion) {
        if (completion == null) {
            return;
        }
        sizeInBytes += completion.getSizeInBytes();
        if (completion.fields != null) {
            if (fields == null) {
                fields = completion.fields.copy();
            } else {
                fields.add(completion.fields);
            }
        }
    }
}

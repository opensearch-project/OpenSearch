/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.ingest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Information about an ingest processor
*
* @opensearch.internal
*/
public class ProtobufProcessorInfo implements ProtobufWriteable, ToXContentObject {

    private final String type;

    public ProtobufProcessorInfo(String type) {
        this.type = type;
    }

    /**
     * Read from a stream.
    */
    public ProtobufProcessorInfo(CodedInputStream input) throws IOException {
        type = input.readString();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeStringNoTag(this.type);
    }

    /**
     * @return The unique processor type
    */
    public String getType() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.endObject();
        return null;
    }
}

/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.http;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for HTTP connections
*
* @opensearch.internal
*/
public class ProtobufHttpStats implements ProtobufWriteable, ToXContentFragment {

    private final long serverOpen;
    private final long totalOpen;

    public ProtobufHttpStats(long serverOpen, long totalOpened) {
        this.serverOpen = serverOpen;
        this.totalOpen = totalOpened;
    }

    public ProtobufHttpStats(CodedInputStream in) throws IOException {
        serverOpen = in.readInt64();
        totalOpen = in.readInt64();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(serverOpen);
        out.writeInt64NoTag(totalOpen);
    }

    public long getServerOpen() {
        return this.serverOpen;
    }

    public long getTotalOpen() {
        return this.totalOpen;
    }

    static final class Fields {
        static final String HTTP = "http";
        static final String CURRENT_OPEN = "current_open";
        static final String TOTAL_OPENED = "total_opened";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.HTTP);
        builder.field(Fields.CURRENT_OPEN, serverOpen);
        builder.field(Fields.TOTAL_OPENED, totalOpen);
        builder.endObject();
        return builder;
    }
}

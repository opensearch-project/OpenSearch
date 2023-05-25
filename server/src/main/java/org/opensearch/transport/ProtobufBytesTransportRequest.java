/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;

import java.io.IOException;

/**
 * A specialized, bytes only request, that can potentially be optimized on the network
* layer, specifically for the same large buffer send to several nodes.
*
* @opensearch.internal
*/
public class ProtobufBytesTransportRequest extends ProtobufTransportRequest {

    BytesReference bytes;
    Version version;

    public ProtobufBytesTransportRequest(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        bytes = protobufStreamInput.readBytesReference();
        version = protobufStreamInput.getVersion();
    }

    public ProtobufBytesTransportRequest(BytesReference bytes, Version version) {
        this.bytes = bytes;
        this.version = version;
    }

    public Version version() {
        return this.version;
    }

    public BytesReference bytes() {
        return this.bytes;
    }

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
    * the actual bytes will be appended right after this content.
    */
    public void writeThin(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        out.writeInt32NoTag(bytes.length());
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeByteArrayNoTag(BytesReference.toBytes(bytes));
    }
}

/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.http;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.transport.ProtobufBoundTransportAddress;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;

/**
 * Information about an http connection
*
* @opensearch.internal
*/
public class ProtobufHttpInfo implements ProtobufReportingService.ProtobufInfo {

    private final ProtobufBoundTransportAddress address;
    private final long maxContentLength;

    public ProtobufHttpInfo(CodedInputStream in) throws IOException {
        this(new ProtobufBoundTransportAddress(in), in.readInt64());
    }

    public ProtobufHttpInfo(ProtobufBoundTransportAddress address, long maxContentLength) {
        this.address = address;
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        address.writeTo(out);
        out.writeInt64NoTag(maxContentLength);
    }

    public ProtobufBoundTransportAddress address() {
        return address;
    }

    public ProtobufBoundTransportAddress getAddress() {
        return address();
    }

    public ByteSizeValue maxContentLength() {
        return new ByteSizeValue(maxContentLength);
    }

    public ByteSizeValue getMaxContentLength() {
        return maxContentLength();
    }
}

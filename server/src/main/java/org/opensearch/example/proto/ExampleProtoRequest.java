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

package org.opensearch.example.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import org.opensearch.common.io.stream.TryWriteable;
import org.opensearch.example.proto.ExampleRequestProto.ExampleRequest;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Transport request for obtaining cluster state
*
* @opensearch.internal
*/
public class ExampleProtoRequest extends TransportRequest implements TryWriteable {

    private ExampleRequest request;

    public ExampleProtoRequest(int id, String message) {
        this.request = ExampleRequest.newBuilder().setId(id).setMessage(message).build();
    }

    public ExampleProtoRequest(byte[] data) throws InvalidProtocolBufferException {
        this.request = ExampleRequest.parseFrom(data);
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        // super.writeTo(out);
        out.write(this.request.toByteArray());
    }

    //add getters and setters
    public int id() {
        return request.getId();
    }

    public String message() {
        return request.getMessage();
    }

    public ExampleRequest exampleRequest() {
        return request;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ExampleRequest[");
        builder.append("header=").append(id());
        builder.append(",message=").append(message());
        return builder.append("]").toString();
    }
}

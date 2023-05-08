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

package org.opensearch.common.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;

import java.io.IOException;

/**
 * A bounded transport address is a tuple of {@link TransportAddress}, one array that represents
* the addresses the transport is bound to, and the other is the published one that represents the address clients
* should communicate on.
*
* @opensearch.internal
*/
public class ProtobufBoundTransportAddress implements ProtobufWriteable {

    private ProtobufTransportAddress[] boundAddresses;

    private ProtobufTransportAddress publishAddress;

    public ProtobufBoundTransportAddress(CodedInputStream in) throws IOException {
        int boundAddressLength = in.readInt32();
        boundAddresses = new ProtobufTransportAddress[boundAddressLength];
        for (int i = 0; i < boundAddressLength; i++) {
            boundAddresses[i] = new ProtobufTransportAddress(in);
        }
        publishAddress = new ProtobufTransportAddress(in);
    }

    public ProtobufBoundTransportAddress(ProtobufTransportAddress[] boundAddresses, ProtobufTransportAddress publishAddress) {
        if (boundAddresses == null || boundAddresses.length < 1) {
            throw new IllegalArgumentException("at least one bound address must be provided");
        }
        this.boundAddresses = boundAddresses;
        this.publishAddress = publishAddress;
    }

    public ProtobufTransportAddress[] boundAddresses() {
        return boundAddresses;
    }

    public ProtobufTransportAddress publishAddress() {
        return publishAddress;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(boundAddresses.length);
        for (ProtobufTransportAddress address : boundAddresses) {
            address.writeTo(out);
        }
        publishAddress.writeTo(out);
    }
}

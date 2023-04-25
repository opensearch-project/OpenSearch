/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.transport.TransportAddress;

/**
 * Message over the transport interface with Protobuf serialization.
*
* @opensearch.internal
*/
public abstract class ProtobufTransportMessage implements ProtobufWriteable {

    private TransportAddress remoteAddress;

    public void remoteAddress(TransportAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public TransportAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * Constructs a new empty transport message
    */
    public ProtobufTransportMessage() {}

    /**
     * Constructs a new transport message with the data from the {@link CodedInputStream}. This is
    * currently a no-op
    */
    public ProtobufTransportMessage(CodedInputStream in) {}
}

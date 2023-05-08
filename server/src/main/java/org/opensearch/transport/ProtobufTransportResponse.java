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

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Response over the transport interface
*
* @opensearch.internal
*/
public abstract class ProtobufTransportResponse extends ProtobufTransportMessage {

    /**
     * Constructs a new empty transport response
    */
    public ProtobufTransportResponse() {}

    /**
     * Constructs a new transport response with the data from the {@link StreamInput}. This is
    * currently a no-op. However, this exists to allow extenders to call <code>super(in)</code>
    * so that reading can mirror writing where we often call <code>super.writeTo(out)</code>.
    */
    public ProtobufTransportResponse(CodedInputStream in) throws IOException {
        super(in);
    }

    /**
     * Empty transport response
    *
    * @opensearch.internal
    */
    public static class Empty extends ProtobufTransportResponse {
        public static final Empty INSTANCE = new Empty();

        @Override
        public String toString() {
            return "Empty{}";
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {}
    }
}

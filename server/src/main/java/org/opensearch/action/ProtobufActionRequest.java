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

package org.opensearch.action;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.transport.ProtobufTransportRequest;

import java.io.IOException;

/**
 * Base action request implemented by plugins.
*
* @opensearch.api
*/
public abstract class ProtobufActionRequest extends ProtobufTransportRequest {

    public ProtobufActionRequest() {
        super();
        // this does not set the listenerThreaded API, if needed, its up to the caller to set it
        // since most times, we actually want it to not be threaded...
        // this.listenerThreaded = request.listenerThreaded();
    }

    public ProtobufActionRequest(CodedInputStream in) throws IOException {
        super(in);
    }

    public abstract ActionRequestValidationException validate();

    /**
     * Should this task store its result after it has finished?
    */
    public boolean getShouldStoreResult() {
        return false;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
    }
}

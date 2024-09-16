/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Base action request implemented by plugins.
*
* @opensearch.api
*/
public abstract class ProtobufActionRequest extends TransportRequest {

    public ProtobufActionRequest() {
        super();
        // this does not set the listenerThreaded API, if needed, its up to the caller to set it
        // since most times, we actually want it to not be threaded...
        // this.listenerThreaded = request.listenerThreaded();
    }

    public ProtobufActionRequest(byte[] in) throws IOException {
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
    public void writeTo(OutputStream out) throws IOException {
        super.writeTo(out);
    }
}

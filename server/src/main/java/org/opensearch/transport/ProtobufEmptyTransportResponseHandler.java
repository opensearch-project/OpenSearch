/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;

/**
 * Handler for empty transport response
*
* @opensearch.internal
*/
public class ProtobufEmptyTransportResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {

    public static final ProtobufEmptyTransportResponseHandler INSTANCE_SAME = new ProtobufEmptyTransportResponseHandler(
        ThreadPool.Names.SAME
    );

    private final String executor;

    public ProtobufEmptyTransportResponseHandler(String executor) {
        this.executor = executor;
    }

    @Override
    public void handleResponse(TransportResponse.Empty response) {}

    @Override
    public String executor() {
        return executor;
    }

    @Override
    public TransportResponse.Empty read(StreamInput in) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    @Override
    public void handleException(TransportException exp) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleException'");
    }

    @Override
    public TransportResponse.Empty read(byte[] in) throws IOException {
        return TransportResponse.Empty.INSTANCE;
    }
}

/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.threadpool.ThreadPool;

/**
 * Handler for empty transport response
*
* @opensearch.internal
*/
public class ProtobufEmptyTransportResponseHandler implements ProtobufTransportResponseHandler<ProtobufTransportResponse.Empty> {

    public static final ProtobufEmptyTransportResponseHandler INSTANCE_SAME = new ProtobufEmptyTransportResponseHandler(
        ThreadPool.Names.SAME
    );

    private final String executor;

    public ProtobufEmptyTransportResponseHandler(String executor) {
        this.executor = executor;
    }

    @Override
    public ProtobufTransportResponse.Empty read(CodedInputStream in) {
        return ProtobufTransportResponse.Empty.INSTANCE;
    }

    @Override
    public void handleResponse(ProtobufTransportResponse.Empty response) {}

    @Override
    public void handleException(ProtobufTransportException exp) {}

    @Override
    public String executor() {
        return executor;
    }
}

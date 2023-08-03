/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * A simple base class for action response listeners, defaulting to using the SAME executor (as its
* very common on response handlers).
*
* @opensearch.api
*/
public class ProtobufActionListenerResponseHandler<Response extends TransportResponse> implements TransportResponseHandler<Response> {

    private final ActionListener<? super Response> listener;
    private final ProtobufWriteable.Reader<Response> reader;
    private final String executor;

    public ProtobufActionListenerResponseHandler(
        ActionListener<? super Response> listener,
        ProtobufWriteable.Reader<Response> reader,
        String executor
    ) {
        this.listener = Objects.requireNonNull(listener);
        this.reader = Objects.requireNonNull(reader);
        this.executor = Objects.requireNonNull(executor);
    }

    public ProtobufActionListenerResponseHandler(ActionListener<? super Response> listener, ProtobufWriteable.Reader<Response> reader) {
        this(listener, reader, ThreadPool.Names.SAME);
    }

    @Override
    public void handleResponse(Response response) {
        listener.onResponse(response);
    }

    @Override
    public void handleException(TransportException e) {
        listener.onFailure(e);
    }

    @Override
    public String executor() {
        return executor;
    }

    @Override
    public String toString() {
        return super.toString() + "/" + listener;
    }

    @Override
    public Response read(StreamInput in) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    @Override
    public Response read(byte[] in) throws IOException {
        return reader.read(in);
    }
}

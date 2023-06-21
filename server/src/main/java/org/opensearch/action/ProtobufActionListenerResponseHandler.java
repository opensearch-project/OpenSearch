/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import com.google.protobuf.CodedInputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufTransportResponseHandler;
import org.opensearch.transport.ProtobufTransportException;
import org.opensearch.transport.ProtobufTransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * A simple base class for action response listeners, defaulting to using the SAME executor (as its
* very common on response handlers).
*
* @opensearch.api
*/
public class ProtobufActionListenerResponseHandler<Response extends ProtobufTransportResponse>
    implements
        ProtobufTransportResponseHandler<Response> {

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
    public void handleException(ProtobufTransportException e) {
        listener.onFailure(e);
    }

    @Override
    public String executor() {
        return executor;
    }

    @Override
    public Response read(CodedInputStream in) throws IOException {
        return reader.read(in);
    }

    @Override
    public String toString() {
        return super.toString() + "/" + listener;
    }
}

/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.util.concurrent.BaseFuture;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Future for transporting data
*
* @opensearch.internal
*/
public class ProtobufPlainTransportFuture<V extends TransportResponse> extends BaseFuture<V>
    implements
        TransportFuture<V>,
        TransportResponseHandler<V> {

    private final TransportResponseHandler<V> handler;

    public ProtobufPlainTransportFuture(TransportResponseHandler<V> handler) {
        this.handler = handler;
    }

    @Override
    public V txGet() {
        try {
            return get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof OpenSearchException) {
                throw (OpenSearchException) e.getCause();
            } else {
                throw new ProtobufTransportException("Failed execution", e);
            }
        }
    }

    @Override
    public V txGet(long timeout, TimeUnit unit) {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            throw new OpenSearchTimeoutException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof OpenSearchException) {
                throw (OpenSearchException) e.getCause();
            } else {
                throw new ProtobufTransportException("Failed execution", e);
            }
        }
    }

    @Override
    public V read(CodedInputStream in) throws IOException {
        return handler.read(in);
    }

    @Override
    public String executor() {
        return handler.executor();
    }

    @Override
    public void handleResponse(V response) {
        try {
            handler.handleResponse(response);
            set(response);
        } catch (Exception e) {
            handleExceptionProtobuf(new ProtobufTransportException(e));
        }
    }

    @Override
    public void handleExceptionProtobuf(ProtobufTransportException exp) {
        try {
            handler.handleExceptionProtobuf(exp);
        } finally {
            setException(exp);
        }
    }

    @Override
    public String toString() {
        return "future(" + handler.toString() + ")";
    }

    @Override
    public V read(StreamInput in) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    @Override
    public void handleException(TransportException exp) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'handleException'");
    }

    @Override
    public V read(byte[] in) throws IOException {
        return handler.read(in);
    }
}

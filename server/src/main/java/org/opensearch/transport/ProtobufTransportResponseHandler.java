/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;

import java.io.IOException;
import java.util.function.Function;

/**
 * Handles transport responses
*
* @opensearch.internal
*/
public interface ProtobufTransportResponseHandler<T extends ProtobufTransportResponse> extends ProtobufWriteable.Reader<T> {

    void handleResponse(T response);

    void handleException(ProtobufTransportException exp);

    String executor();

    default <Q extends ProtobufTransportResponse> ProtobufTransportResponseHandler<Q> wrap(
        Function<Q, T> converter,
        ProtobufWriteable.Reader<Q> reader
    ) {
        final ProtobufTransportResponseHandler<T> self = this;
        return new ProtobufTransportResponseHandler<Q>() {
            @Override
            public void handleResponse(Q response) {
                self.handleResponse(converter.apply(response));
            }

            @Override
            public void handleException(ProtobufTransportException exp) {
                self.handleException(exp);
            }

            @Override
            public String executor() {
                return self.executor();
            }

            @Override
            public Q read(CodedInputStream in) throws IOException {
                return reader.read(in);
            }
        };
    }
}

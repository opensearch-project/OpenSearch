/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.BytesWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

/**
 * Handles transport responses
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface TransportResponseHandler<T extends TransportResponse> extends Writeable.Reader<T>, BytesWriteable.Reader<T> {

    void handleResponse(T response);

    void handleException(TransportException exp);

    String executor();

    /**
     * Read {@code V}-type value from a byte array.
     *
     * @param in byte array to read the value from
     */
    default T read(final InputStream in) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    /**
     * This method should be handling the rejection/failure scenarios where connection to the node is rejected or failed.
     * It should be used to clear up the resources held by the {@link TransportResponseHandler}.
     * @param exp exception
     */
    default void handleRejection(Exception exp) {}

    default <Q extends TransportResponse> TransportResponseHandler<Q> wrap(Function<Q, T> converter, Writeable.Reader<Q> reader) {
        final TransportResponseHandler<T> self = this;
        return new TransportResponseHandler<Q>() {
            @Override
            public void handleResponse(Q response) {
                self.handleResponse(converter.apply(response));
            }

            @Override
            public void handleException(TransportException exp) {
                self.handleException(exp);
            }

            @Override
            public String executor() {
                return self.executor();
            }

            @Override
            public Q read(StreamInput in) throws IOException {
                return reader.read(in);
            }

            @Override
            public Q read(InputStream in) throws IOException {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'read'");
            }
        };
    }
}

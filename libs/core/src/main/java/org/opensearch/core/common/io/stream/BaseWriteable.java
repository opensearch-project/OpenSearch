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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.core.common.io.stream;

import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;

/**
 * Implementers can be written to a an output stream and read from an input stream. The type of input and output stream depends on the
 * serialization/deserialization protocol.
 *
 * @opensearch.api
 */
public interface BaseWriteable<T, S> {

    /**
     * Write this into the specified output stream.
     */
    void writeTo(T out) throws IOException;

    /**
     * Reference to a method that can read some object from a specified input stream.
     * @opensearch.api
     */
    @FunctionalInterface
    @InternalApi
    interface Reader<S, V> {

        /**
         * Read {@code V}-type value from a stream.
         *
         * @param in Input to read the value from
         */
        V read(final S in) throws IOException;
    }
}

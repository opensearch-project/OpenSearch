/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Implementers can be written to a {@linkplain OutputStream} and read from a {@linkplain byte[]}. This allows them to be "thrown
 * across the wire" using OpenSearch's internal protocol with protobuf serialization/de-serialization.
 *
 * @opensearch.internal
 */
public interface ProtobufWriteable {

    /**
     * Write this into the {@linkplain OutputStream}.
     */
    void writeTo(OutputStream out) throws IOException;

    /**
     * Reference to a method that can read some object from a byte array stream.
     */
    @FunctionalInterface
    interface Reader<V> {

        /**
         * Read {@code V}-type value from a stream.
         *
         * @param in Input to read the value from
         */
        V read(final byte[] in) throws IOException;
    }
}

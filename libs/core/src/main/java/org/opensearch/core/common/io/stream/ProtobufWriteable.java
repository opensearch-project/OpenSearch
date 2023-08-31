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
 * Implementers can be written to a {@linkplain StreamOutput} and read from a {@linkplain StreamInput}. This allows them to be "thrown
 * across the wire" using OpenSearch's internal protocol. If the implementer also implements equals and hashCode then a copy made by
 * serializing and deserializing must be equal and have the same hashCode. It isn't required that such a copy be entirely unchanged.
 *
 * @opensearch.internal
 */
public interface ProtobufWriteable {

    /**
     * Write this into the {@linkplain OutputStream}.
     */
    void writeTo(OutputStream out) throws IOException;

    /**
     * Reference to a method that can write some object to a {@link OutputStream}.
     */
    @FunctionalInterface
    interface Writer<V> {

        /**
         * Write {@code V}-type {@code value} to the {@code out}put stream.
         *
         * @param out Output to write the {@code value} too
         * @param value The value to add
         */
        void write(final OutputStream out, V value) throws IOException;
    }

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

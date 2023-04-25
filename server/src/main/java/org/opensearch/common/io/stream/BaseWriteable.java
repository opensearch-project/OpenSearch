/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import java.io.IOException;

/**
 * This interface can be extended to different types of serialization and deserialization mechanisms.
 *
 * @opensearch.internal
 */
public interface BaseWriteable<T, S> {

    /**
     * Write this into the stream output.
    */
    void writeTo(T out) throws IOException;

    /**
     * Reference to a method that can write some object to a given type.
     */
    @FunctionalInterface
    interface Writer<T, V> {

        /**
         * Write {@code V}-type {@code value} to the {@code T}-type stream.
         *
         * @param out Output to write the {@code value} too
         * @param value The value to add
         */
        void write(T out, V value) throws IOException;
    }

    /**
     * Reference to a method that can read some object from a given stream type.
     */
    @FunctionalInterface
    interface Reader<S, V> {

        /**
         * Read {@code V}-type value from a {@code T}-type stream.
        *
        * @param in Input to read the value from
        */
        V read(S in) throws IOException;
    }
}

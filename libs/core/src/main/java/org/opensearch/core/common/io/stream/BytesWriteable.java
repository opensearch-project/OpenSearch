/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implementers can be written to a {@linkplain OutputStream} and read from a byte array. This allows them to be "thrown
 * across the wire" using OpenSearch's internal protocol with protobuf bytes.
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface BytesWriteable {

    /**
     * Write this into the {@linkplain OutputStream}.
     */
    void writeTo(OutputStream out) throws IOException;

    /**
     * Reference to a method that can write some object to a {@link OutputStream}.
     *
     * @opensearch.experimental
     */
    @FunctionalInterface
    @ExperimentalApi
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
     * Reference to a method that can read some object from a byte array.
     *
     * @opensearch.experimental
     */
    @FunctionalInterface
    @ExperimentalApi
    interface Reader<V> {

        /**
         * Read {@code V}-type value from a byte array.
         *
         * @param in byte array to read the value from
         */
        V read(final InputStream in) throws IOException;
    }
}

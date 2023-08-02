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
 * Implementers can be written to write to output and read from input using Protobuf.
*
* @opensearch.internal
*/
public interface ProtobufWriteable {

    /**
     * Write this into the stream output.
    */
    public void writeTo(OutputStream out) throws IOException;

    /**
     * Reference to a method that can write some object to a {@link OutputStream}.
     * Most classes should implement {@link ProtobufWriteable} and the {@link ProtobufWriteable#writeTo(OutputStream)} method should <em>use</em>
     * {@link OutputStream} methods directly or this indirectly:
     * <pre><code>
     * public void writeTo(OutputStream out) throws IOException {
     *     out.writeVInt(someValue);
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Writer<V> {

        /**
         * Write {@code V}-type {@code value} to the {@code out}put stream.
        *
        * @param out Output to write the {@code value} too
        * @param value The value to add
        */
        void write(OutputStream out, V value) throws IOException;

    }

    /**
     * Reference to a method that can read some object from a stream. By convention this is a constructor that takes
     * {@linkplain byte[]} as an argument for most classes and a static method for things like enums.
     * <pre><code>
     * public MyClass(final byte[] in) throws IOException {
     *     this.someValue = in.readVInt();
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Reader<V> {

        /**
         * Read {@code V}-type value from a stream.
        *
        * @param in Input to read the value from
        */
        V read(byte[] in) throws IOException;

    }

}

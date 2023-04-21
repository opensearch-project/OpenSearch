/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.common.io.stream;

import java.io.IOException;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

/**
 * Implementers can be written to write to output and read from input using Protobuf.
*
* @opensearch.internal
*/
public interface ProtobufWriteable extends BaseWriteable<CodedOutputStream, CodedInputStream> {

    /**
     * Write this into the stream output.
    */
    public void writeTo(CodedOutputStream out) throws IOException;

    /**
     * Reference to a method that can write some object to a {@link CodedOutputStream}.
     * Most classes should implement {@link ProtobufWriteable} and the {@link ProtobufWriteable#writeTo(CodedOutputStream)} method should <em>use</em>
     * {@link CodedOutputStream} methods directly or this indirectly:
     * <pre><code>
     * public void writeTo(CodedOutputStream out) throws IOException {
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
        void write(CodedOutputStream out, V value) throws IOException;

    }

    /**
     * Reference to a method that can read some object from a stream. By convention this is a constructor that takes
     * {@linkplain CodedInputStream} as an argument for most classes and a static method for things like enums.
     * <pre><code>
     * public MyClass(final CodedInputStream in) throws IOException {
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
        V read(CodedInputStream in) throws IOException;

    }

}

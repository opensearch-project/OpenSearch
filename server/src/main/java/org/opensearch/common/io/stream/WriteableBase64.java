/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Base64;

/**
 * Utility for round-tripping a {@link Writeable} through its binary wire format encoded as a
 * Base64 string. This is useful when a {@link Writeable} needs to travel over a channel that only
 * carries strings (for example a {@code ThreadContext} response header), where the binary form is
 * both cheaper to produce/consume than JSON and more compact once Base64-encoded.
 *
 * @opensearch.internal
 */
@InternalApi
public final class WriteableBase64 {

    private WriteableBase64() {}

    /**
     * Serializes a {@link Writeable} to its binary wire format and Base64-encodes the result.
     *
     * @param writeable the object to serialize
     * @return a Base64-encoded string of the object's binary representation
     * @throws IOException if serialization fails
     */
    public static String encode(Writeable writeable) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeable.writeTo(out);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    /**
     * Decodes a Base64 string produced by {@link #encode(Writeable)} back into an object using the
     * supplied reader.
     *
     * @param encoded the Base64-encoded string
     * @param reader  reconstructs the object from a {@link StreamInput} (for example
     *                {@code MyType::readFromStream} or {@code MyType::new})
     * @param <T>     the type produced by {@code reader}
     * @return the deserialized object
     * @throws IOException              if deserialization fails
     * @throws IllegalArgumentException if the input is not valid Base64
     */
    public static <T> T decode(String encoded, Writeable.Reader<T> reader) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(encoded);
        try (StreamInput in = StreamInput.wrap(bytes)) {
            return reader.read(in);
        }
    }
}

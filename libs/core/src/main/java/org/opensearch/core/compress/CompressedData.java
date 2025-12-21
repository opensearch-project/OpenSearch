/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.compress;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents compressed data that can be serialized and deserialized across OpenSearch modules.
 * This interface provides an abstraction for compressed content, allowing different implementations
 * to be used without depending on server-specific classes.
 * <p>
 * Implementations of this interface should:
 * <ul>
 *   <li>Store data in a compressed format to reduce memory and network overhead</li>
 *   <li>Provide access to both compressed and uncompressed representations</li>
 *   <li>Support serialization via the {@link Writeable} interface</li>
 *   <li>Be immutable to ensure thread safety</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>{@code
 * // Using with a concrete implementation
 * CompressedData data = new CompressedXContent("{\"query\":{\"match_all\":{}}}");
 *
 * // Accessing compressed bytes
 * byte[] compressed = data.compressed();
 *
 * // Accessing uncompressed content
 * String json = data.string();
 *
 * // Serialization
 * data.writeTo(output);
 *
 * // Deserialization with a specific implementation
 * CompressedData deserialized = CompressedData.readFrom(input, CompressedXContent.READER);
 * }</pre>
 *
 * @opensearch.api
 * @since 1.0.0
 */
@PublicApi(since = "1.0.0")
public interface CompressedData extends Writeable {

    /**
     * Returns the compressed data as a byte array.
     * <p>
     * This method provides direct access to the compressed bytes, which is useful for
     * storage or transmission. The returned array should not be modified.
     *
     * @return the compressed data as a byte array
     */
    byte[] compressed();

    /**
     * Returns the compressed data as a {@link BytesReference}.
     * <p>
     * This method provides a reference to the compressed bytes without copying,
     * which is more efficient for large data sets.
     *
     * @return the compressed data as a BytesReference
     */
    BytesReference compressedReference();

    /**
     * Returns the uncompressed data as a {@link BytesReference}.
     * <p>
     * This method decompresses the data on demand. Implementations may cache
     * the uncompressed result for efficiency.
     *
     * @return the uncompressed data as a BytesReference
     */
    BytesReference uncompressed();

    /**
     * Returns the uncompressed data as a UTF-8 string.
     * <p>
     * This is a convenience method equivalent to calling
     * {@code uncompressed().utf8ToString()}.
     *
     * @return the uncompressed data as a string
     */
    String string();

    /**
     * Helper method to read a {@link CompressedData} implementation from a stream.
     * <p>
     * This method delegates to the provided reader to deserialize the specific
     * implementation. This pattern allows the interface to support multiple
     * implementations without knowing their concrete types.
     * <p>
     * Example usage:
     * <pre>{@code
     * // Reading a CompressedXContent
     * CompressedData data = CompressedData.readFrom(in, CompressedXContent.READER);
     *
     * // Reading a custom implementation
     * CompressedData data = CompressedData.readFrom(in, MyCompressedData.READER);
     * }</pre>
     *
     * @param in the stream to read from
     * @param reader the reader for the specific implementation
     * @param <T> the concrete type of CompressedData
     * @return the deserialized CompressedData instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    static <T extends CompressedData> T readFrom(StreamInput in, Reader<T> reader) throws IOException {
        return reader.read(in);
    }
}

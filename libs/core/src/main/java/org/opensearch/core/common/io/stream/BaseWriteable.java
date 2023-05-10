/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.core.common.io.stream;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementers can be written to a {@code StreamOutput} and read from a {@code StreamInput}. This allows them to be "thrown
 * across the wire" using OpenSearch's internal protocol. If the implementer also implements equals and hashCode then a copy made by
 * serializing and deserializing must be equal and have the same hashCode. It isn't required that such a copy be entirely unchanged.
 *
 * @opensearch.internal
 */
public interface BaseWriteable<S extends BaseStreamOutput> {
    /**
     * A WriteableRegistry registers {@link Writer} methods for writing data types over a
     * {@link BaseStreamOutput} channel and {@link Reader} methods for reading data from a
     * {@link BaseStreamInput} channel.
     *
     * @opensearch.internal
     */
    class WriteableRegistry {
        private static final Map<Class<?>, Writer<? extends BaseStreamOutput, ?>> WRITER_REGISTRY = new ConcurrentHashMap<>();
        private static final Map<Byte, Reader<? extends BaseStreamInput, ?>> READER_REGISTRY = new ConcurrentHashMap<>();

        /**
         * registers a streamable writer
         *
         * @opensearch.internal
         */
        public static <W extends Writer<? extends BaseStreamOutput, ?>> void registerWriter(final Class<?> clazz, final W writer) {
            if (WRITER_REGISTRY.containsKey(clazz)) {
                throw new IllegalArgumentException("Streamable writer already registered for type [" + clazz.getName() + "]");
            }
            WRITER_REGISTRY.put(clazz, writer);
        }

        /**
         * registers a streamable reader
         *
         * @opensearch.internal
         */
        public static <R extends Reader<? extends BaseStreamInput, ?>> void registerReader(final byte ordinal, final R reader) {
            if (READER_REGISTRY.containsKey(ordinal)) {
                throw new IllegalArgumentException("Streamable reader already registered for ordinal [" + (int) ordinal + "]");
            }
            READER_REGISTRY.put(ordinal, reader);
        }

        /**
         * Returns the registered writer keyed by the class type
         */
        @SuppressWarnings("unchecked")
        public static <W extends Writer<? extends BaseStreamOutput, ?>> W getWriter(final Class<?> clazz) {
            return (W) WRITER_REGISTRY.get(clazz);
        }

        /**
         * Returns the ristered reader keyed by the unique ordinal
         */
        @SuppressWarnings("unchecked")
        public static <R extends Reader<? extends BaseStreamInput, ?>> R getReader(final byte b) {
            return (R) READER_REGISTRY.get(b);
        }
    }

    /**
     * Write this into the {@linkplain BaseStreamOutput}.
     */
    void writeTo(final S out) throws IOException;

    /**
     * Reference to a method that can write some object to a {@link BaseStreamOutput}.
     * <p>
     * By convention this is a method from {@link BaseStreamOutput} itself (e.g., {@code StreamOutput#writeString}). If the value can be
     * {@code null}, then the "optional" variant of methods should be used!
     * <p>
     * Most classes should implement {@code Writeable} and the {@code Writeable#writeTo(BaseStreamOutput)} method should <em>use</em>
     * {@link BaseStreamOutput} methods directly or this indirectly:
     * <pre><code>
     * public void writeTo(StreamOutput out) throws IOException {
     *     out.writeVInt(someValue);
     *     out.writeMapOfLists(someMap, StreamOutput::writeString, StreamOutput::writeString);
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Writer<S extends BaseStreamOutput, V> {

        /**
         * Write {@code V}-type {@code value} to the {@code out}put stream.
         *
         * @param out Output to write the {@code value} too
         * @param value The value to add
         */
        void write(final S out, V value) throws IOException;
    }

    /**
     * Reference to a method that can read some object from a stream. By convention this is a constructor that takes
     * {@linkplain BaseStreamInput} as an argument for most classes and a static method for things like enums. Returning null from one of these
     * is always wrong - for that we use methods like {@code StreamInput#readOptionalWriteable(Reader)}.
     * <p>
     * As most classes will implement this via a constructor (or a static method in the case of enumerations), it's something that should
     * look like:
     * <pre><code>
     * public MyClass(final StreamInput in) throws IOException {
     *     this.someValue = in.readVInt();
     *     this.someMap = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
     * }
     * </code></pre>
     */
    @FunctionalInterface
    interface Reader<S extends BaseStreamInput, V> {

        /**
         * Read {@code V}-type value from a stream.
         *
         * @param in Input to read the value from
         */
        V read(final S in) throws IOException;
    }
}

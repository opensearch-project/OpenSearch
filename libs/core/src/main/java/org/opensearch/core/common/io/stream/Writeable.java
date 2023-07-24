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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementers can be written to a {@linkplain StreamOutput} and read from a {@linkplain StreamInput}. This allows them to be "thrown
 * across the wire" using OpenSearch's internal protocol. If the implementer also implements equals and hashCode then a copy made by
 * serializing and deserializing must be equal and have the same hashCode. It isn't required that such a copy be entirely unchanged.
 *
 * @opensearch.internal
 */
public interface Writeable {
    /**
     * A WriteableRegistry registers {@link Writer} methods for writing data types over a
     * {@link StreamOutput} channel and {@link Reader} methods for reading data from a
     * {@link StreamInput} channel.
     *
     * @opensearch.internal
     */
    class WriteableRegistry {
        private static final Map<Class<?>, Writer<?>> WRITER_REGISTRY = new ConcurrentHashMap<>();
        private static final Map<Class<?>, Class<?>> WRITER_CUSTOM_CLASS_MAP = new ConcurrentHashMap<>();
        private static final Map<Byte, Reader<?>> READER_REGISTRY = new ConcurrentHashMap<>();

        /**
         * registers a streamable writer
         *
         * @opensearch.internal
         */
        public static <W extends Writer<?>> void registerWriter(final Class<?> clazz, final W writer) {
            if (WRITER_REGISTRY.putIfAbsent(clazz, writer) != null) {
                throw new IllegalArgumentException("Streamable writer already registered for type [" + clazz.getName() + "]");
            }
        }

        /**
         * registers a streamable reader
         *
         * @opensearch.internal
         */
        public static <R extends Reader<?>> void registerReader(final byte ordinal, final R reader) {
            if (READER_REGISTRY.putIfAbsent(ordinal, reader) != null) {
                throw new IllegalArgumentException("Streamable reader already registered for ordinal [" + (int) ordinal + "]");
            }
        }

        public static void registerClassAlias(final Class<?> classInstance, final Class<?> classGeneric) {
            if (WRITER_CUSTOM_CLASS_MAP.putIfAbsent(classInstance, classGeneric) != null) {
                throw new IllegalArgumentException("Streamable custom class already registered [" + classInstance.getClass() + "]");
            }
        }

        /**
         * Returns the registered writer keyed by the class type
         */
        @SuppressWarnings("unchecked")
        public static <W extends Writer<?>> W getWriter(final Class<?> clazz) {
            return (W) WRITER_REGISTRY.get(clazz);
        }

        /**
         * Returns the ristered reader keyed by the unique ordinal
         */
        @SuppressWarnings("unchecked")
        public static <R extends Reader<?>> R getReader(final byte b) {
            return (R) READER_REGISTRY.get(b);
        }

        public static Class<?> getCustomClassFromInstance(final Object value) {
            if (value == null) {
                throw new IllegalArgumentException("Attempting to retrieve a class type from a null value");
            }
            // rip through registered classes; return the class iff 'value' is an instance
            // we do it this way to cover inheritance and interfaces (e.g., joda DateTime is an instanceof
            // a ReadableInstant interface)
            for (final Class<?> clazz : WRITER_CUSTOM_CLASS_MAP.values()) {
                if (clazz.isInstance(value)) {
                    return clazz;
                }
            }
            return null;
        }
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     */
    void writeTo(StreamOutput out) throws IOException;

    /**
     * Reference to a method that can write some object to a {@link StreamOutput}.
     * <p>
     * By convention this is a method from {@link StreamOutput} itself (e.g., {@code StreamOutput#writeString}). If the value can be
     * {@code null}, then the "optional" variant of methods should be used!
     * <p>
     * Most classes should implement {@code Writeable} and the {@code Writeable#writeTo(StreamOutput)} method should <em>use</em>
     * {@link StreamOutput} methods directly or this indirectly:
     * <pre><code>
     * public void writeTo(StreamOutput out) throws IOException {
     *     out.writeVInt(someValue);
     *     out.writeMapOfLists(someMap, StreamOutput::writeString, StreamOutput::writeString);
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
        void write(final StreamOutput out, V value) throws IOException;
    }

    /**
     * Reference to a method that can read some object from a stream. By convention this is a constructor that takes
     * {@linkplain StreamInput} as an argument for most classes and a static method for things like enums. Returning null from one of these
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
    interface Reader<V> {

        /**
         * Read {@code V}-type value from a stream.
         *
         * @param in Input to read the value from
         */
        V read(final StreamInput in) throws IOException;
    }
}

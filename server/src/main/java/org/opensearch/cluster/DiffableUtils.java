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

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable.Reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for a diffable
 *
 * @opensearch.internal
 */
public final class DiffableUtils {
    private DiffableUtils() {}

    /**
     * Returns a map key serializer for String keys
     */
    public static KeySerializer<String> getStringKeySerializer() {
        return StringKeySerializer.INSTANCE;
    }

    /**
     * Returns a map key serializer for Integer keys. Encodes as Int.
     */
    public static KeySerializer<Integer> getIntKeySerializer() {
        return IntKeySerializer.INSTANCE;
    }

    /**
     * Returns a map key serializer for Integer keys. Encodes as VInt.
     */
    public static KeySerializer<Integer> getVIntKeySerializer() {
        return VIntKeySerializer.INSTANCE;
    }

    /**
     * Calculates diff between two Maps of Diffable objects.
     */
    public static <K, T extends Diffable<T>> MapDiff<K, T, Map<K, T>> diff(
        Map<K, T> before,
        Map<K, T> after,
        KeySerializer<K> keySerializer
    ) {
        assert after != null && before != null;
        return new JdkMapDiff<>(before, after, keySerializer, DiffableValueSerializer.getWriteOnlyInstance());
    }

    /**
     * Calculates diff between two Maps of non-diffable objects
     */
    public static <K, T> MapDiff<K, T, Map<K, T>> diff(
        Map<K, T> before,
        Map<K, T> after,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) {
        assert after != null && before != null;
        return new JdkMapDiff<>(before, after, keySerializer, valueSerializer);
    }

    /**
     * Loads an object that represents difference between two Maps of Diffable objects
     */
    public static <K, T> MapDiff<K, T, Map<K, T>> readJdkMapDiff(
        StreamInput in,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) throws IOException {
        return new JdkMapDiff<>(in, keySerializer, valueSerializer);
    }

    /**
     * Loads an object that represents difference between two Maps of Diffable objects using Diffable proto object
     */
    public static <K, T extends Diffable<T>> MapDiff<K, T, Map<K, T>> readJdkMapDiff(
        StreamInput in,
        KeySerializer<K> keySerializer,
        Reader<T> reader,
        Reader<Diff<T>> diffReader
    ) throws IOException {
        return new JdkMapDiff<>(in, keySerializer, new DiffableValueReader<>(reader, diffReader));
    }

    /**
     * Represents differences between two Maps of (possibly diffable) objects.
     *
     * @param <T> the diffable object
     *
     * @opensearch.internal
     */
    private static class JdkMapDiff<K, T> extends MapDiff<K, T, Map<K, T>> {

        protected JdkMapDiff(StreamInput in, KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer) throws IOException {
            super(in, keySerializer, valueSerializer);
        }

        JdkMapDiff(Map<K, T> before, Map<K, T> after, KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer) {
            super(keySerializer, valueSerializer);
            assert after != null && before != null;

            for (K key : before.keySet()) {
                if (!after.containsKey(key)) {
                    deletes.add(key);
                }
            }

            for (Map.Entry<K, T> partIter : after.entrySet()) {
                T beforePart = before.get(partIter.getKey());
                if (beforePart == null) {
                    upserts.put(partIter.getKey(), partIter.getValue());
                } else if (partIter.getValue().equals(beforePart) == false) {
                    if (valueSerializer.supportsDiffableValues()) {
                        diffs.put(partIter.getKey(), valueSerializer.diff(partIter.getValue(), beforePart));
                    } else {
                        upserts.put(partIter.getKey(), partIter.getValue());
                    }
                }
            }
        }

        @Override
        public Map<K, T> apply(Map<K, T> map) {
            Map<K, T> builder = new HashMap<>(map);

            for (K part : deletes) {
                builder.remove(part);
            }

            for (Map.Entry<K, Diff<T>> diff : diffs.entrySet()) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<K, T> upsert : upserts.entrySet()) {
                builder.put(upsert.getKey(), upsert.getValue());
            }
            return builder;
        }
    }

    /**
     * Represents differences between two maps of objects and is used as base class for different map implementations.
     * <p>
     * Implements serialization. How differences are applied is left to subclasses.
     *
     * @param <K> the type of map keys
     * @param <T> the type of map values
     * @param <M> the map implementation type
     *
     * @opensearch.internal
     */
    public abstract static class MapDiff<K, T, M> implements Diff<M> {

        protected final List<K> deletes;
        protected final Map<K, Diff<T>> diffs; // incremental updates
        protected final Map<K, T> upserts; // additions or full updates
        protected final KeySerializer<K> keySerializer;
        protected final ValueSerializer<K, T> valueSerializer;

        protected MapDiff(KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            deletes = new ArrayList<>();
            diffs = new HashMap<>();
            upserts = new HashMap<>();
        }

        protected MapDiff(
            KeySerializer<K> keySerializer,
            ValueSerializer<K, T> valueSerializer,
            List<K> deletes,
            Map<K, Diff<T>> diffs,
            Map<K, T> upserts
        ) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.deletes = deletes;
            this.diffs = diffs;
            this.upserts = upserts;
        }

        protected MapDiff(StreamInput in, KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer) throws IOException {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            deletes = in.readList(keySerializer::readKey);
            int diffsCount = in.readVInt();
            diffs = diffsCount == 0 ? Collections.emptyMap() : new HashMap<>(diffsCount);
            for (int i = 0; i < diffsCount; i++) {
                K key = keySerializer.readKey(in);
                Diff<T> diff = valueSerializer.readDiff(in, key);
                diffs.put(key, diff);
            }
            int upsertsCount = in.readVInt();
            upserts = upsertsCount == 0 ? Collections.emptyMap() : new HashMap<>(upsertsCount);
            for (int i = 0; i < upsertsCount; i++) {
                K key = keySerializer.readKey(in);
                T newValue = valueSerializer.read(in, key);
                upserts.put(key, newValue);
            }
        }

        /**
         * The keys that, when this diff is applied to a map, should be removed from the map.
         *
         * @return the list of keys that are deleted
         */
        public List<K> getDeletes() {
            return deletes;
        }

        /**
         * Map entries that, when this diff is applied to a map, should be
         * incrementally updated. The incremental update is represented using
         * the {@link Diff} interface.
         *
         * @return the map entries that are incrementally updated
         */
        public Map<K, Diff<T>> getDiffs() {
            return diffs;
        }

        /**
         * Map entries that, when this diff is applied to a map, should be
         * added to the map or fully replace the previous value.
         *
         * @return the map entries that are additions or full updates
         */
        public Map<K, T> getUpserts() {
            return upserts;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("MapDiff{deletes=")
                .append(deletes)
                .append(", diffs=")
                .append(diffs)
                .append(", upserts=")
                .append(upserts)
                .append("}")
                .toString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(deletes, (o, v) -> keySerializer.writeKey(v, o));
            Version version = out.getVersion();
            // filter out custom states not supported by the other node
            int diffCount = 0;
            for (Diff<T> diff : diffs.values()) {
                if (valueSerializer.supportsVersion(diff, version)) {
                    diffCount++;
                }
            }
            out.writeVInt(diffCount);
            for (Map.Entry<K, Diff<T>> entry : diffs.entrySet()) {
                if (valueSerializer.supportsVersion(entry.getValue(), version)) {
                    keySerializer.writeKey(entry.getKey(), out);
                    valueSerializer.writeDiff(entry.getValue(), out);
                }
            }
            // filter out custom states not supported by the other node
            int upsertsCount = 0;
            for (T upsert : upserts.values()) {
                if (valueSerializer.supportsVersion(upsert, version)) {
                    upsertsCount++;
                }
            }
            out.writeVInt(upsertsCount);
            for (Map.Entry<K, T> entry : upserts.entrySet()) {
                if (valueSerializer.supportsVersion(entry.getValue(), version)) {
                    keySerializer.writeKey(entry.getKey(), out);
                    valueSerializer.write(entry.getValue(), out);
                }
            }
        }
    }

    /**
     * Provides read and write operations to serialize keys of map
     * @param <K> type of key
     *
     * @opensearch.internal
     */
    public interface KeySerializer<K> {
        void writeKey(K key, StreamOutput out) throws IOException;

        K readKey(StreamInput in) throws IOException;
    }

    /**
     * Serializes String keys of a map
     *
     * @opensearch.internal
     */
    private static final class StringKeySerializer implements KeySerializer<String> {
        private static final StringKeySerializer INSTANCE = new StringKeySerializer();

        @Override
        public void writeKey(String key, StreamOutput out) throws IOException {
            out.writeString(key);
        }

        @Override
        public String readKey(StreamInput in) throws IOException {
            return in.readString();
        }
    }

    /**
     * Serializes Integer keys of a map as an Int
     *
     * @opensearch.internal
     */
    private static final class IntKeySerializer implements KeySerializer<Integer> {
        public static final IntKeySerializer INSTANCE = new IntKeySerializer();

        @Override
        public void writeKey(Integer key, StreamOutput out) throws IOException {
            out.writeInt(key);
        }

        @Override
        public Integer readKey(StreamInput in) throws IOException {
            return in.readInt();
        }
    }

    /**
     * Serializes Integer keys of a map as a VInt. Requires keys to be positive.
     *
     * @opensearch.internal
     */
    private static final class VIntKeySerializer implements KeySerializer<Integer> {
        public static final IntKeySerializer INSTANCE = new IntKeySerializer();

        @Override
        public void writeKey(Integer key, StreamOutput out) throws IOException {
            if (key < 0) {
                throw new IllegalArgumentException("Map key [" + key + "] must be positive");
            }
            out.writeVInt(key);
        }

        @Override
        public Integer readKey(StreamInput in) throws IOException {
            return in.readVInt();
        }
    }

    /**
     * Provides read and write operations to serialize map values.
     * Reading of values can be made dependent on map key.
     * <p>
     * Also provides operations to distinguish whether map values are diffable.
     * <p>
     * Should not be directly implemented, instead implement either
     * {@link DiffableValueSerializer} or {@link NonDiffableValueSerializer}.
     *
     * @param <K> key type of map
     * @param <V> value type of map
     *
     * @opensearch.internal
     */
    public interface ValueSerializer<K, V> {

        /**
         * Writes value to stream
         */
        void write(V value, StreamOutput out) throws IOException;

        /**
         * Reads value from stream. Reading operation can be made dependent on map key.
         */
        V read(StreamInput in, K key) throws IOException;

        /**
         * Whether this serializer supports diffable values
         */
        boolean supportsDiffableValues();

        /**
         * Whether this serializer supports the version of the output stream
         */
        default boolean supportsVersion(Diff<V> value, Version version) {
            return true;
        }

        /**
         * Whether this serializer supports the version of the output stream
         */
        default boolean supportsVersion(V value, Version version) {
            return true;
        }

        /**
         * Computes diff if this serializer supports diffable values
         */
        Diff<V> diff(V value, V beforePart);

        /**
         * Writes value as diff to stream if this serializer supports diffable values
         */
        void writeDiff(Diff<V> value, StreamOutput out) throws IOException;

        /**
         * Reads value as diff from stream if this serializer supports diffable values.
         * Reading operation can be made dependent on map key.
         */
        Diff<V> readDiff(StreamInput in, K key) throws IOException;
    }

    /**
     * Serializer for Diffable map values. Needs to implement read and readDiff methods.
     *
     * @param <K> type of map keys
     * @param <V> type of map values
     *
     * @opensearch.internal
     */
    public abstract static class DiffableValueSerializer<K, V extends Diffable<V>> implements ValueSerializer<K, V> {
        private static final DiffableValueSerializer WRITE_ONLY_INSTANCE = new DiffableValueSerializer() {
            @Override
            public Object read(StreamInput in, Object key) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Diff<Object> readDiff(StreamInput in, Object key) throws IOException {
                throw new UnsupportedOperationException();
            }
        };

        private static <K, V extends Diffable<V>> DiffableValueSerializer<K, V> getWriteOnlyInstance() {
            return WRITE_ONLY_INSTANCE;
        }

        @Override
        public boolean supportsDiffableValues() {
            return true;
        }

        @Override
        public Diff<V> diff(V value, V beforePart) {
            return value.diff(beforePart);
        }

        @Override
        public void write(V value, StreamOutput out) throws IOException {
            value.writeTo(out);
        }

        public void writeDiff(Diff<V> value, StreamOutput out) throws IOException {
            value.writeTo(out);
        }
    }

    /**
     * Serializer for non-diffable map values
     *
     * @param <K> type of map keys
     * @param <V> type of map values
     *
     * @opensearch.internal
     */
    public abstract static class NonDiffableValueSerializer<K, V> implements ValueSerializer<K, V> {
        private static final NonDiffableValueSerializer ABSTRACT_INSTANCE = new NonDiffableValueSerializer<>() {
            @Override
            public void write(Object value, StreamOutput out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object read(StreamInput in, Object key) {
                throw new UnsupportedOperationException();
            }
        };

        @Override
        public boolean supportsDiffableValues() {
            return false;
        }

        @Override
        public Diff<V> diff(V value, V beforePart) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDiff(Diff<V> value, StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Diff<V> readDiff(StreamInput in, K key) throws IOException {
            throw new UnsupportedOperationException();
        }

        public static <K, V> NonDiffableValueSerializer<K, V> getAbstractInstance() {
            return ABSTRACT_INSTANCE;
        }
    }

    /**
     * Implementation of the ValueSerializer that wraps value and diff readers.
     * <p>
     * Note: this implementation is ignoring the key.
     *
     * @opensearch.internal
     */
    public static class DiffableValueReader<K, V extends Diffable<V>> extends DiffableValueSerializer<K, V> {
        private final Reader<V> reader;
        private final Reader<Diff<V>> diffReader;

        public DiffableValueReader(Reader<V> reader, Reader<Diff<V>> diffReader) {
            this.reader = reader;
            this.diffReader = diffReader;
        }

        @Override
        public V read(StreamInput in, K key) throws IOException {
            return reader.read(in);
        }

        @Override
        public Diff<V> readDiff(StreamInput in, K key) throws IOException {
            return diffReader.read(in);
        }
    }

    /**
     * Implementation of ValueSerializer that serializes immutable sets
     *
     * @param <K> type of map key
     *
     * @opensearch.internal
     */
    public static class StringSetValueSerializer<K> extends NonDiffableValueSerializer<K, Set<String>> {
        private static final StringSetValueSerializer INSTANCE = new StringSetValueSerializer();

        public static <K> StringSetValueSerializer<K> getInstance() {
            return INSTANCE;
        }

        @Override
        public void write(Set<String> value, StreamOutput out) throws IOException {
            out.writeStringCollection(value);
        }

        @Override
        public Set<String> read(StreamInput in, K key) throws IOException {
            return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(in.readStringArray())));
        }
    }
}

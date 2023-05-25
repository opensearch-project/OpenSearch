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

package org.opensearch.cluster;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.opensearch.Version;
import org.opensearch.common.collect.ImmutableOpenIntMap;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;

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
public final class ProtobufDiffableUtils {
    private ProtobufDiffableUtils() {}

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
     * Calculates diff between two ImmutableOpenMaps of Diffable objects
    */
    public static <K, T extends ProtobufDiffable<T>> MapDiff<K, T, ImmutableOpenMap<K, T>> diff(
        ImmutableOpenMap<K, T> before,
        ImmutableOpenMap<K, T> after,
        KeySerializer<K> keySerializer
    ) {
        assert after != null && before != null;
        return new ImmutableOpenMapDiff<>(before, after, keySerializer, DiffableValueSerializer.getWriteOnlyInstance());
    }

    /**
     * Calculates diff between two ImmutableOpenMaps of non-diffable objects
    */
    public static <K, T> MapDiff<K, T, ImmutableOpenMap<K, T>> diff(
        ImmutableOpenMap<K, T> before,
        ImmutableOpenMap<K, T> after,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) {
        assert after != null && before != null;
        return new ImmutableOpenMapDiff<>(before, after, keySerializer, valueSerializer);
    }

    /**
     * Calculates diff between two ImmutableOpenIntMaps of Diffable objects
    */
    public static <T extends ProtobufDiffable<T>> MapDiff<Integer, T, ImmutableOpenIntMap<T>> diff(
        ImmutableOpenIntMap<T> before,
        ImmutableOpenIntMap<T> after,
        KeySerializer<Integer> keySerializer
    ) {
        assert after != null && before != null;
        return new ImmutableOpenIntMapDiff<>(before, after, keySerializer, DiffableValueSerializer.getWriteOnlyInstance());
    }

    /**
     * Calculates diff between two ImmutableOpenIntMaps of non-diffable objects
    */
    public static <T> MapDiff<Integer, T, ImmutableOpenIntMap<T>> diff(
        ImmutableOpenIntMap<T> before,
        ImmutableOpenIntMap<T> after,
        KeySerializer<Integer> keySerializer,
        ValueSerializer<Integer, T> valueSerializer
    ) {
        assert after != null && before != null;
        return new ImmutableOpenIntMapDiff<>(before, after, keySerializer, valueSerializer);
    }

    /**
     * Calculates diff between two Maps of Diffable objects.
    */
    public static <K, T extends ProtobufDiffable<T>> MapDiff<K, T, Map<K, T>> diff(
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
     * Loads an object that represents difference between two ImmutableOpenMaps
    */
    public static <K, T> MapDiff<K, T, ImmutableOpenMap<K, T>> readImmutableOpenMapDiff(
        CodedInputStream in,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) throws IOException {
        return new ImmutableOpenMapDiff<>(in, keySerializer, valueSerializer);
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenMaps
    */
    public static <T> MapDiff<Integer, T, ImmutableOpenIntMap<T>> readImmutableOpenIntMapDiff(
        CodedInputStream in,
        KeySerializer<Integer> keySerializer,
        ValueSerializer<Integer, T> valueSerializer
    ) throws IOException {
        return new ImmutableOpenIntMapDiff<>(in, keySerializer, valueSerializer);
    }

    /**
     * Loads an object that represents difference between two Maps of Diffable objects
    */
    public static <K, T> MapDiff<K, T, Map<K, T>> readJdkMapDiff(
        CodedInputStream in,
        KeySerializer<K> keySerializer,
        ValueSerializer<K, T> valueSerializer
    ) throws IOException {
        return new JdkMapDiff<>(in, keySerializer, valueSerializer);
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenMaps of Diffable objects using Diffable proto object
    */
    public static <K, T extends ProtobufDiffable<T>> MapDiff<K, T, ImmutableOpenMap<K, T>> readImmutableOpenMapDiff(
        CodedInputStream in,
        KeySerializer<K> keySerializer,
        DiffableValueReader<K, T> diffableValueReader
    ) throws IOException {
        return new ImmutableOpenMapDiff<>(in, keySerializer, diffableValueReader);
    }

    /**
     * Loads an object that represents difference between two ImmutableOpenIntMaps of Diffable objects using Diffable proto object
    */
    public static <T extends ProtobufDiffable<T>> MapDiff<Integer, T, ImmutableOpenIntMap<T>> readImmutableOpenIntMapDiff(
        CodedInputStream in,
        KeySerializer<Integer> keySerializer,
        ProtobufWriteable.Reader<T> reader,
        ProtobufWriteable.Reader<ProtobufDiff<T>> diffReader
    ) throws IOException {
        return new ImmutableOpenIntMapDiff<>(in, keySerializer, new DiffableValueReader<>(reader, diffReader));
    }

    /**
     * Loads an object that represents difference between two Maps of Diffable objects using Diffable proto object
    */
    public static <K, T extends ProtobufDiffable<T>> MapDiff<K, T, Map<K, T>> readJdkMapDiff(
        CodedInputStream in,
        KeySerializer<K> keySerializer,
        ProtobufWriteable.Reader<T> reader,
        ProtobufWriteable.Reader<ProtobufDiff<T>> diffReader
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

        protected JdkMapDiff(CodedInputStream in, KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer)
            throws IOException {
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

            for (Map.Entry<K, ProtobufDiff<T>> diff : diffs.entrySet()) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<K, T> upsert : upserts.entrySet()) {
                builder.put(upsert.getKey(), upsert.getValue());
            }
            return builder;
        }
    }

    /**
     * Represents differences between two ImmutableOpenMap of (possibly diffable) objects
    *
    * @param <T> the object type
    *
    * @opensearch.internal
    */
    public static class ImmutableOpenMapDiff<K, T> extends MapDiff<K, T, ImmutableOpenMap<K, T>> {

        protected ImmutableOpenMapDiff(CodedInputStream in, KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer)
            throws IOException {
            super(in, keySerializer, valueSerializer);
        }

        private ImmutableOpenMapDiff(
            KeySerializer<K> keySerializer,
            ValueSerializer<K, T> valueSerializer,
            List<K> deletes,
            Map<K, ProtobufDiff<T>> diffs,
            Map<K, T> upserts
        ) {
            super(keySerializer, valueSerializer, deletes, diffs, upserts);
        }

        public ImmutableOpenMapDiff(
            ImmutableOpenMap<K, T> before,
            ImmutableOpenMap<K, T> after,
            KeySerializer<K> keySerializer,
            ValueSerializer<K, T> valueSerializer
        ) {
            super(keySerializer, valueSerializer);
            assert after != null && before != null;

            for (ObjectCursor<K> key : before.keys()) {
                if (!after.containsKey(key.value)) {
                    deletes.add(key.value);
                }
            }

            for (ObjectObjectCursor<K, T> partIter : after) {
                T beforePart = before.get(partIter.key);
                if (beforePart == null) {
                    upserts.put(partIter.key, partIter.value);
                } else if (partIter.value.equals(beforePart) == false) {
                    if (valueSerializer.supportsDiffableValues()) {
                        diffs.put(partIter.key, valueSerializer.diff(partIter.value, beforePart));
                    } else {
                        upserts.put(partIter.key, partIter.value);
                    }
                }
            }
        }

        /**
         * Returns a new diff map with the given key removed, does not modify the invoking instance.
        * If the key does not exist in the diff map, the same instance is returned.
        */
        public ImmutableOpenMapDiff<K, T> withKeyRemoved(K key) {
            if (this.diffs.containsKey(key) == false && this.upserts.containsKey(key) == false) {
                return this;
            }
            Map<K, ProtobufDiff<T>> newDiffs = new HashMap<>(this.diffs);
            newDiffs.remove(key);
            Map<K, T> newUpserts = new HashMap<>(this.upserts);
            newUpserts.remove(key);
            return new ImmutableOpenMapDiff<>(this.keySerializer, this.valueSerializer, this.deletes, newDiffs, newUpserts);
        }

        @Override
        public ImmutableOpenMap<K, T> apply(ImmutableOpenMap<K, T> map) {
            ImmutableOpenMap.Builder<K, T> builder = ImmutableOpenMap.builder();
            builder.putAll(map);

            for (K part : deletes) {
                builder.remove(part);
            }

            for (Map.Entry<K, ProtobufDiff<T>> diff : diffs.entrySet()) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<K, T> upsert : upserts.entrySet()) {
                builder.put(upsert.getKey(), upsert.getValue());
            }
            return builder.build();
        }
    }

    /**
     * Represents differences between two ImmutableOpenIntMap of (possibly diffable) objects
    *
    * @param <T> the object type
    *
    * @opensearch.internal
    */
    private static class ImmutableOpenIntMapDiff<T> extends MapDiff<Integer, T, ImmutableOpenIntMap<T>> {

        protected ImmutableOpenIntMapDiff(
            CodedInputStream in,
            KeySerializer<Integer> keySerializer,
            ValueSerializer<Integer, T> valueSerializer
        ) throws IOException {
            super(in, keySerializer, valueSerializer);
        }

        ImmutableOpenIntMapDiff(
            ImmutableOpenIntMap<T> before,
            ImmutableOpenIntMap<T> after,
            KeySerializer<Integer> keySerializer,
            ValueSerializer<Integer, T> valueSerializer
        ) {
            super(keySerializer, valueSerializer);
            assert after != null && before != null;

            for (IntCursor key : before.keys()) {
                if (!after.containsKey(key.value)) {
                    deletes.add(key.value);
                }
            }

            for (IntObjectCursor<T> partIter : after) {
                T beforePart = before.get(partIter.key);
                if (beforePart == null) {
                    upserts.put(partIter.key, partIter.value);
                } else if (partIter.value.equals(beforePart) == false) {
                    if (valueSerializer.supportsDiffableValues()) {
                        diffs.put(partIter.key, valueSerializer.diff(partIter.value, beforePart));
                    } else {
                        upserts.put(partIter.key, partIter.value);
                    }
                }
            }
        }

        @Override
        public ImmutableOpenIntMap<T> apply(ImmutableOpenIntMap<T> map) {
            ImmutableOpenIntMap.Builder<T> builder = ImmutableOpenIntMap.builder();
            builder.putAll(map);

            for (Integer part : deletes) {
                builder.remove(part);
            }

            for (Map.Entry<Integer, ProtobufDiff<T>> diff : diffs.entrySet()) {
                builder.put(diff.getKey(), diff.getValue().apply(builder.get(diff.getKey())));
            }

            for (Map.Entry<Integer, T> upsert : upserts.entrySet()) {
                builder.put(upsert.getKey(), upsert.getValue());
            }
            return builder.build();
        }
    }

    /**
     * Represents differences between two maps of objects and is used as base class for different map implementations.
    *
    * Implements serialization. How differences are applied is left to subclasses.
    *
    * @param <K> the type of map keys
    * @param <T> the type of map values
    * @param <M> the map implementation type
    *
    * @opensearch.internal
    */
    public abstract static class MapDiff<K, T, M> implements ProtobufDiff<M> {

        protected final List<K> deletes;
        protected final Map<K, ProtobufDiff<T>> diffs; // incremental updates
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
            Map<K, ProtobufDiff<T>> diffs,
            Map<K, T> upserts
        ) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.deletes = deletes;
            this.diffs = diffs;
            this.upserts = upserts;
        }

        protected MapDiff(CodedInputStream in, KeySerializer<K> keySerializer, ValueSerializer<K, T> valueSerializer) throws IOException {
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            deletes = protobufStreamInput.readList(keySerializer::readKey);
            int diffsCount = protobufStreamInput.readVInt();
            diffs = diffsCount == 0 ? Collections.emptyMap() : new HashMap<>(diffsCount);
            for (int i = 0; i < diffsCount; i++) {
                K key = keySerializer.readKey(in);
                ProtobufDiff<T> diff = valueSerializer.readDiff(in, key);
                diffs.put(key, diff);
            }
            int upsertsCount = protobufStreamInput.readVInt();
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
        public Map<K, ProtobufDiff<T>> getDiffs() {
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
        public void writeTo(CodedOutputStream out) throws IOException {
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeCollection(deletes, (o, v) -> keySerializer.writeKey(v, o));
            Version version = protobufStreamOutput.getVersion();
            // filter out custom states not supported by the other node
            int diffCount = 0;
            for (ProtobufDiff<T> diff : diffs.values()) {
                if (valueSerializer.supportsVersion(diff, version)) {
                    diffCount++;
                }
            }
            out.writeInt32NoTag(diffCount);
            for (Map.Entry<K, ProtobufDiff<T>> entry : diffs.entrySet()) {
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
            out.writeInt32NoTag(upsertsCount);
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
        void writeKey(K key, CodedOutputStream out) throws IOException;

        K readKey(CodedInputStream in) throws IOException;
    }

    /**
     * Serializes String keys of a map
    *
    * @opensearch.internal
    */
    private static final class StringKeySerializer implements KeySerializer<String> {
        private static final StringKeySerializer INSTANCE = new StringKeySerializer();

        @Override
        public void writeKey(String key, CodedOutputStream out) throws IOException {
            out.writeStringNoTag(key);
        }

        @Override
        public String readKey(CodedInputStream in) throws IOException {
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
        public void writeKey(Integer key, CodedOutputStream out) throws IOException {
            out.writeInt32NoTag(key);
        }

        @Override
        public Integer readKey(CodedInputStream in) throws IOException {
            return in.readInt32();
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
        public void writeKey(Integer key, CodedOutputStream out) throws IOException {
            if (key < 0) {
                throw new IllegalArgumentException("Map key [" + key + "] must be positive");
            }
            out.writeInt32NoTag(key);
        }

        @Override
        public Integer readKey(CodedInputStream in) throws IOException {
            return in.readInt32();
        }
    }

    /**
     * Provides read and write operations to serialize map values.
    * Reading of values can be made dependent on map key.
    *
    * Also provides operations to distinguish whether map values are diffable.
    *
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
        void write(V value, CodedOutputStream out) throws IOException;

        /**
         * Reads value from stream. Reading operation can be made dependent on map key.
        */
        V read(CodedInputStream in, K key) throws IOException;

        /**
         * Whether this serializer supports diffable values
        */
        boolean supportsDiffableValues();

        /**
         * Whether this serializer supports the version of the output stream
        */
        default boolean supportsVersion(ProtobufDiff<V> value, Version version) {
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
        ProtobufDiff<V> diff(V value, V beforePart);

        /**
         * Writes value as diff to stream if this serializer supports diffable values
        */
        void writeDiff(ProtobufDiff<V> value, CodedOutputStream out) throws IOException;

        /**
         * Reads value as diff from stream if this serializer supports diffable values.
        * Reading operation can be made dependent on map key.
        */
        ProtobufDiff<V> readDiff(CodedInputStream in, K key) throws IOException;
    }

    /**
     * Serializer for Diffable map values. Needs to implement read and readDiff methods.
    *
    * @param <K> type of map keys
    * @param <V> type of map values
    *
    * @opensearch.internal
    */
    public abstract static class DiffableValueSerializer<K, V extends ProtobufDiffable<V>> implements ValueSerializer<K, V> {
        private static final DiffableValueSerializer WRITE_ONLY_INSTANCE = new DiffableValueSerializer() {
            @Override
            public Object read(CodedInputStream in, Object key) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public ProtobufDiff<Object> readDiff(CodedInputStream in, Object key) throws IOException {
                throw new UnsupportedOperationException();
            }
        };

        private static <K, V extends ProtobufDiffable<V>> DiffableValueSerializer<K, V> getWriteOnlyInstance() {
            return WRITE_ONLY_INSTANCE;
        }

        @Override
        public boolean supportsDiffableValues() {
            return true;
        }

        @Override
        public ProtobufDiff<V> diff(V value, V beforePart) {
            return value.diff(beforePart);
        }

        @Override
        public void write(V value, CodedOutputStream out) throws IOException {
            value.writeTo(out);
        }

        public void writeDiff(ProtobufDiff<V> value, CodedOutputStream out) throws IOException {
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
        @Override
        public boolean supportsDiffableValues() {
            return false;
        }

        @Override
        public ProtobufDiff<V> diff(V value, V beforePart) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDiff(ProtobufDiff<V> value, CodedOutputStream out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProtobufDiff<V> readDiff(CodedInputStream in, K key) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Implementation of the ValueSerializer that wraps value and diff readers.
    *
    * Note: this implementation is ignoring the key.
    *
    * @opensearch.internal
    */
    public static class DiffableValueReader<K, V extends ProtobufDiffable<V>> extends DiffableValueSerializer<K, V> {
        private final ProtobufWriteable.Reader<V> reader;
        private final ProtobufWriteable.Reader<ProtobufDiff<V>> diffReader;

        public DiffableValueReader(ProtobufWriteable.Reader<V> reader, ProtobufWriteable.Reader<ProtobufDiff<V>> diffReader) {
            this.reader = reader;
            this.diffReader = diffReader;
        }

        @Override
        public V read(CodedInputStream in, K key) throws IOException {
            return reader.read(in);
        }

        @Override
        public ProtobufDiff<V> readDiff(CodedInputStream in, K key) throws IOException {
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
        public void write(Set<String> value, CodedOutputStream out) throws IOException {
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeStringCollection(value);
        }

        @Override
        public Set<String> read(CodedInputStream in, K key) throws IOException {
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
            return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(protobufStreamInput.readStringArray())));
        }
    }
}

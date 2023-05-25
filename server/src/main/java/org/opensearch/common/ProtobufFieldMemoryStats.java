/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * A reusable class to encode {@code field -&gt; memory size} mappings
*
* @opensearch.internal
*/
public final class ProtobufFieldMemoryStats implements ProtobufWriteable, Iterable<ObjectLongCursor<String>> {

    private final ObjectLongHashMap<String> stats;

    /**
     * Creates a new ProtobufFieldMemoryStats instance
    */
    public ProtobufFieldMemoryStats(ObjectLongHashMap<String> stats) {
        this.stats = Objects.requireNonNull(stats, "status must be non-null");
        assert !stats.containsKey(null);
    }

    /**
     * Creates a new ProtobufFieldMemoryStats instance from a stream
    */
    public ProtobufFieldMemoryStats(CodedInputStream input) throws IOException {
        int size = input.readInt32();
        stats = new ObjectLongHashMap<>(size);
        for (int i = 0; i < size; i++) {
            stats.put(input.readString(), input.readInt64());
        }
    }

    /**
     * Adds / merges the given field memory stats into this stats instance
    */
    public void add(ProtobufFieldMemoryStats fieldMemoryStats) {
        for (ObjectLongCursor<String> entry : fieldMemoryStats.stats) {
            stats.addTo(entry.key, entry.value);
        }
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(stats.size());
        for (ObjectLongCursor<String> entry : stats) {
            out.writeStringNoTag(entry.key);
            out.writeInt64NoTag(entry.value);
        }
    }

    /**
     * Generates x-content into the given builder for each of the fields in this stats instance
    * @param builder the builder to generated on
    * @param key the top level key for this stats object
    * @param rawKey the raw byte key for each of the fields byte sizes
    * @param readableKey the readable key for each of the fields byte sizes
    */
    public void toXContent(XContentBuilder builder, String key, String rawKey, String readableKey) throws IOException {
        builder.startObject(key);
        for (ObjectLongCursor<String> entry : stats) {
            builder.startObject(entry.key);
            builder.humanReadableField(rawKey, readableKey, new ByteSizeValue(entry.value));
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Creates a deep copy of this stats instance
    */
    public ProtobufFieldMemoryStats copy() {
        return new ProtobufFieldMemoryStats(stats.clone());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProtobufFieldMemoryStats that = (ProtobufFieldMemoryStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public Iterator<ObjectLongCursor<String>> iterator() {
        return stats.iterator();
    }

    /**
     * Returns the fields value in bytes or <code>0</code> if it's not present in the stats
    */
    public long get(String field) {
        return stats.get(field);
    }

    /**
     * Returns <code>true</code> iff the given field is in the stats
    */
    public boolean containsField(String field) {
        return stats.containsKey(field);
    }
}

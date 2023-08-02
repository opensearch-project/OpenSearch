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

package org.opensearch.common;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A reusable class to encode {@code field -&gt; memory size} mappings
 *
 * @opensearch.internal
 */
public final class FieldMemoryStats implements Writeable, Iterable<Map.Entry<String, Long>> {

    private final Map<String, Long> stats;

    /**
     * Creates a new FieldMemoryStats instance
     */
    public FieldMemoryStats(Map<String, Long> stats) {
        this.stats = Objects.requireNonNull(stats, "status must be non-null");
        assert stats.containsKey(null) == false;
    }

    /**
     * Creates a new FieldMemoryStats instance from a stream
     */
    public FieldMemoryStats(StreamInput input) throws IOException {
        stats = input.readMap(StreamInput::readString, StreamInput::readVLong);
    }

    /**
     * Adds / merges the given field memory stats into this stats instance
     */
    public void add(FieldMemoryStats fieldMemoryStats) {
        for (final var entry : fieldMemoryStats.stats.entrySet()) {
            stats.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, StreamOutput::writeVLong);
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
        for (final var entry : stats.entrySet()) {
            builder.startObject(entry.getKey());
            builder.humanReadableField(rawKey, readableKey, new ByteSizeValue(entry.getValue()));
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Creates a deep copy of this stats instance
     */
    public FieldMemoryStats copy() {
        return new FieldMemoryStats(new HashMap<>(stats));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMemoryStats that = (FieldMemoryStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public Iterator<Map.Entry<String, Long>> iterator() {
        return stats.entrySet().iterator();
    }

    /**
     * Returns the fields value in bytes or <code>0</code> if it's not present in the stats
     */
    public long get(String field) {
        return stats.getOrDefault(field, 0L);
    }

    /**
     * Returns <code>true</code> iff the given field is in the stats
     */
    public boolean containsField(String field) {
        return stats.containsKey(field);
    }
}

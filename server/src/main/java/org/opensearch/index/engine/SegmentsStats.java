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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.engine;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracker for segment stats
 *
 * @opensearch.internal
 */
public class SegmentsStats implements Writeable, ToXContentFragment {

    private long count;
    private long indexWriterMemoryInBytes;
    private long versionMapMemoryInBytes;
    private long maxUnsafeAutoIdTimestamp = Long.MIN_VALUE;
    private long bitsetMemoryInBytes;
    private final Map<String, Long> fileSizes;

    private static final ByteSizeValue ZERO_BYTE_SIZE_VALUE = new ByteSizeValue(0L);

    /*
     * A map to provide a best-effort approach describing Lucene index files.
     *
     * Ideally this should be in sync to what the current version of Lucene is using, but it's harmless to leave extensions out,
     * they'll just miss a proper description in the stats
     */
    private static final Map<String, String> FILE_DESCRIPTIONS = Map.ofEntries(
        Map.entry("si", "Segment Info"),
        Map.entry("fnm", "Fields"),
        Map.entry("fdx", "Field Index"),
        Map.entry("fdt", "Field Data"),
        Map.entry("tim", "Term Dictionary"),
        Map.entry("tip", "Term Index"),
        Map.entry("doc", "Frequencies"),
        Map.entry("pos", "Positions"),
        Map.entry("pay", "Payloads"),
        Map.entry("nvd", "Norms"),
        Map.entry("nvm", "Norms"),
        Map.entry("dii", "Points"),
        Map.entry("dim", "Points"),
        Map.entry("dvd", "DocValues"),
        Map.entry("dvm", "DocValues"),
        Map.entry("tvx", "Term Vector Index"),
        Map.entry("tvd", "Term Vector Documents"),
        Map.entry("tvf", "Term Vector Fields"),
        Map.entry("liv", "Live Documents")
    );

    public SegmentsStats() {
        fileSizes = new HashMap<>();
    }

    public SegmentsStats(StreamInput in) throws IOException {
        count = in.readVLong();
        // the following was removed in Lucene 9 (https://issues.apache.org/jira/browse/LUCENE-9387)
        // retain for bwc only (todo: remove in OpenSearch 3)
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readLong(); // estimated segment memory
            in.readLong(); // estimated terms memory
            in.readLong(); // estimated stored fields memory
            in.readLong(); // estimated term vector memory
            in.readLong(); // estimated norms memory
            in.readLong(); // estimated points memory
            in.readLong(); // estimated doc values memory
        }
        indexWriterMemoryInBytes = in.readLong();
        versionMapMemoryInBytes = in.readLong();
        bitsetMemoryInBytes = in.readLong();
        maxUnsafeAutoIdTimestamp = in.readLong();
        fileSizes = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    public void add(long count) {
        this.count += count;
    }

    public void addIndexWriterMemoryInBytes(long indexWriterMemoryInBytes) {
        this.indexWriterMemoryInBytes += indexWriterMemoryInBytes;
    }

    public void addVersionMapMemoryInBytes(long versionMapMemoryInBytes) {
        this.versionMapMemoryInBytes += versionMapMemoryInBytes;
    }

    void updateMaxUnsafeAutoIdTimestamp(long maxUnsafeAutoIdTimestamp) {
        this.maxUnsafeAutoIdTimestamp = Math.max(maxUnsafeAutoIdTimestamp, this.maxUnsafeAutoIdTimestamp);
    }

    public void addBitsetMemoryInBytes(long bitsetMemoryInBytes) {
        this.bitsetMemoryInBytes += bitsetMemoryInBytes;
    }

    public void addFileSizes(final Map<String, Long> newFileSizes) {
        newFileSizes.forEach((k, v) -> this.fileSizes.merge(k, v, (a, b) -> {
            assert a != null;
            assert b != null;
            return Math.addExact(a, b);
        }));
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        updateMaxUnsafeAutoIdTimestamp(mergeStats.maxUnsafeAutoIdTimestamp);
        add(mergeStats.count);
        addIndexWriterMemoryInBytes(mergeStats.indexWriterMemoryInBytes);
        addVersionMapMemoryInBytes(mergeStats.versionMapMemoryInBytes);
        addBitsetMemoryInBytes(mergeStats.bitsetMemoryInBytes);
        addFileSizes(mergeStats.fileSizes);
    }

    /**
     * The number of segments.
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Estimation of the memory usage by index writer
     */
    public long getIndexWriterMemoryInBytes() {
        return this.indexWriterMemoryInBytes;
    }

    public ByteSizeValue getIndexWriterMemory() {
        return new ByteSizeValue(indexWriterMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by version map
     */
    public long getVersionMapMemoryInBytes() {
        return this.versionMapMemoryInBytes;
    }

    public ByteSizeValue getVersionMapMemory() {
        return new ByteSizeValue(versionMapMemoryInBytes);
    }

    /**
     * Estimation of how much the cached bit sets are taking. (which nested and p/c rely on)
     */
    public long getBitsetMemoryInBytes() {
        return bitsetMemoryInBytes;
    }

    public ByteSizeValue getBitsetMemory() {
        return new ByteSizeValue(bitsetMemoryInBytes);
    }

    /** Returns mapping of file names to their size (only used in tests) */
    public Map<String, Long> getFileSizes() {
        return Collections.unmodifiableMap(this.fileSizes);
    }

    /**
     * Returns the max timestamp that is used to de-optimize documents with auto-generated IDs in the engine.
     * This is used to ensure we don't add duplicate documents when we assume an append only case based on auto-generated IDs
     */
    public long getMaxUnsafeAutoIdTimestamp() {
        return maxUnsafeAutoIdTimestamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENTS);
        builder.field(Fields.COUNT, count);
        builder.humanReadableField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.TERMS_MEMORY_IN_BYTES, Fields.TERMS_MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.STORED_FIELDS_MEMORY_IN_BYTES, Fields.STORED_FIELDS_MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.TERM_VECTORS_MEMORY_IN_BYTES, Fields.TERM_VECTORS_MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.NORMS_MEMORY_IN_BYTES, Fields.NORMS_MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.POINTS_MEMORY_IN_BYTES, Fields.POINTS_MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.DOC_VALUES_MEMORY_IN_BYTES, Fields.DOC_VALUES_MEMORY, ZERO_BYTE_SIZE_VALUE);
        builder.humanReadableField(Fields.INDEX_WRITER_MEMORY_IN_BYTES, Fields.INDEX_WRITER_MEMORY, getIndexWriterMemory());
        builder.humanReadableField(Fields.VERSION_MAP_MEMORY_IN_BYTES, Fields.VERSION_MAP_MEMORY, getVersionMapMemory());
        builder.humanReadableField(Fields.FIXED_BIT_SET_MEMORY_IN_BYTES, Fields.FIXED_BIT_SET, getBitsetMemory());
        builder.field(Fields.MAX_UNSAFE_AUTO_ID_TIMESTAMP, maxUnsafeAutoIdTimestamp);
        builder.startObject(Fields.FILE_SIZES);
        for (Map.Entry<String, Long> entry : fileSizes.entrySet()) {
            builder.startObject(entry.getKey());
            builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(entry.getValue()));
            builder.field(Fields.DESCRIPTION, FILE_DESCRIPTIONS.getOrDefault(entry.getKey(), "Others"));
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Fields for segment statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEGMENTS = "segments";
        static final String COUNT = "count";
        static final String MEMORY = "memory";
        static final String MEMORY_IN_BYTES = "memory_in_bytes";
        static final String TERMS_MEMORY = "terms_memory";
        static final String TERMS_MEMORY_IN_BYTES = "terms_memory_in_bytes";
        static final String STORED_FIELDS_MEMORY = "stored_fields_memory";
        static final String STORED_FIELDS_MEMORY_IN_BYTES = "stored_fields_memory_in_bytes";
        static final String TERM_VECTORS_MEMORY = "term_vectors_memory";
        static final String TERM_VECTORS_MEMORY_IN_BYTES = "term_vectors_memory_in_bytes";
        static final String NORMS_MEMORY = "norms_memory";
        static final String NORMS_MEMORY_IN_BYTES = "norms_memory_in_bytes";
        static final String POINTS_MEMORY = "points_memory";
        static final String POINTS_MEMORY_IN_BYTES = "points_memory_in_bytes";
        static final String DOC_VALUES_MEMORY = "doc_values_memory";
        static final String DOC_VALUES_MEMORY_IN_BYTES = "doc_values_memory_in_bytes";
        static final String INDEX_WRITER_MEMORY = "index_writer_memory";
        static final String INDEX_WRITER_MEMORY_IN_BYTES = "index_writer_memory_in_bytes";
        static final String VERSION_MAP_MEMORY = "version_map_memory";
        static final String VERSION_MAP_MEMORY_IN_BYTES = "version_map_memory_in_bytes";
        static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP = "max_unsafe_auto_id_timestamp";
        static final String FIXED_BIT_SET = "fixed_bit_set";
        static final String FIXED_BIT_SET_MEMORY_IN_BYTES = "fixed_bit_set_memory_in_bytes";
        static final String FILE_SIZES = "file_sizes";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String DESCRIPTION = "description";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        if (out.getVersion().before(Version.V_2_0_0)) {
            // the following was removed in Lucene 9 (https://issues.apache.org/jira/browse/LUCENE-9387)
            // retain the following for bwc only (todo: remove in OpenSearch 3)
            out.writeLong(0L); // estimated memory
            out.writeLong(0L); // estimated terms memory
            out.writeLong(0L); // estimated stored fields memory
            out.writeLong(0L); // estimated term vector memory
            out.writeLong(0L); // estimated norms memory
            out.writeLong(0L); // estimated points memory
            out.writeLong(0L); // estimated doc values memory
        }
        out.writeLong(indexWriterMemoryInBytes);
        out.writeLong(versionMapMemoryInBytes);
        out.writeLong(bitsetMemoryInBytes);
        out.writeLong(maxUnsafeAutoIdTimestamp);
        out.writeMap(this.fileSizes, StreamOutput::writeString, StreamOutput::writeLong);
    }

    public void clearFileSizes() {
        fileSizes.clear();
    }

    /**
     * Used only for deprecating memory tracking in REST interface
     * todo remove in OpenSearch 3.0
     * @deprecated
     */
    @Deprecated
    public ByteSizeValue getZeroMemory() {
        return ZERO_BYTE_SIZE_VALUE;
    }
}

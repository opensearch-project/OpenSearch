/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Concrete implementation of {@link CatalogSnapshot} for the composite multi-format engine.
 * Holds segments grouped by data format, supports searchable file lookups across formats,
 * and tracks snapshot metadata including user data and writer generation.
 */
@ExperimentalApi
public class DataformatAwareCatalogSnapshot extends CatalogSnapshot {

    private final long id;
    private final List<Segment> segments;
    private final long lastWriterGeneration;
    private Map<String, String> userData;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Constructs a new DataformatAwareCatalogSnapshot.
     *
     * @param id the unique snapshot identifier
     * @param generation the monotonically increasing generation number
     * @param version the schema version for serialization compatibility
     * @param segments the list of segments in this snapshot
     * @param lastWriterGeneration the generation of the last writer that contributed to this snapshot
     * @param userData user-defined metadata key-value pairs
     */
    DataformatAwareCatalogSnapshot(
        long id,
        long generation,
        long version,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData
    ) {
        super("dataformat_aware_catalog_snapshot", generation, version);
        this.id = id;
        this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
        this.lastWriterGeneration = lastWriterGeneration;
        this.userData = Map.copyOf(userData);
    }

    /**
     * Constructs a DataformatAwareCatalogSnapshot from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @param directoryResolver function that maps a data format name to its directory path
     * @throws IOException if an I/O error occurs
     */
    DataformatAwareCatalogSnapshot(StreamInput in, Function<String, String> directoryResolver) throws IOException {
        super(in);

        this.userData = in.readMap(StreamInput::readString, StreamInput::readString);

        this.id = in.readLong();
        this.lastWriterGeneration = in.readLong();

        int segmentCount = in.readVInt();
        List<Segment> segmentList = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segmentList.add(new Segment(in, directoryResolver));
        }
        this.segments = Collections.unmodifiableList(segmentList);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public List<Segment> getSegments() {
        return segments;
    }

    @Override
    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        List<WriterFileSet> result = new ArrayList<>();
        for (Segment segment : segments) {
            WriterFileSet writerFileSet = segment.dfGroupedSearchableFiles().get(dataFormat);
            if (writerFileSet != null) {
                result.add(writerFileSet);
            }
        }
        return result;
    }

    @Override
    public Set<String> getDataFormats() {
        Set<String> formats = new HashSet<>();
        for (Segment segment : segments) {
            formats.addAll(segment.dfGroupedSearchableFiles().keySet());
        }
        return formats;
    }

    @Override
    public long getLastWriterGeneration() {
        return lastWriterGeneration;
    }

    @Override
    public Map<String, String> getUserData() {
        return userData;
    }

    @Override
    public void setUserData(Map<String, String> userData) {
        this.userData = Map.copyOf(userData);
    }

    @Override
    public String serializeToString() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            this.writeTo(out);
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }

    /**
     * Deserializes a {@link DataformatAwareCatalogSnapshot} from a Base64-encoded binary string.
     *
     * @param serializedData the Base64 string produced by {@link #serializeToString()}
     * @param directoryResolver function that maps a data format name to its directory path
     * @return a reconstructed {@link DataformatAwareCatalogSnapshot}
     * @throws IOException if the data is malformed or missing required fields
     */
    public static DataformatAwareCatalogSnapshot deserializeFromString(String serializedData, Function<String, String> directoryResolver)
        throws IOException {
        if (serializedData == null || serializedData.isEmpty()) {
            throw new IOException("Cannot deserialize DataformatAwareCatalogSnapshot: input is null or empty");
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(serializedData);
            try (BytesStreamInput in = new BytesStreamInput(bytes)) {
                return new DataformatAwareCatalogSnapshot(in, directoryResolver);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to deserialize DataformatAwareCatalogSnapshot: " + e.getMessage(), e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(userData, StreamOutput::writeString, StreamOutput::writeString);
        out.writeLong(id);
        out.writeLong(lastWriterGeneration);
        out.writeVInt(segments.size());
        for (Segment seg : segments) {
            seg.writeTo(out);
        }
    }

    @Override
    public DataformatAwareCatalogSnapshot clone() {
        return new DataformatAwareCatalogSnapshot(id, generation, version, segments, lastWriterGeneration, userData);
    }

    @Override
    protected void closeInternal() {
        closed.set(true);
    }

    /**
     * Returns {@code true} if {@link #closeInternal()} has been invoked (ref count reached zero).
     * This method is intended for testing only.
     */
    public boolean isClosed() {
        return closed.get();
    }
}

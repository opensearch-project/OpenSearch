/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A thin adapter that wraps Lucene's {@link SegmentInfos} as a {@link CatalogSnapshot}.
 * Used by {@code InternalEngine} (the standard single-format Lucene engine) to participate
 * in the {@link CatalogSnapshot} abstraction without requiring composite engine infrastructure.
 *
 * <p>Multi-format methods ({@link #getSegments()}, {@link #getSearchableFiles(String)},
 * {@link #getDataFormats()}, {@link #serializeToString()}) throw {@link UnsupportedOperationException}
 * since Lucene-only engines do not use composite segments.</p>
 */
@ExperimentalApi
public class SegmentInfosCatalogSnapshot extends CatalogSnapshot {

    private static final String CATALOG_SNAPSHOT_KEY = "_segment_infos_catalog_snapshot_";

    private final SegmentInfos segmentInfos;

    /**
     * Constructs a new SegmentInfosCatalogSnapshot wrapping the given SegmentInfos.
     *
     * @param segmentInfos the Lucene SegmentInfos to wrap
     */
    public SegmentInfosCatalogSnapshot(SegmentInfos segmentInfos) {
        super(CATALOG_SNAPSHOT_KEY + segmentInfos.getGeneration(), segmentInfos.getGeneration(), segmentInfos.getVersion());
        this.segmentInfos = segmentInfos;
    }

    /**
     * Constructs a SegmentInfosCatalogSnapshot from a {@link StreamInput} by deserializing the
     * SegmentInfos binary representation.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public SegmentInfosCatalogSnapshot(StreamInput in) throws IOException {
        super(in);
        byte[] segmentInfosBytes = in.readByteArray();
        this.segmentInfos = SegmentInfos.readCommit(
            null,
            new BufferedChecksumIndexInput(new ByteArrayIndexInput("SegmentInfos", segmentInfosBytes)),
            0L
        );
    }

    /**
     * Returns the wrapped Lucene SegmentInfos instance.
     *
     * @return the SegmentInfos
     */
    public SegmentInfos getSegmentInfos() {
        return segmentInfos;
    }

    @Override
    public long getId() {
        return generation;
    }

    @Override
    public Map<String, String> getUserData() {
        return segmentInfos.getUserData();
    }

    @Override
    public long getLastWriterGeneration() {
        return -1;
    }

    @Override
    public List<Segment> getSegments() {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support getSegments()");
    }

    @Override
    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support getSearchableFiles()");
    }

    @Override
    public Set<String> getDataFormats() {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support getDataFormats()");
    }

    @Override
    public String serializeToString() throws IOException {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support serializeToString()");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput indexOutput = new ByteBuffersIndexOutput(buffer, "", null)) {
            segmentInfos.write(indexOutput);
        }
        out.writeByteArray(buffer.toArrayCopy());
    }

    @Override
    public void setUserData(Map<String, String> userData) {
        // No-op for SegmentInfosCatalogSnapshot
    }

    @Override
    protected void closeInternal() {
        // No resources to release for SegmentInfos wrapper.
    }

    @Override
    public SegmentInfosCatalogSnapshot clone() {
        return new SegmentInfosCatalogSnapshot(segmentInfos);
    }
}

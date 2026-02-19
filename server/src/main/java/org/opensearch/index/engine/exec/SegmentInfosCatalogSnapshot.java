/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Concrete implementation of CatalogSnapshot backed by Lucene's SegmentInfos.
 * Provides a lightweight snapshot view of segment metadata without supporting
 * the full catalog snapshot functionality. Used primarily for compatibility
 * with Lucene-based operations.
 * <p>
 * Note: This implementation throws UnsupportedOperationException for several
 * catalog-specific methods as it's designed for basic segment information access only.
 *
 * @opensearch.experimental
 */
public class SegmentInfosCatalogSnapshot extends CatalogSnapshot {

    private static final String CATALOG_SNAPSHOT_KEY = "_segment_infos_catalog_snapshot_";

    private final SegmentInfos segmentInfos;

    /**
     * Constructs a catalog snapshot from Lucene SegmentInfos.
     *
     * @param segmentInfos the Lucene segment information
     */
    public SegmentInfosCatalogSnapshot(SegmentInfos segmentInfos) {
        super(CATALOG_SNAPSHOT_KEY + segmentInfos.getGeneration(), segmentInfos.getGeneration(), segmentInfos.getVersion());
        this.segmentInfos = segmentInfos;
    }

    /**
     * Constructs a SegmentInfosCatalogSnapshot from a StreamInput for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during deserialization
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
    public Collection<FileMetadata> getFileMetadataList() throws IOException {
        return segmentInfos.files(true).stream().map(file -> new FileMetadata("lucene", file)).collect(Collectors.toList());
    }

    /**
     * Returns the underlying Lucene SegmentInfos.
     *
     * @return the segment information
     */
    public SegmentInfos getSegmentInfos() {
        return segmentInfos;
    }

    @Override
    public Map<String, String> getUserData() {
        return segmentInfos.getUserData();
    }

    @Override
    public long getId() {
        return generation;
    }

    @Override
    public List<org.opensearch.index.engine.exec.coord.Segment> getSegments() {
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
    public long getLastWriterGeneration() {
        return -1;
    }

    @Override
    public String serializeToString() throws IOException {
        throw new UnsupportedOperationException("SegmentInfosCatalogSnapshot does not support serializeToString()");
    }

    @Override
    public void setIndexFileDeleterSupplier(Supplier<IndexFileReferenceManager> supplier) {
        // No-op for SegmentInfosCatalogSnapshot
    }

    @Override
    public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> catalogSnapshotMap) {
        // No-op for SegmentInfosCatalogSnapshot
    }

    @Override
    public SegmentInfosCatalogSnapshot clone() {
        return new SegmentInfosCatalogSnapshot(segmentInfos);
    }

    @Override
    protected void closeInternal() {
        // TODO no op since SegmentInfosCatalogSnapshot is not refcounted
    }

    @Override
    public void setUserData(Map<String, String> userData, boolean b) {
        // TODO no op since SegmentInfosCatalogSnapshot is not refcounted
    }
}

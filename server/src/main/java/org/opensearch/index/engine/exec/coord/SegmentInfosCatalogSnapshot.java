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
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.exec.FileMetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SegmentInfosCatalogSnapshot extends CatalogSnapshot {

    private final SegmentInfos segmentInfos;

    public SegmentInfosCatalogSnapshot(long id, long version, List<Segment> segmentList, Map<Long, CatalogSnapshot> catalogSnapshotMap, Supplier<IndexFileDeleter> indexFileDeleterSupplier, SegmentInfos segmentInfos) {
        super(id, version, segmentList, catalogSnapshotMap, indexFileDeleterSupplier);
        this.segmentInfos = segmentInfos;
    }

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
        return segmentInfos.files(true).stream().map(file -> new FileMetadata(file, "lucene")).collect(Collectors.toList());
    }

    public SegmentInfos getSegmentInfos() {
        return segmentInfos;
    }
}

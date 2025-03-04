/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Metadata object for Remote Segment
 *
 * @opensearch.api
 */
@PublicApi(since = "2.6.0")
public class RemoteSegmentMetadata {

    public static final int VERSION_ONE = 1;

    public static final int VERSION_TWO = 2;

    /**
     * Latest supported version of metadata
     */
    public static final int CURRENT_VERSION = VERSION_TWO;
    /**
     * Metadata codec
     */
    public static final String METADATA_CODEC = "segment_md";

    /**
     * Data structure holding metadata content
     */
    private final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata;

    private final byte[] segmentInfosBytes;

    private final ReplicationCheckpoint replicationCheckpoint;

    public RemoteSegmentMetadata(
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata,
        byte[] segmentInfosBytes,
        ReplicationCheckpoint replicationCheckpoint
    ) {
        this.metadata = metadata;
        this.segmentInfosBytes = segmentInfosBytes;
        this.replicationCheckpoint = replicationCheckpoint;
    }

    /**
     * Exposes underlying metadata content data structure.
     * @return {@code metadata}
     */
    public Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getMetadata() {
        return this.metadata;
    }

    public byte[] getSegmentInfosBytes() {
        return segmentInfosBytes;
    }

    public long getGeneration() {
        return replicationCheckpoint.getSegmentsGen();
    }

    public long getPrimaryTerm() {
        return replicationCheckpoint.getPrimaryTerm();
    }

    public ReplicationCheckpoint getReplicationCheckpoint() {
        return replicationCheckpoint;
    }

    /**
     * Generate {@code Map<String, String>} from {@link RemoteSegmentMetadata}
     * @return {@code Map<String, String>}
     */
    public Map<String, String> toMapOfStrings() {
        return this.metadata.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
    }

    /**
     * Generate {@link RemoteSegmentMetadata} from {@code segmentMetadata}
     * @param segmentMetadata metadata content in the form of {@code Map<String, String>}
     * @return {@link RemoteSegmentMetadata}
     */
    public static Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> fromMapOfStrings(Map<String, String> segmentMetadata) {
        return segmentMetadata.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(entry.getValue())
                )
            );
    }

    /**
     * Write always writes with the latest version of the RemoteSegmentMetadata
     * @param out file output stream which will store stream content
     * @throws IOException in case there is a problem writing the file
     */
    public void write(IndexOutput out) throws IOException {
        out.writeMapOfStrings(toMapOfStrings());
        writeCheckpointToIndexOutput(replicationCheckpoint, out);
        out.writeLong(segmentInfosBytes.length);
        out.writeBytes(segmentInfosBytes, segmentInfosBytes.length);
    }

    /**
     * Read can happen in the upgraded version of replica which needs to support all versions of RemoteSegmentMetadata
     * @param indexInput file input stream
     * @param version version of the RemoteSegmentMetadata
     * @return {@code RemoteSegmentMetadata}
     * @throws IOException in case there is a problem reading from the file input stream
     */
    public static RemoteSegmentMetadata read(IndexInput indexInput, int version) throws IOException {
        Map<String, String> metadata = indexInput.readMapOfStrings();
        final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap = RemoteSegmentMetadata
            .fromMapOfStrings(metadata);
        ReplicationCheckpoint replicationCheckpoint = readCheckpointFromIndexInput(indexInput, uploadedSegmentMetadataMap, version);
        int byteArraySize = (int) indexInput.readLong();
        byte[] segmentInfosBytes = new byte[byteArraySize];
        indexInput.readBytes(segmentInfosBytes, 0, byteArraySize);
        return new RemoteSegmentMetadata(uploadedSegmentMetadataMap, segmentInfosBytes, replicationCheckpoint);
    }

    public static void writeCheckpointToIndexOutput(ReplicationCheckpoint replicationCheckpoint, IndexOutput out) throws IOException {
        ShardId shardId = replicationCheckpoint.getShardId();
        // Write ShardId
        out.writeString(shardId.getIndex().getName());
        out.writeString(shardId.getIndex().getUUID());
        out.writeVInt(shardId.getId());
        // Write remaining checkpoint fields
        out.writeLong(replicationCheckpoint.getPrimaryTerm());
        out.writeLong(replicationCheckpoint.getSegmentsGen());
        out.writeLong(replicationCheckpoint.getSegmentInfosVersion());
        out.writeLong(replicationCheckpoint.getLength());
        out.writeString(replicationCheckpoint.getCodec());
        out.writeLong(replicationCheckpoint.getCreatedTimeStamp());
    }

    private static ReplicationCheckpoint readCheckpointFromIndexInput(
        IndexInput in,
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap,
        int version
    ) throws IOException {
        return new ReplicationCheckpoint(
            new ShardId(new Index(in.readString(), in.readString()), in.readVInt()),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readLong(),
            in.readString(),
            toStoreFileMetadata(uploadedSegmentMetadataMap),
            version >= VERSION_TWO ? in.readLong() : 0
        );
    }

    private static Map<String, StoreFileMetadata> toStoreFileMetadata(
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata
    ) {
        return metadata.entrySet()
            .stream()
            // TODO: Version here should be read from UploadedSegmentMetadata.
            .map(
                entry -> new StoreFileMetadata(entry.getKey(), entry.getValue().getLength(), entry.getValue().getChecksum(), Version.LATEST)
            )
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
    }
}

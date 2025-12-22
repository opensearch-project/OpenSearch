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
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.UploadedSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.HashMap;
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
    private final Map<FileMetadata, UploadedSegmentMetadata> metadata;

    private final byte[] segmentInfosBytes;

    private final ReplicationCheckpoint replicationCheckpoint;

    public RemoteSegmentMetadata(
        Map<FileMetadata, UploadedSegmentMetadata> metadata,
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
    public Map<String, UploadedSegmentMetadata> getMetadata() {
        return this.metadata.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().serialize(), Map.Entry::getValue));
    }

    /**
     * Exposes underlying metadata content data structure.
     * @return {@code metadata}
     */
    public Map<FileMetadata, UploadedSegmentMetadata> getMetadataV2() {
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
        return this.metadata.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().file(), entry -> entry.getValue().toString()));
    }

    /**
     * Generate {@link RemoteSegmentMetadata} from {@code segmentMetadata}
     * @param segmentMetadata metadata content in the form of {@code Map<String, String>}
     * @return {@link RemoteSegmentMetadata}
     */
    public static Map<FileMetadata, UploadedSegmentMetadata> fromMapOfStringsV2(Map<FileMetadata, String> segmentMetadata) {
        return segmentMetadata.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> UploadedSegmentMetadata.fromString(entry.getValue())
                )
            );
    }

    /**
     * Generate {@link RemoteSegmentMetadata} from {@code segmentMetadata}
     * @param segmentMetadata metadata content in the form of {@code Map<String, String>}
     * @return {@link RemoteSegmentMetadata}
     */
    public static Map<String, UploadedSegmentMetadata> fromMapOfStrings(Map<String, String> segmentMetadata) {
        return segmentMetadata.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> UploadedSegmentMetadata.fromString(entry.getValue())
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
    public static RemoteSegmentMetadata readV2(IndexInput indexInput, int version) throws IOException {
        Map<String, String> serializedMetadata = indexInput.readMapOfStrings();

        // Add null check and validation
        if (serializedMetadata == null) {
            throw new IOException("Serialized metadata cannot be null during RemoteSegmentMetadata deserialization");
        }

        // Convert Map<String, String> to Map<FileMetadata, String> by reconstructing format information
        Map<FileMetadata, String> fileMetadataMap = new HashMap<>();
        for (Map.Entry<String, String> entry : serializedMetadata.entrySet()) {
            String filename = entry.getKey();
            String metadataString = entry.getValue();

            if (filename == null || metadataString == null) {
                throw new IOException("Invalid metadata entry: filename=" + filename + ", metadataString=" + metadataString);
            }

            try {
                // Parse format from UploadedSegmentMetadata string (format is preserved in toString())
                UploadedSegmentMetadata parsedMetadata =
                    UploadedSegmentMetadata.fromString(metadataString);

                // Reconstruct FileMetadata with correct format
                FileMetadata fileMetadata = new FileMetadata(parsedMetadata.getDataFormat(), filename);
                fileMetadataMap.put(fileMetadata, metadataString);
            } catch (Exception e) {
                throw new IOException("Failed to parse UploadedSegmentMetadata for file: " + filename, e);
            }
        }

        // Add null check before calling fromMapOfStrings
        if (fileMetadataMap.isEmpty()) {
            // Handle empty metadata case gracefully
            final Map<FileMetadata, UploadedSegmentMetadata> emptyMap = new HashMap<>();
            ReplicationCheckpoint replicationCheckpoint = readCheckpointFromIndexInput(indexInput, new HashMap<>(), version);
            int byteArraySize = (int) indexInput.readLong();
            byte[] segmentInfosBytes = new byte[byteArraySize];
            indexInput.readBytes(segmentInfosBytes, 0, byteArraySize);
            return new RemoteSegmentMetadata(emptyMap, segmentInfosBytes, replicationCheckpoint);
        }

        // Now pass the properly converted map instead of null
        final Map<FileMetadata, UploadedSegmentMetadata> uploadedSegmentMetadataMap =
            RemoteSegmentMetadata.fromMapOfStringsV2(fileMetadataMap);

        // Create String-based map for readCheckpointFromIndexInput (backward compatibility)
        Map<String, UploadedSegmentMetadata> stringKeyedMap = uploadedSegmentMetadataMap.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey().file(),
                Map.Entry::getValue
            ));

        ReplicationCheckpoint replicationCheckpoint = readCheckpointFromIndexInput(indexInput, stringKeyedMap, version);
        int byteArraySize = (int) indexInput.readLong();
        byte[] segmentInfosBytes = new byte[byteArraySize];
        indexInput.readBytes(segmentInfosBytes, 0, byteArraySize);
        return new RemoteSegmentMetadata(uploadedSegmentMetadataMap, segmentInfosBytes, replicationCheckpoint);
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
        final Map<String, UploadedSegmentMetadata> uploadedSegmentMetadataMap = RemoteSegmentMetadata
            .fromMapOfStrings(metadata);
        ReplicationCheckpoint replicationCheckpoint = readCheckpointFromIndexInput(indexInput, uploadedSegmentMetadataMap, version);
        int byteArraySize = (int) indexInput.readLong();
        byte[] segmentInfosBytes = new byte[byteArraySize];
        indexInput.readBytes(segmentInfosBytes, 0, byteArraySize);
        return new RemoteSegmentMetadata(
            uploadedSegmentMetadataMap.entrySet().stream().collect(
                Collectors.toMap(
                    entry -> new FileMetadata(entry.getKey() + FileMetadata.DELIMITER + "lucene"),
                    Map.Entry::getValue
                )
            ),
            segmentInfosBytes,
            replicationCheckpoint
        );
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
        Map<String, UploadedSegmentMetadata> uploadedSegmentMetadataMap,
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
        Map<String, UploadedSegmentMetadata> metadata
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

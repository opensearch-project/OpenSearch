/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.time.DateUtils;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a Replication Checkpoint which is sent to a replica shard.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public class ReplicationCheckpoint implements Writeable, Comparable<ReplicationCheckpoint> {

    private final ShardId shardId;
    private final long primaryTerm;
    private final long segmentsGen;
    private final long segmentInfosVersion;
    private final long length;
    private final String codec;
    private final Map<FileMetadata, StoreFileMetadata> formatAwareMetadataMap;
    private final long createdTimeStamp;

    // Version compatibility for FileMetadata-aware serialization
    private static final Version FILEMETADATA_AWARE_VERSION = Version.V_3_1_0;

    public static ReplicationCheckpoint empty(ShardId shardId) {
        return empty(shardId, "");
    }

    public static ReplicationCheckpoint empty(ShardId shardId, String codec) {
        return new ReplicationCheckpoint(shardId, codec);
    }

    private ReplicationCheckpoint(ShardId shardId, String codec) {
        this.shardId = shardId;
        primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
        segmentsGen = SequenceNumbers.NO_OPS_PERFORMED;
        segmentInfosVersion = SequenceNumbers.NO_OPS_PERFORMED;
        length = 0L;
        this.codec = codec;
        this.formatAwareMetadataMap = Collections.emptyMap();
        this.createdTimeStamp = DateUtils.toLong(Instant.now());
    }

    public ReplicationCheckpoint(ShardId shardId, long primaryTerm, long segmentsGen, long segmentInfosVersion, String codec) {
        this(shardId, primaryTerm, segmentsGen, segmentInfosVersion, 0L, codec, Collections.emptyMap(), DateUtils.toLong(Instant.now()));
    }

    // Format-aware constructor (with codec first to avoid erasure clash)
    public ReplicationCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long segmentsGen,
        long segmentInfosVersion,
        long length,
        Map<FileMetadata, StoreFileMetadata> formatAwareMetadataMap,
        String codec
    ) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.segmentsGen = segmentsGen;
        this.segmentInfosVersion = segmentInfosVersion;
        this.length = length;
        this.codec = codec;
        this.formatAwareMetadataMap = formatAwareMetadataMap;
        this.createdTimeStamp = DateUtils.toLong(Instant.now());
    }

    // Legacy constructor (with codec after length to distinguish from format-aware)
    public ReplicationCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long segmentsGen,
        long segmentInfosVersion,
        long length,
        String codec,
        Map<String, StoreFileMetadata> legacyMetadataMap
    ) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.segmentsGen = segmentsGen;
        this.segmentInfosVersion = segmentInfosVersion;
        this.length = length;
        this.codec = codec;
        this.createdTimeStamp = DateUtils.toLong(Instant.now());

        // Convert legacy metadata to format-aware metadata
        Map<FileMetadata, StoreFileMetadata> convertedMap = new HashMap<>();
        for (Map.Entry<String, StoreFileMetadata> entry : legacyMetadataMap.entrySet()) {
            String dataFormat = entry.getValue().dataFormat() != null ? entry.getValue().dataFormat() : "lucene";
            FileMetadata fileMetadata = new FileMetadata(dataFormat, entry.getKey());
            convertedMap.put(fileMetadata, entry.getValue());
        }
        this.formatAwareMetadataMap = convertedMap;
    }

    // Legacy constructor with timestamp (with codec after length to distinguish)
    public ReplicationCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long segmentsGen,
        long segmentInfosVersion,
        long length,
        String codec,
        Map<String, StoreFileMetadata> legacyMetadataMap,
        long createdTimeStamp
    ) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.segmentsGen = segmentsGen;
        this.segmentInfosVersion = segmentInfosVersion;
        this.length = length;
        this.codec = codec;
        this.createdTimeStamp = createdTimeStamp;

        // Convert legacy metadata to format-aware metadata
        Map<FileMetadata, StoreFileMetadata> convertedMap = new HashMap<>();
        for (Map.Entry<String, StoreFileMetadata> entry : legacyMetadataMap.entrySet()) {
            String dataFormat = entry.getValue().dataFormat() != null ? entry.getValue().dataFormat() : "lucene";
            FileMetadata fileMetadata = new FileMetadata(dataFormat, entry.getKey());
            convertedMap.put(fileMetadata, entry.getValue());
        }
        this.formatAwareMetadataMap = convertedMap;
    }

    // Format-aware constructor with timestamp (with metadata before codec to distinguish)
    public ReplicationCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long segmentsGen,
        long segmentInfosVersion,
        long length,
        Map<FileMetadata, StoreFileMetadata> formatAwareMetadataMap,
        String codec,
        long createdTimeStamp
    ) {
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.segmentsGen = segmentsGen;
        this.segmentInfosVersion = segmentInfosVersion;
        this.length = length;
        this.codec = codec;
        this.formatAwareMetadataMap = formatAwareMetadataMap;
        this.createdTimeStamp = createdTimeStamp;
    }

    /**
     * Static factory method for legacy String-based metadata maps.
     * Creates a ReplicationCheckpoint with converted FileMetadata-based map.
     *
     * @deprecated Use constructor with Map&lt;FileMetadata, StoreFileMetadata&gt; instead
     */
    @Deprecated
    public static ReplicationCheckpoint fromLegacyMetadata(
        ShardId shardId,
        long primaryTerm,
        long segmentsGen,
        long segmentInfosVersion,
        long length,
        String codec,
        Map<String, StoreFileMetadata> legacyMetadataMap
    ) {
        return new ReplicationCheckpoint(shardId, primaryTerm, segmentsGen, segmentInfosVersion, length, codec, legacyMetadataMap);
    }

    /**
     * Static factory method for legacy String-based metadata maps with timestamp.
     * Creates a ReplicationCheckpoint with converted FileMetadata-based map.
     *
     * @deprecated Use constructor with Map&lt;FileMetadata, StoreFileMetadata&gt; instead
     */
    @Deprecated
    public static ReplicationCheckpoint fromLegacyMetadata(
        ShardId shardId,
        long primaryTerm,
        long segmentsGen,
        long segmentInfosVersion,
        long length,
        String codec,
        Map<String, StoreFileMetadata> legacyMetadataMap,
        long createdTimeStamp
    ) {
        return new ReplicationCheckpoint(shardId, primaryTerm, segmentsGen, segmentInfosVersion, length, codec, legacyMetadataMap, createdTimeStamp);
    }

    public ReplicationCheckpoint(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        primaryTerm = in.readLong();
        segmentsGen = in.readLong();
        segmentInfosVersion = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            length = in.readLong();
            codec = in.readString();
        } else {
            length = 0L;
            codec = null;
        }
        if (in.getVersion().onOrAfter(FILEMETADATA_AWARE_VERSION)) {
            // Read FileMetadata-aware format
            this.formatAwareMetadataMap = in.readMap(
                streamInput -> new FileMetadata(streamInput.readString(), streamInput.readString()),
                StoreFileMetadata::new
            );
        } else if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
            // Convert legacy String-based format to FileMetadata format
            Map<String, StoreFileMetadata> legacyMap = in.readMap(StreamInput::readString, StoreFileMetadata::new);
            Map<FileMetadata, StoreFileMetadata> convertedMap = new HashMap<>();
            for (Map.Entry<String, StoreFileMetadata> entry : legacyMap.entrySet()) {
                String dataFormat = entry.getValue().dataFormat() != null ? entry.getValue().dataFormat() : "lucene";
                FileMetadata fileMetadata = new FileMetadata(dataFormat, entry.getKey());
                convertedMap.put(fileMetadata, entry.getValue());
            }
            this.formatAwareMetadataMap = convertedMap;
        } else {
            this.formatAwareMetadataMap = Collections.emptyMap();
        }
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.createdTimeStamp = in.readLong();
        } else {
            this.createdTimeStamp = 0;
        }
    }

    /**
     * The primary term of this Replication Checkpoint.
     *
     * @return the primary term
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * @return the Segments Gen number
     */
    public long getSegmentsGen() {
        return segmentsGen;
    }

    /**
     * @return the Segment Info version
     */
    public long getSegmentInfosVersion() {
        return segmentInfosVersion;
    }

    /**
     * Shard Id of primary shard.
     *
     * @return the Shard Id
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * @return The size in bytes of this checkpoint.
     */
    public long getLength() {
        return length;
    }

    /**
     * Latest supported codec version
     *
         * @return the codec name
     */
    public String getCodec() {
        return codec;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeLong(primaryTerm);
        out.writeLong(segmentsGen);
        out.writeLong(segmentInfosVersion);
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeLong(length);
            out.writeString(codec);
        }
        if (out.getVersion().onOrAfter(FILEMETADATA_AWARE_VERSION)) {
            // Write FileMetadata-aware format
            out.writeMap(formatAwareMetadataMap,
                (keyOut, fileMetadata) -> {
                    keyOut.writeString(fileMetadata.dataFormat());
                    keyOut.writeString(fileMetadata.file());
                },
                (valueOut, storeFileMetadata) -> storeFileMetadata.writeTo(valueOut)
            );
        } else if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            // Convert to legacy String-based format for backward compatibility
            Map<String, StoreFileMetadata> legacyMap = new HashMap<>();
            for (Map.Entry<FileMetadata, StoreFileMetadata> entry : formatAwareMetadataMap.entrySet()) {
                legacyMap.put(entry.getKey().file(), entry.getValue());
            }
            out.writeMap(legacyMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        }
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeLong(createdTimeStamp);
        }
    }

    @Override
    public int compareTo(ReplicationCheckpoint other) {
        return this.isAheadOf(other) ? -1 : 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationCheckpoint that = (ReplicationCheckpoint) o;
        return primaryTerm == that.primaryTerm
            && segmentsGen == that.segmentsGen
            && segmentInfosVersion == that.segmentInfosVersion
            && Objects.equals(shardId, that.shardId)
            && codec.equals(that.codec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, primaryTerm, segmentsGen);
    }

    /**
     * Checks if current replication checkpoint is AheadOf `other` replication checkpoint point by first comparing
     * primaryTerm followed by segmentInfosVersion. Returns true when `other` is null.
     */
    public boolean isAheadOf(@Nullable ReplicationCheckpoint other) {
        return other == null
            || primaryTerm > other.getPrimaryTerm()
            || (primaryTerm == other.getPrimaryTerm() && segmentInfosVersion > other.getSegmentInfosVersion());
    }

    public Map<FileMetadata, StoreFileMetadata> getFormatAwareMetadataMap() {
        return formatAwareMetadataMap;
    }

    /**
     * Returns legacy String-based metadata map for backward compatibility.
     * Format information is lost in this conversion.
     *
     * @return legacy metadata map with filename keys
     * @deprecated Use getFormatAwareMetadataMap() instead to preserve format information
     */
    @Deprecated
    public Map<String, StoreFileMetadata> getMetadataMap() {
        Map<String, StoreFileMetadata> legacyMap = new HashMap<>();
        for (Map.Entry<FileMetadata, StoreFileMetadata> entry : formatAwareMetadataMap.entrySet()) {
            legacyMap.put(entry.getKey().file(), entry.getValue());
        }
        return legacyMap;
    }

    public long getCreatedTimeStamp() {
        return createdTimeStamp;
    }

    @Override
    public String toString() {
        return "ReplicationCheckpoint{"
            + "shardId="
            + shardId
            + ", primaryTerm="
            + primaryTerm
            + ", segmentsGen="
            + segmentsGen
            + ", version="
            + segmentInfosVersion
            + ", size="
            + length
            + ", codec="
            + codec
            + ", timestamp="
            + createdTimeStamp
            + '}';
    }
}

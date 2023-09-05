/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

/**
 * Metadata object for Remote Segment
 *
 * @opensearch.internal
 */
public class RemoteSegmentMetadata {
    /**
     * Latest supported version of metadata
     */
    public static final int CURRENT_VERSION = 1;
    /**
     * Metadata codec
     */
    public static final String METADATA_CODEC = "segment_md";

    /**
     * Data structure holding metadata content
     */
    private final Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata;

    private final byte[] segmentInfosBytes;

    private final long primaryTerm;
    private final long generation;

    public RemoteSegmentMetadata(
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata,
        byte[] segmentInfosBytes,
        long primaryTerm,
        long generation
    ) {
        this.metadata = metadata;
        this.segmentInfosBytes = segmentInfosBytes;
        this.generation = generation;
        this.primaryTerm = primaryTerm;
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
        return generation;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
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

    public void write(IndexOutput out) throws IOException {
        out.writeMapOfStrings(toMapOfStrings());
        out.writeLong(generation);
        out.writeLong(primaryTerm);
        out.writeLong(segmentInfosBytes.length);
        out.writeBytes(segmentInfosBytes, segmentInfosBytes.length);
    }

    public static RemoteSegmentMetadata read(IndexInput indexInput) throws IOException {
        Map<String, String> metadata = indexInput.readMapOfStrings();
        long generation = indexInput.readLong();
        long primaryTerm = indexInput.readLong();
        int byteArraySize = (int) indexInput.readLong();
        byte[] segmentInfosBytes = new byte[byteArraySize];
        indexInput.readBytes(segmentInfosBytes, 0, byteArraySize);
        return new RemoteSegmentMetadata(RemoteSegmentMetadata.fromMapOfStrings(metadata), segmentInfosBytes, primaryTerm, generation);
    }
}

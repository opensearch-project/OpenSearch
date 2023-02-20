/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import java.util.Map;
import java.util.stream.Collectors;

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

    public RemoteSegmentMetadata(Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata) {
        this.metadata = metadata;
    }

    /**
     * Exposes underlying metadata content data structure.
     * @return {@code metadata}
     */
    public Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> getMetadata() {
        return this.metadata;
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
    public static RemoteSegmentMetadata fromMapOfStrings(Map<String, String> segmentMetadata) {
        return new RemoteSegmentMetadata(
            segmentMetadata.entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(entry.getValue())
                    )
                )
        );
    }
}

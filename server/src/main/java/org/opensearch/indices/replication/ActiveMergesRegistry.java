/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry to track active segment merges in OpenSearch.
 * This registry maintains a mapping of segment filenames to their metadata and processing state.
 * It allows segments to be registered, tracked, and have their state updated as they move through
 * the pre-copy lifecycle.
 *
 * <p>The registry is thread-safe and uses a concurrent hash map to store segment entries.
 * Each entry contains the segment filename, its current state (REGISTERED or PROCESSED),
 * and {@link UploadedSegmentMetadata} for the segment.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class ActiveMergesRegistry {
    private final Map<String, SegmentEntry> registryMap = new ConcurrentHashMap<>();

    public void register(String localSegmentName) {
        registryMap.put(localSegmentName, new SegmentEntry(localSegmentName, SegmentState.REGISTERED));
    }

    public void register(String localSegmentName, UploadedSegmentMetadata metadata) {
        SegmentEntry entry = registryMap.computeIfAbsent(localSegmentName, (k) -> new SegmentEntry(k, SegmentState.REGISTERED));
        entry.setMetadata(metadata);
    }

    public void unregister(String localSegmentName) {
        registryMap.remove(localSegmentName);
    }

    public SegmentEntry get(String localSegmentName) {
        return registryMap.get(localSegmentName);
    }

    public boolean contains(String localSegmentName) {
        return registryMap.containsKey(localSegmentName);
    }

    public boolean isEmpty() {
        return registryMap.isEmpty();
    }

    public Set<String> getRegisteredSegments() {
        return Collections.unmodifiableSet(registryMap.keySet());
    }

    public void setStateProcessed(String filename) {
        SegmentEntry entry = registryMap.get(filename);
        if (entry != null) {
            entry.setState(SegmentState.PROCESSED);
        }
    }

    public boolean containsRemoteFile(String filename) {
        String localFilename = getLocalFilename(filename);
        return contains(localFilename);
    }

    private String getLocalFilename(String filename) {
        String[] parts = filename.split(RemoteSegmentStoreDirectory.SEGMENT_NAME_UUID_SEPARATOR);
        return parts.length > 0 ? parts[0] : filename;
    }

    /**
     * Represents the processing state of a segment in the registry.
     * <p> Possible states:
     * <ul>
     *   <li>REGISTERED - The segment has been registered but not yet processed</li>
     *   <li>PROCESSED - The segment has been uploaded/downloaded and can be unregistered.</li>
     * </ul>
     */
    @ExperimentalApi
    public enum SegmentState {
        REGISTERED,
        PROCESSED
    }

    /**
     * Entry in the ActiveMergesRegistry that holds information about a segment.
     * <ul>
     *   <li>filename - The name of the segment file</li>
     *   <li>state - The current processing state of the segment</li>
     *   <li>metadata - Optional metadata about the uploaded segment</li>
     * </ul>
     */
    @ExperimentalApi
    public static class SegmentEntry {
        private final String filename;
        private SegmentState state;
        private UploadedSegmentMetadata metadata;

        public SegmentEntry(String filename, SegmentState state) {
            this(filename, state, null);
        }

        public SegmentEntry(String filename, SegmentState state, UploadedSegmentMetadata metadata) {
            this.filename = filename;
            this.state = state;
            this.metadata = metadata;
        }

        public void setMetadata(UploadedSegmentMetadata metadata) {
            this.metadata = metadata;
        }

        public UploadedSegmentMetadata getMetadata() {
            return metadata;
        }

        public SegmentState getState() {
            return state;
        }

        public void setState(SegmentState state) {
            this.state = state;
        }

        public String toString() {
            return "SegmentEntry{" + "filename=" + filename + ", metadata=" + metadata + ", state=" + state + "}";
        }
    }
}

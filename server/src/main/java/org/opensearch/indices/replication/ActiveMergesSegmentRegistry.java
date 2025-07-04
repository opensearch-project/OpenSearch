/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import reactor.util.annotation.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Registry to track active merge segments and their metadata.
 * This class is implemented as a singleton and provides methods to register, update metadata,
 * and unregister segment files that are being merged. It also maintains a mapping between
 * local segment filenames and their corresponding uploaded segment metadata.
 * */
public class ActiveMergesSegmentRegistry {
    private final Map<String, UploadedSegmentMetadata> segmentMetadataMap = new ConcurrentHashMap<>();
    final Set<String> filenameRegistry = ConcurrentHashMap.newKeySet(); // package-private for tests
    private final ReentrantLock lock = new ReentrantLock();

    private static class HOLDER {
        private static final ActiveMergesSegmentRegistry INSTANCE = new ActiveMergesSegmentRegistry();
    }

    private ActiveMergesSegmentRegistry() {};

    public static ActiveMergesSegmentRegistry getInstance() {
        return HOLDER.INSTANCE;
    }

    /**
     * Registers a segment file. Throws exception if already registered.
     * @param localSegmentFilename Segment filename in the local store
     */
    public void register(@NonNull String localSegmentFilename) {
        lock.lock();
        try {
            if (contains(localSegmentFilename)){
                throw new IllegalArgumentException(localSegmentFilename + " is already registered. Cannot reregister.");
            }
            filenameRegistry.add(localSegmentFilename);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds {@link UploadedSegmentMetadata} for a registered segment file. Throws an error if the file is not registered.
     * @param localSegmentFilename Segment filename in the local store
     * @param metadata {@link UploadedSegmentMetadata} for the segment file
     */
    public void updateMetadata(@NonNull String localSegmentFilename, @NonNull UploadedSegmentMetadata metadata) {
        lock.lock();
        try {
            if (contains(localSegmentFilename) == false) {
                throw new IllegalArgumentException("Segment " + localSegmentFilename + " is not registered");
            }
            segmentMetadataMap.put(localSegmentFilename, metadata);
            filenameRegistry.add(metadata.getUploadedFilename());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unregisters a segment file from the registry.
     * @param segmentFilename Segment filename in local store
     */
    public void unregister(@NonNull String segmentFilename) {
        lock.lock();
        try {
            if (segmentMetadataMap.containsKey(segmentFilename)) {
                String remoteFilename = segmentMetadataMap.get(segmentFilename).getUploadedFilename();
                filenameRegistry.remove(remoteFilename);
            }
            filenameRegistry.remove(segmentFilename);
            segmentMetadataMap.remove(segmentFilename);
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(@NonNull String segmentFilename) {
        return filenameRegistry.contains(segmentFilename);
    }

    public String getExistingRemoteSegmentFilename(@NonNull String localSegmentFilename) {
        if (segmentMetadataMap.containsKey(localSegmentFilename) == false) {
            throw new IllegalArgumentException("Metadata for segment " + localSegmentFilename + " is not available.");
        }
        return segmentMetadataMap.get(localSegmentFilename).getUploadedFilename();
    }

    public boolean canDelete(@NonNull String segmentFilename) {
        return contains(segmentFilename) == false;
    }

    public Map<String, UploadedSegmentMetadata> segmentMetadataMap() {
        return Collections.unmodifiableMap(segmentMetadataMap);
    }

    public UploadedSegmentMetadata getMetadata(String localSegmentFilename){
        return segmentMetadataMap.get(localSegmentFilename);
    }
}

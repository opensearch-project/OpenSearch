/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    @ExperimentalApi
    public enum SegmentState {
        REGISTERED,
        PROCESSED
    }

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

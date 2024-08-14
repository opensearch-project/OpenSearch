/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.remote.RemoteWriteableBlobEntity;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.compress.Compressor;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Wrapper class for uploading/downloading {@link RemotePinnedTimestamps} to/from remote blob store
 *
 * @opensearch.internal
 */
public class RemotePinnedTimestamps extends RemoteWriteableBlobEntity<RemotePinnedTimestamps.PinnedTimestamps> {
    private static final Logger logger = LogManager.getLogger(RemotePinnedTimestamps.class);

    public static class PinnedTimestamps implements Writeable {
        private final Map<Long, List<String>> pinnedTimestampPinningEntityMap;

        public PinnedTimestamps(Map<Long, List<String>> pinnedTimestampPinningEntityMap) {
            this.pinnedTimestampPinningEntityMap = new ConcurrentHashMap<>(pinnedTimestampPinningEntityMap);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(pinnedTimestampPinningEntityMap, StreamOutput::writeLong, StreamOutput::writeStringCollection);
        }

        public static PinnedTimestamps readFrom(StreamInput in) throws IOException {
            return new PinnedTimestamps(in.readMap(StreamInput::readLong, StreamInput::readStringList));
        }

        public void pin(Long timestamp, String pinningEntity) {
            logger.debug("Pinning timestamp = {} against entity = {}", timestamp, pinningEntity);
            pinnedTimestampPinningEntityMap.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(pinningEntity);
        }

        public void unpin(Long timestamp, String pinningEntity) {
            logger.debug("Unpinning timestamp = {} against entity = {}", timestamp, pinningEntity);
            pinnedTimestampPinningEntityMap.computeIfPresent(timestamp, (k, v) -> {
                v.remove(pinningEntity);
                return v.isEmpty() ? null : v;
            });
        }

        public Map<Long, List<String>> getPinnedTimestampPinningEntityMap() {
            return new HashMap<>(pinnedTimestampPinningEntityMap);
        }
    }

    public static final String PINNED_TIMESTAMPS = "pinned_timestamps";
    public static final ChecksumWritableBlobStoreFormat<PinnedTimestamps> PINNED_TIMESTAMPS_FORMAT = new ChecksumWritableBlobStoreFormat<>(
        PINNED_TIMESTAMPS,
        PinnedTimestamps::readFrom
    );

    private PinnedTimestamps pinnedTimestamps;

    public RemotePinnedTimestamps(String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        pinnedTimestamps = new PinnedTimestamps(new HashMap<>());
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(PINNED_TIMESTAMPS), PINNED_TIMESTAMPS);
    }

    @Override
    public String getType() {
        return PINNED_TIMESTAMPS;
    }

    @Override
    public String generateBlobFileName() {
        return this.blobFileName = String.join(DELIMITER, PINNED_TIMESTAMPS, RemoteStoreUtils.invertLong(System.currentTimeMillis()));
    }

    @Override
    public InputStream serialize() throws IOException {
        return PINNED_TIMESTAMPS_FORMAT.serialize(pinnedTimestamps, generateBlobFileName(), getCompressor()).streamInput();
    }

    @Override
    public PinnedTimestamps deserialize(InputStream inputStream) throws IOException {
        return PINNED_TIMESTAMPS_FORMAT.deserialize(blobName, Streams.readFully(inputStream));
    }

    public void setBlobFileName(String blobFileName) {
        this.blobFileName = blobFileName;
    }

    public void setPinnedTimestamps(PinnedTimestamps pinnedTimestamps) {
        this.pinnedTimestamps = pinnedTimestamps;
    }

    public PinnedTimestamps getPinnedTimestamps() {
        return pinnedTimestamps;
    }
}

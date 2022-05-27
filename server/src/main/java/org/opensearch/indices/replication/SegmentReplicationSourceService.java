/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Service class that handles segment replication requests from replica shards.
 * Typically, the "source" is a primary shard.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceService {

    /**
     * Internal actions used by the segment replication source service on the primary shard
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/replication/get_checkpoint_info";
        public static final String GET_SEGMENT_FILES = "internal:index/shard/replication/get_segment_files";
    }

    private final Map<ReplicationCheckpoint, CopyState> copyStateMap;
    private final TransportService transportService;

    public SegmentReplicationSourceService(TransportService transportService) {
        copyStateMap = Collections.synchronizedMap(new HashMap<>());
        this.transportService = transportService;
        // TODO register request handlers
    }

    /**
     * A synchronized method that checks {@link #copyStateMap} for the given {@link ReplicationCheckpoint} key
     * and returns the cached value if one is present. If the key is not present, a {@link CopyState}
     * object is constructed and stored in the map before being returned.
     */
    private synchronized CopyState getCachedCopyState(ReplicationCheckpoint replicationCheckpoint) {
        if (isInCopyStateMap(replicationCheckpoint)) {
            final CopyState copyState = fetchFromCopyStateMap(replicationCheckpoint);
            copyState.incRef();
            return copyState;
        } else {
            // TODO fetch the shard object to build the CopyState
            return null;
        }
    }

    /**
     * Operations on the {@link #copyStateMap} member.
     */

    /**
     * Adds the input {@link CopyState} object to {@link #copyStateMap}.
     * The key is the CopyState's {@link ReplicationCheckpoint} object.
     */
    private void addToCopyStateMap(CopyState copyState) {
        copyStateMap.putIfAbsent(copyState.getReplicationCheckpoint(), copyState);
    }

    /**
     * Given a {@link ReplicationCheckpoint}, return the corresponding
     * {@link CopyState} object, if any, from {@link #copyStateMap}.
     */
    private CopyState fetchFromCopyStateMap(ReplicationCheckpoint replicationCheckpoint) {
        return copyStateMap.get(replicationCheckpoint);
    }

    /**
     * Checks if the {@link #copyStateMap} has the input {@link ReplicationCheckpoint}
     * as a key by invoking {@link Map#containsKey(Object)}.
     */
    private boolean isInCopyStateMap(ReplicationCheckpoint replicationCheckpoint) {
        return copyStateMap.containsKey(replicationCheckpoint);
    }
}

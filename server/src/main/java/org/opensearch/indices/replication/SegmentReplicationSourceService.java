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
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Service class that handles segment replication requests from replica shards.
 * Typically, the "source" is a primary shard. This code executes on the source node.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceService {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationSourceService.class);

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
    private final IndicesService indicesService;

    // TODO mark this as injected and bind in Node
    public SegmentReplicationSourceService(TransportService transportService, IndicesService indicesService) {
        copyStateMap = Collections.synchronizedMap(new HashMap<>());
        this.transportService = transportService;
        this.indicesService = indicesService;

        transportService.registerRequestHandler(
            Actions.GET_CHECKPOINT_INFO,
            ThreadPool.Names.GENERIC,
            CheckpointInfoRequest::new,
            new CheckpointInfoRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.GET_SEGMENT_FILES,
            ThreadPool.Names.GENERIC,
            GetSegmentFilesRequest::new,
            new GetSegmentFilesRequestHandler()
        );
    }

    private class CheckpointInfoRequestHandler implements TransportRequestHandler<CheckpointInfoRequest> {
        @Override
        public void messageReceived(CheckpointInfoRequest request, TransportChannel channel, Task task) throws Exception {
            final ReplicationCheckpoint checkpoint = request.getCheckpoint();
            logger.trace("Received request for checkpoint {}", checkpoint);
            final CopyState copyState = getCachedCopyState(checkpoint);
            channel.sendResponse(
                new CheckpointInfoResponse(
                    copyState.getCheckpoint(),
                    copyState.getMetadataSnapshot(),
                    copyState.getInfosBytes(),
                    copyState.getPendingDeleteFiles()
                )
            );
        }
    }

    class GetSegmentFilesRequestHandler implements TransportRequestHandler<GetSegmentFilesRequest> {
        @Override
        public void messageReceived(GetSegmentFilesRequest request, TransportChannel channel, Task task) throws Exception {
            if (isInCopyStateMap(request.getCheckpoint())) {
                // TODO send files
            } else {
                // Return an empty list of files
                channel.sendResponse(new GetSegmentFilesResponse(Collections.emptyList()));
            }
        }
    }

    /**
     * Operations on the {@link #copyStateMap} member.
     */

    /**
     * A synchronized method that checks {@link #copyStateMap} for the given {@link ReplicationCheckpoint} key
     * and returns the cached value if one is present. If the key is not present, a {@link CopyState}
     * object is constructed and stored in the map before being returned.
     */
    private synchronized CopyState getCachedCopyState(ReplicationCheckpoint checkpoint) throws IOException {
        if (isInCopyStateMap(checkpoint)) {
            final CopyState copyState = fetchFromCopyStateMap(checkpoint);
            copyState.incRef();
            return copyState;
        } else {
            // From the checkpoint's shard ID, fetch the IndexShard
            ShardId shardId = checkpoint.getShardId();
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            // build the CopyState object and cache it before returning
            final CopyState copyState = new CopyState(indexShard);

            /**
             * Use the checkpoint from the request as the key in the map, rather than
             * the checkpoint from the created CopyState. This maximizes cache hits
             * if replication targets make a request with an older checkpoint.
             * Replication targets are expected to fetch the checkpoint in the response
             * CopyState to bring themselves up to date.
             */
            addToCopyStateMap(checkpoint, copyState);
            return copyState;
        }
    }

    /**
     * Adds the input {@link CopyState} object to {@link #copyStateMap}.
     * The key is the CopyState's {@link ReplicationCheckpoint} object.
     */
    private void addToCopyStateMap(ReplicationCheckpoint checkpoint, CopyState copyState) {
        copyStateMap.putIfAbsent(checkpoint, copyState);
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

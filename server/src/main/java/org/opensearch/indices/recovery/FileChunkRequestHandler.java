/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.lucene.store.RateLimiter;
import org.opensearch.action.ActionListener;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTarget;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A request handler for processing file-chunk Requests.
 * @param <T> a {@link ReplicationTarget} implementation that can write incoming file chunks.
 *
 * @opensearch.internal
 */
public class FileChunkRequestHandler<T extends ReplicationTarget> implements TransportRequestHandler<FileChunkRequest> {

    // How many bytes we've copied since we last called RateLimiter.pause
    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final RecoverySettings recoverySettings;
    private final ReplicationCollection<T> onGoingTransfers;

    public FileChunkRequestHandler(ReplicationCollection<T> onGoingTransfers, RecoverySettings recoverySettings) {
        this.onGoingTransfers = onGoingTransfers;
        this.recoverySettings = recoverySettings;
    }

    @Override
    public void messageReceived(final FileChunkRequest request, TransportChannel channel, Task task) throws Exception {
        try (ReplicationCollection.ReplicationRef<T> recoveryRef = onGoingTransfers.getSafe(request.recoveryId(), request.shardId())) {
            final ReplicationTarget replicationTarget = recoveryRef.get();
            final ActionListener<Void> listener = replicationTarget.createOrFinishListener(
                channel,
                PeerRecoveryTargetService.Actions.FILE_CHUNK,
                request
            );
            if (listener == null) {
                return;
            }

            final ReplicationLuceneIndex indexState = replicationTarget.state().getIndex();
            if (request.sourceThrottleTimeInNanos() != ReplicationLuceneIndex.UNKNOWN) {
                indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
            }

            RateLimiter rateLimiter = recoverySettings.rateLimiter();
            if (rateLimiter != null) {
                long bytes = bytesSinceLastPause.addAndGet(request.content().length());
                if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                    // Time to pause
                    bytesSinceLastPause.addAndGet(-bytes);
                    long throttleTimeInNanos = rateLimiter.pause(bytes);
                    indexState.addTargetThrottling(throttleTimeInNanos);
                    replicationTarget.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                }
            }

            replicationTarget.writeFileChunk(
                request.metadata(),
                request.position(),
                request.content(),
                request.lastChunk(),
                request.totalTranslogOps(),
                listener
            );
        }
    }
}

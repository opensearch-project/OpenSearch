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
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A handler for processing file-chunk Requests.
 *
 * @opensearch.internal
 */
public final class RateLimitedFileChunkHandler {

    /**
     * Handle a FileChunkRequest for a {@link ReplicationTarget}.
     *
     * @param request {@link FileChunkRequest} Request containing the file chunk.
     * @param bytesSinceLastPause {@link AtomicLong} Bytes since the last pause.
     * @param rateLimiter {@link RateLimiter} Rate limiter.
     * @throws IOException When there is an issue pausing the rate limiter.
     */
    public static void handleFileChunk(
        final FileChunkRequest request,
        final ReplicationTarget replicationTarget,
        final AtomicLong bytesSinceLastPause,
        final RateLimiter rateLimiter,
        final ActionListener<Void> listener
    ) throws IOException {

        if (listener == null) {
            return;
        }

        final ReplicationLuceneIndex indexState = replicationTarget.state().getIndex();
        if (request.sourceThrottleTimeInNanos() != ReplicationLuceneIndex.UNKNOWN) {
            indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
        }

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

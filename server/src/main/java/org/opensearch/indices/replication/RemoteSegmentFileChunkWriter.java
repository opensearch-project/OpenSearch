/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.store.RateLimiter;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This class handles sending file chunks over the transport layer to a target shard.
 *
 * @opensearch.internal
 */
public final class RemoteSegmentFileChunkWriter implements FileChunkWriter {

    private final AtomicLong requestSeqNoGenerator;
    private final RetryableTransportClient retryableTransportClient;
    private final ShardId shardId;
    private final RecoverySettings recoverySettings;
    private final long replicationId;
    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final TransportRequestOptions fileChunkRequestOptions;
    private final Consumer<Long> onSourceThrottle;
    private final String action;

    public RemoteSegmentFileChunkWriter(
        long replicationId,
        RecoverySettings recoverySettings,
        RetryableTransportClient retryableTransportClient,
        ShardId shardId,
        String action,
        AtomicLong requestSeqNoGenerator,
        Consumer<Long> onSourceThrottle
    ) {
        this.replicationId = replicationId;
        this.recoverySettings = recoverySettings;
        this.retryableTransportClient = retryableTransportClient;
        this.shardId = shardId;
        this.requestSeqNoGenerator = requestSeqNoGenerator;
        this.onSourceThrottle = onSourceThrottle;
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.RECOVERY)
            .withTimeout(recoverySettings.internalActionTimeout())
            .build();

        this.action = action;
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        // Pause using the rate limiter, if desired, to throttle the recovery
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the recovery settings
        final RateLimiter rl = recoverySettings.rateLimiter();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new OpenSearchException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
         * see how many translog ops we accumulate while copying files across the network. A future optimization
         * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
         */
        final FileChunkRequest request = new FileChunkRequest(
            replicationId,
            requestSeqNo,
            shardId,
            fileMetadata,
            position,
            content,
            lastChunk,
            totalTranslogOps,
            throttleTimeInNanos
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        retryableTransportClient.executeRetryableAction(
            action,
            request,
            fileChunkRequestOptions,
            ActionListener.map(listener, r -> null),
            reader
        );
    }

    @Override
    public void cancel() {
        retryableTransportClient.cancel();
    }
}

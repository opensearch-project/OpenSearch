/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.index.shard.IndexShard;
import org.opensearch.otel.OtelService;

/**
 * Factory that supplies {@link RecoverySourceHandler}.
 *
 * @opensearch.internal
 */
public class RecoverySourceHandlerFactory {

    public static RecoverySourceHandler create(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        StartRecoveryRequest request,
        RecoverySettings recoverySettings,
        OtelService otelService
    ) {
        boolean isReplicaRecoveryWithRemoteTranslog = request.isPrimaryRelocation() == false && shard.isRemoteTranslogEnabled();
        if (isReplicaRecoveryWithRemoteTranslog) {
            return new RemoteStorePeerRecoverySourceHandler(
                shard,
                recoveryTarget,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations(),
                otelService
            );
        } else {
            return new LocalStorePeerRecoverySourceHandler(
                shard,
                recoveryTarget,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations(),
                otelService
            );
        }
    }
}

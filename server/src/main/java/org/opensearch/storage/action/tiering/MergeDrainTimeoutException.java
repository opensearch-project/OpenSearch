/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception thrown when a shard's merge drain times out during tiering preparation.
 * <p>
 * This is registered with the {@code OpenSearchException} serialization registry (see
 * {@code OpenSearchServerException}, id 179, {@code versionAdded = 3.8.0}). Between nodes running
 * 3.8.0+ it round-trips as a typed {@code MergeDrainTimeoutException}, so the coordinator
 * ({@code TransportHotToWarmTierAction}) detects it with {@code instanceof} and can read the
 * {@link #getActiveMerges()} / {@link #getPendingMerges()} counts directly off the wire.
 * <p>
 * For mixed-version clusters, a peer older than {@code versionAdded} cannot deserialize the typed
 * class and receives a {@code NotSerializableExceptionWrapper} instead (the typed id is only written
 * once the negotiated stream version is on-or-after {@code versionAdded}, so the extra count fields
 * never corrupt an older reader). The exception message therefore always embeds the stable
 * {@link #MERGE_DRAIN_TIMEOUT_MARKER}, which the coordinator matches as a fallback when the typed
 * class did not survive transport.
 *
 * @opensearch.internal
 */
public class MergeDrainTimeoutException extends OpenSearchException {

    /**
     * Stable marker substring embedded in every merge-drain-timeout message. It is the fallback
     * detection signal for mixed-version clusters where the typed class is not deserializable on the
     * receiving node (the message is always wire-preserved, even when wrapped). Changing this string
     * is a wire-compatibility concern — keep it stable across versions.
     */
    public static final String MERGE_DRAIN_TIMEOUT_MARKER = "timed out waiting for merges to drain";

    private final int activeMerges;
    private final int pendingMerges;

    public MergeDrainTimeoutException(ShardId shardId, int activeMerges, int pendingMerges, String timeoutValue) {
        super(buildMessage(shardId, activeMerges, pendingMerges, timeoutValue));
        setShard(shardId);
        this.activeMerges = activeMerges;
        this.pendingMerges = pendingMerges;
    }

    public MergeDrainTimeoutException(StreamInput in) throws IOException {
        super(in);
        this.activeMerges = in.readVInt();
        this.pendingMerges = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(activeMerges);
        out.writeVInt(pendingMerges);
    }

    /** Number of merges still active (in-flight) on the shard when the drain timed out. */
    public int getActiveMerges() {
        return activeMerges;
    }

    /** Number of merges still pending (queued) on the shard when the drain timed out. */
    public int getPendingMerges() {
        return pendingMerges;
    }

    private static String buildMessage(ShardId shardId, int activeMerges, int pendingMerges, String timeoutValue) {
        return "Shard ["
            + shardId
            + "] "
            + MERGE_DRAIN_TIMEOUT_MARKER
            + ". Active merges: "
            + activeMerges
            + ", pending merges: "
            + pendingMerges
            + ". "
            + "Consider increasing cluster.tiering.prepare_timeout (current: "
            + timeoutValue
            + ") "
            + "or wait for merges to complete before retrying.";
    }
}

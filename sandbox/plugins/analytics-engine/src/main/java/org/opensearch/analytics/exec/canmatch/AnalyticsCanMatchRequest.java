/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Per-shard can-match request. Carries the serialized filter list
 * produced by {@link CanMatchFilterSerializer}. The data node
 * deserializes and evaluates against parquet row-group statistics.
 *
 * <p>An optional {@code sortColumn} asks the data node to also fold that column's min/max
 * onto the response, for coordinator-side shard ordering. Independent of the filters — a
 * query may have either, both, or neither.
 *
 * @opensearch.internal
 */
public class AnalyticsCanMatchRequest extends ActionRequest {

    private final ShardId shardId;
    private final byte[] filterBytes;
    private final String backendId;
    @Nullable
    private final String sortColumn;

    public AnalyticsCanMatchRequest(ShardId shardId, byte[] filterBytes, String backendId) {
        this(shardId, filterBytes, backendId, null);
    }

    public AnalyticsCanMatchRequest(ShardId shardId, byte[] filterBytes, String backendId, @Nullable String sortColumn) {
        this.shardId = shardId;
        this.filterBytes = filterBytes;
        this.backendId = backendId;
        this.sortColumn = sortColumn;
    }

    public AnalyticsCanMatchRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.filterBytes = in.readByteArray();
        this.backendId = in.readString();
        this.sortColumn = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeByteArray(filterBytes);
        out.writeString(backendId);
        out.writeOptionalString(sortColumn);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public byte[] getFilterBytes() {
        return filterBytes;
    }

    public String getBackendId() {
        return backendId;
    }

    /** Column to fold shard-wide min/max for, or {@code null} when the query has no usable sort. */
    @Nullable
    public String getSortColumn() {
        return sortColumn;
    }
}

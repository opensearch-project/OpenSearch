/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Delivers a single shuffle partition (as a {@code byte[]} payload — typically Arrow IPC bytes
 * encoding one or more record batches) to a target worker node. Ported from OLAP's
 * {@code ShuffleDataRequest}.
 *
 * <p>Wire shape: {@code queryId, targetStageId, side ("left"/"right"), int partitionIndex,
 * byte[] data, boolean isLast}.
 *
 * @opensearch.internal
 */
public class AnalyticsShuffleDataRequest extends ActionRequest {

    private final String queryId;
    private final int targetStageId;
    private final String side;
    private final int partitionIndex;
    private final byte[] data;
    private final boolean isLast;
    private final String targetNodeId;

    public AnalyticsShuffleDataRequest(String queryId, int targetStageId, String side, int partitionIndex, byte[] data, boolean isLast) {
        this(queryId, targetStageId, side, partitionIndex, data, isLast, null);
    }

    public AnalyticsShuffleDataRequest(
        String queryId,
        int targetStageId,
        String side,
        int partitionIndex,
        byte[] data,
        boolean isLast,
        String targetNodeId
    ) {
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.side = side;
        this.partitionIndex = partitionIndex;
        this.data = data;
        this.isLast = isLast;
        this.targetNodeId = targetNodeId;
    }

    public AnalyticsShuffleDataRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.targetStageId = in.readVInt();
        this.side = in.readString();
        this.partitionIndex = in.readVInt();
        if (in.readBoolean()) {
            this.data = in.readByteArray();
        } else {
            this.data = null;
        }
        this.isLast = in.readBoolean();
        this.targetNodeId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeVInt(targetStageId);
        out.writeString(side);
        out.writeVInt(partitionIndex);
        if (data != null) {
            out.writeBoolean(true);
            out.writeByteArray(data);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(isLast);
        out.writeOptionalString(targetNodeId);
    }

    /** Target worker node id for routing. Null means "execute on the receiving node" (already
     *  routed by the caller). Set by {@code ShuffleSenderImpl} so the transport handler can
     *  forward when this request lands on a node that isn't the intended target. */
    public String getTargetNodeId() {
        return targetNodeId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getQueryId() {
        return queryId;
    }

    public int getTargetStageId() {
        return targetStageId;
    }

    public String getSide() {
        return side;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isLast() {
        return isLast;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Request for shard-level plan fragment execution.
 */
public class ShardQueryRequest extends ActionRequest {

    private final ShardId shardId;
    private final byte[] planFragment;

    public ShardQueryRequest(ShardId shardId, byte[] planFragment) {
        this.shardId = shardId;
        this.planFragment = planFragment;
    }

    public ShardQueryRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.planFragment = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeByteArray(planFragment);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ShardId shardId() { return shardId; }
    public byte[] getPlanFragment() { return planFragment; }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Response containing usage information per PIT
 *
 * @opensearch.api
 */
@PublicApi(since = "3.11.0")
public class PitUsageResponse extends BroadcastResponse {
    private final List<PitUsageInfo> pitUsages;

    public PitUsageResponse(StreamInput in) throws IOException {
        super(in);
        pitUsages = in.readList(PitUsageInfo::new);
    }

    public PitUsageResponse(
        List<PitUsageInfo> pitUsages,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.pitUsages = pitUsages;
    }

    public List<PitUsageInfo> getPitUsages() {
        return Collections.unmodifiableList(pitUsages);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(pitUsages);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("pits");
        for (PitUsageInfo pitUsage : pitUsages) {
            pitUsage.toXContent(builder, params);
        }
        builder.endArray();
    }
}

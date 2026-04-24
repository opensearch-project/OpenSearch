/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Response for the publish shard action. Contains per-shard success/failure counts.
 *
 * @opensearch.experimental
 */
public class PublishShardResponse extends BroadcastResponse {

    public PublishShardResponse(StreamInput in) throws IOException {
        super(in);
    }

    public PublishShardResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }
}

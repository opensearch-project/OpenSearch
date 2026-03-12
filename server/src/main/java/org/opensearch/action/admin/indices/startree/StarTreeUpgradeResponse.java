/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.startree;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Response for the star tree upgrade action.
 * Contains per-shard results including total, successful, and failed shards.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeUpgradeResponse extends BroadcastResponse {

    StarTreeUpgradeResponse(StreamInput in) throws IOException {
        super(in);
    }

    StarTreeUpgradeResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }
}

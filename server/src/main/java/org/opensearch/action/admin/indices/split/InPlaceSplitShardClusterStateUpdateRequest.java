/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.cluster.ack.ClusterStateUpdateRequest;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Cluster state update request for in-place shard split.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InPlaceSplitShardClusterStateUpdateRequest extends ClusterStateUpdateRequest<InPlaceSplitShardClusterStateUpdateRequest> {

    private final String index;
    private final int shardId;
    private final int splitInto;
    private final String cause;

    public InPlaceSplitShardClusterStateUpdateRequest(String cause, String index, int shardId, int splitInto) {
        this.index = index;
        this.shardId = shardId;
        this.splitInto = splitInto;
        this.cause = cause;
    }

    public String getIndex() {
        return index;
    }

    public int getSplitInto() {
        return splitInto;
    }

    public String cause() {
        return cause;
    }

    public int getShardId() {
        return shardId;
    }
}

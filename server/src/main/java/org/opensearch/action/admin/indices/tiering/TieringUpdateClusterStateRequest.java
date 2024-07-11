/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;

import java.util.Map;

/**
 * Cluster state update request that allows tiering for indices
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieringUpdateClusterStateRequest extends IndicesClusterStateUpdateRequest<TieringUpdateClusterStateRequest> {

    private final Map<Index, String> rejectedIndices;
    private final boolean waitForCompletion;

    public TieringUpdateClusterStateRequest(Map<Index, String> rejectedIndices, boolean waitForCompletion) {
        this.rejectedIndices = rejectedIndices;
        this.waitForCompletion = waitForCompletion;
    }

    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    public Map<Index, String> getRejectedIndices() {
        return rejectedIndices;
    }
}

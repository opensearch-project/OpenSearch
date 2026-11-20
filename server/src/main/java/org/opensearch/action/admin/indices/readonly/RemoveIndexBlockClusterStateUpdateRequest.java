/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.cluster.ack.IndicesClusterStateUpdateRequest;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;

import java.util.Objects;

/**
 * Cluster state update request that allows to remove a block from one or more indices
 *
 * @opensearch.internal
 */
public class RemoveIndexBlockClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<RemoveIndexBlockClusterStateUpdateRequest> {

    private final APIBlock block;

    public RemoveIndexBlockClusterStateUpdateRequest(final APIBlock block) {
        this.block = Objects.requireNonNull(block);
    }

    /**
     * Returns the block to be removed
     */
    public APIBlock getBlock() {
        return block;
    }
}

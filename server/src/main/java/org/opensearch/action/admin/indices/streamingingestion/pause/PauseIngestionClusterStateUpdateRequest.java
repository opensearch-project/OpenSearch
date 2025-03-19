/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.opensearch.cluster.ack.IndicesClusterStateUpdateRequest;

/**
 * Cluster state update request that allows to pause ingestion.
 *
 * @opensearch.experimental
 */
public class PauseIngestionClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<PauseIngestionClusterStateUpdateRequest> {

    private long taskId;

    public PauseIngestionClusterStateUpdateRequest(final long taskId) {
        this.taskId = taskId;
    }

    public long taskId() {
        return taskId;
    }

    public PauseIngestionClusterStateUpdateRequest taskId(final long taskId) {
        this.taskId = taskId;
        return this;
    }
}

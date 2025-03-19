/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.resume;

import org.opensearch.cluster.ack.IndicesClusterStateUpdateRequest;

import java.util.List;

/**
 * Cluster state update request that allows to close one or more indices
 *
 * @opensearch.internal
 */
public class ResumeIngestionClusterStateUpdateRequest extends IndicesClusterStateUpdateRequest<ResumeIngestionClusterStateUpdateRequest> {

    private long taskId;
    private List<ResumeIngestionRequest.ResetSettings> resetSettingsList;

    public ResumeIngestionClusterStateUpdateRequest(final long taskId) {
        this.taskId = taskId;
    }

    public long taskId() {
        return taskId;
    }

    public ResumeIngestionClusterStateUpdateRequest taskId(final long taskId) {
        this.taskId = taskId;
        return this;
    }

    public ResumeIngestionClusterStateUpdateRequest resetSettings(final List<ResumeIngestionRequest.ResetSettings> resetSettingsList) {
        this.resetSettingsList = resetSettingsList;
        return this;
    }

}

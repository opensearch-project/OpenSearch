/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.Nodes;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.action.admin.cluster.ClusterAdminTask;

import java.io.IOException;
import java.util.Map;

/**
 * A request of _cat/nodes.
 *
 * @opensearch.api
 */
public class CatNodesRequest extends ClusterManagerNodeReadRequest<CatNodesRequest> {

    private TimeValue cancelAfterTimeInterval;
    private long timeout = -1;

    public CatNodesRequest() {}

    public CatNodesRequest(StreamInput in) throws IOException {
        super(in);
    }

    public void setCancelAfterTimeInterval(TimeValue timeout) {
        this.cancelAfterTimeInterval = timeout;
    }

    public TimeValue getCancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public ClusterAdminTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ClusterAdminTask(id, type, action, parentTaskId, headers, this.cancelAfterTimeInterval);
    }
}

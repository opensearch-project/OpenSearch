/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.management.decommission;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;

public class ClusterManagementDecommissionRequest extends ClusterManagerNodeRequest<ClusterManagementDecommissionRequest> {

    private String[] nodeIds;
    private String[] nodeNames;
    private TimeValue timeout;

    /**
     * Construct a request to decommission nodes matching the given node names, and wait for a
     * default 30 seconds for these exclusions to take effect, removing the nodes from the cluster.
     *
     * @param nodeNames Names of the nodes to be decommissioned
     */
    public ClusterManagementDecommissionRequest(String... nodeNames) {
        this(Strings.EMPTY_ARRAY, nodeNames, TimeValue.timeValueSeconds(30));
    }

    /**
     * Construct a request to decommission nodes matching the given descriptions, and wait for these
     * nodes to be removed from the cluster.
     *
     * @param nodeIds   Ids of the nodes which are to be decommissioned
     * @param nodeNames Names of the nodes which are to be decommissioned
     * @param timeout   How long to wait for the nodes to be decommissioned.
     */
    public ClusterManagementDecommissionRequest(String[] nodeIds, String[] nodeNames, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }

        if (noneOrMoreThanOneIsSet(nodeIds, nodeNames)) {
            throw new IllegalArgumentException(
                "Please set node identifiers correctly. " + "One and only one of [node_names] and [node_ids] has to be set"
            );
        }

        this.nodeIds = nodeIds;
        this.nodeNames = nodeNames;
        this.timeout = timeout;
    }

    public ClusterManagementDecommissionRequest(StreamInput in) throws IOException {
        super(in);
        nodeIds = in.readStringArray();
        nodeNames = in.readStringArray();
        timeout = in.readTimeValue();
    }

    private boolean noneOrMoreThanOneIsSet(String[] nodeIds, String[] nodeNames) {
        if (nodeIds.length > 0) {
            return nodeNames.length > 0;
        } else {
            return nodeNames.length > 0 == false;
        }
    }

    /**
     * Sets the ids of node to be decommissioned
     */
    public ClusterManagementDecommissionRequest setNodeIds(String... nodeIds) {
        this.nodeIds = nodeIds;
        return this;
    }

    /**
     * @return ids of the nodes for whom to be decommissioned.
     */
    public String[] getNodeIds() {
        return nodeIds;
    }

    /**
     * Sets the names of node to be decommissioned
     */
    public ClusterManagementDecommissionRequest setNodeNames(String... nodeNames) {
        this.nodeNames = nodeNames;
        return this;
    }

    /**
     * @return names of the nodes for whom to be decommissioned.
     */
    public String[] getNodeNames() {
        return nodeNames;
    }

    /**
     * Sets the timeout
     */
    public ClusterManagementDecommissionRequest setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * @return how long to wait after decommissioning the nodes to be removed from the cluster.
     */
    public TimeValue getTimeout() {
        return timeout;
    }


    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(nodeIds);
        out.writeStringArray(nodeNames);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "ClusterManagementDecommissionRequest{"
            + "nodeIds="
            + Arrays.asList(nodeIds)
            + ", "
            + "nodeNames="
            + Arrays.asList(nodeNames)
            + ", "
            + "timeout="
            + timeout
            + '}';
    }
}

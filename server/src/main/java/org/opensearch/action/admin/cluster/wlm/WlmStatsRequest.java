/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.wlm;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A request to get Workload Management Stats
 */
@ExperimentalApi
public class WlmStatsRequest extends BaseNodesRequest<WlmStatsRequest> {

    private final Set<String> workloadGroupIds;
    private final Boolean breach;

    public WlmStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.workloadGroupIds = new HashSet<>(Set.of(in.readStringArray()));
        this.breach = in.readOptionalBoolean();
    }

    /**
     * Get WorkloadGroup stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public WlmStatsRequest(String[] nodesIds, Set<String> workloadGroupIds, Boolean breach) {
        super(nodesIds);
        this.workloadGroupIds = workloadGroupIds;
        this.breach = breach;
    }

    public WlmStatsRequest() {
        super((String[]) null);
        workloadGroupIds = new HashSet<>();
        this.breach = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(workloadGroupIds.toArray(new String[0]));
        out.writeOptionalBoolean(breach);
    }

    public Set<String> getWorkloadGroupIds() {
        return workloadGroupIds;
    }

    public Boolean isBreach() {
        return breach;
    }
}

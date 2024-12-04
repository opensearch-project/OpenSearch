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

    private final Set<String> queryGroupIds;
    private final Boolean breach;

    public WlmStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.queryGroupIds = new HashSet<>(Set.of(in.readStringArray()));
        this.breach = in.readOptionalBoolean();
    }

    /**
     * Get QueryGroup stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public WlmStatsRequest(String[] nodesIds, Set<String> queryGroupIds, Boolean breach) {
        super(false, nodesIds);
        this.queryGroupIds = queryGroupIds;
        this.breach = breach;
    }

    public WlmStatsRequest() {
        super(false, (String[]) null);
        queryGroupIds = new HashSet<>();
        this.breach = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(queryGroupIds.toArray(new String[0]));
        out.writeOptionalBoolean(breach);
    }

    public Set<String> getQueryGroupIds() {
        return queryGroupIds;
    }

    public Boolean isBreach() {
        return breach;
    }
}

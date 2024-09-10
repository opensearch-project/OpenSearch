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

/**
 * A request to get QueryGroupStats
 */
@ExperimentalApi
public class QueryGroupStatsRequest extends BaseNodesRequest<QueryGroupStatsRequest> {

    protected QueryGroupStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public QueryGroupStatsRequest() {
        super(false, (String[]) null);
    }

    /**
     * Get QueryGroup stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public QueryGroupStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}

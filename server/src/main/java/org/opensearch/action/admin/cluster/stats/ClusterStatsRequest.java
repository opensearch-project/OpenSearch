/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get cluster level stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    private boolean includeMappingStats = true;

    private boolean includeAnalysisStats = true;

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_2_16_0)) {
            useAggregatedNodeLevelResponses = in.readOptionalBoolean();
        }
    }

    private Boolean useAggregatedNodeLevelResponses = false;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    public boolean useAggregatedNodeLevelResponses() {
        return useAggregatedNodeLevelResponses;
    }

    public void useAggregatedNodeLevelResponses(boolean useAggregatedNodeLevelResponses) {
        this.useAggregatedNodeLevelResponses = useAggregatedNodeLevelResponses;
    }

    public void setIncludeMappingStats(boolean setIncludeMappingStats) {
        this.includeMappingStats = setIncludeMappingStats;
    }

    public boolean isIncludeMappingStats() {
        return includeMappingStats;
    }

    public void setIncludeAnalysisStats(boolean setIncludeAnalysisStats) {
        this.includeAnalysisStats = setIncludeAnalysisStats;
    }

    public boolean isIncludeAnalysisStats() {
        return includeAnalysisStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            out.writeOptionalBoolean(useAggregatedNodeLevelResponses);
        }
    }

}

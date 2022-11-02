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

package org.opensearch.action.admin.cluster.node.usage;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport request for collecting OpenSearch telemetry
 *
 * @opensearch.internal
 */
public class NodesUsageRequest extends BaseNodesRequest<NodesUsageRequest> {

    private boolean restActions;
    private boolean aggregations;

    public NodesUsageRequest(StreamInput in) throws IOException {
        super(in);
        this.restActions = in.readBoolean();
        this.aggregations = in.readBoolean();
    }

    /**
     * Get usage from nodes based on the nodes ids specified. If none are
     * passed, usage for all nodes will be returned.
     */
    public NodesUsageRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Sets all the request flags.
     */
    public NodesUsageRequest all() {
        this.restActions = true;
        this.aggregations = true;
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesUsageRequest clear() {
        this.restActions = false;
        return this;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public boolean restActions() {
        return this.restActions;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public NodesUsageRequest restActions(boolean restActions) {
        this.restActions = restActions;
        return this;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public boolean aggregations() {
        return this.aggregations;
    }

    /**
     * Should the node rest actions usage statistics be returned.
     */
    public NodesUsageRequest aggregations(boolean aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(restActions);
        out.writeBoolean(aggregations);
    }
}

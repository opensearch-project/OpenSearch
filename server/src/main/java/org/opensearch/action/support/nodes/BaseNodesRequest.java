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

package org.opensearch.action.support.nodes;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Base class for requests targeting a list of nodes
 *
 * @opensearch.internal
 */
public abstract class BaseNodesRequest<Request extends BaseNodesRequest<Request>> extends ActionRequest {

    /**
     * the list of nodesIds that will be used to resolve this request and {@link #concreteNodes}
     * will be populated. Note that if {@link #concreteNodes} is not null, it will be used and nodeIds
     * will be ignored.
     * <p>
     * See {@link DiscoveryNodes#resolveNodes} for a full description of the options.
     * <p>
     * TODO: get rid of this and resolve it to concrete nodes in the rest layer
     **/
    private String[] nodesIds;

    /**
     * once {@link #nodesIds} are resolved this will contain the concrete nodes that are part of this request. If set, {@link #nodesIds}
     * will be ignored and this will be used.
     * */
    private DiscoveryNode[] concreteNodes;

    /**
     * Since do not use the discovery nodes coming from the request in all code paths following a request extended off from
     * BaseNodeRequest, we do not require it to sent around across all nodes.
     *
     * Setting default behavior as `true` but can be explicitly changed in requests that do not require.
     */
    private boolean includeDiscoveryNodes = true;

    private final TimeValue DEFAULT_TIMEOUT_SECS = TimeValue.timeValueSeconds(30);

    private TimeValue timeout;

    protected BaseNodesRequest(StreamInput in) throws IOException {
        super(in);
        nodesIds = in.readStringArray();
        concreteNodes = in.readOptionalArray(DiscoveryNode::new, DiscoveryNode[]::new);
        timeout = in.readOptionalTimeValue();
    }

    protected BaseNodesRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    protected BaseNodesRequest(boolean includeDiscoveryNodes, String... nodesIds) {
        this.nodesIds = nodesIds;
        this.includeDiscoveryNodes = includeDiscoveryNodes;
    }

    protected BaseNodesRequest(DiscoveryNode... concreteNodes) {
        this.nodesIds = null;
        this.concreteNodes = concreteNodes;
    }

    protected BaseNodesRequest(boolean includeDiscoveryNodes, DiscoveryNode... concreteNodes) {
        this.nodesIds = null;
        this.concreteNodes = concreteNodes;
        this.includeDiscoveryNodes = includeDiscoveryNodes;
    }

    public final String[] nodesIds() {
        return nodesIds;
    }

    @SuppressWarnings("unchecked")
    public final Request nodesIds(String... nodesIds) {
        this.nodesIds = nodesIds;
        return (Request) this;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, DEFAULT_TIMEOUT_SECS, getClass().getSimpleName() + ".timeout");
        return (Request) this;
    }

    public DiscoveryNode[] concreteNodes() {
        return concreteNodes;
    }

    public void setConcreteNodes(DiscoveryNode[] concreteNodes) {
        this.concreteNodes = concreteNodes;
    }

    public boolean getIncludeDiscoveryNodes() {
        return includeDiscoveryNodes;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(nodesIds);
        out.writeOptionalArray(concreteNodes);
        out.writeOptionalTimeValue(timeout);
    }
}

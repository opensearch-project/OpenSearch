/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.nodes;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Base class for requests targeting a list of nodes
*
* @opensearch.internal
*/
public abstract class ProtobufBaseNodesRequest<Request extends ProtobufBaseNodesRequest<Request>> extends ProtobufActionRequest {

    /**
     * the list of nodesIds that will be used to resolve this request and {@link #concreteNodes}
    * will be populated. Note that if {@link #concreteNodes} is not null, it will be used and nodeIds
    * will be ignored.
    *
    * See {@link DiscoveryNodes#resolveNodes} for a full description of the options.
    *
    * TODO: get rid of this and resolve it to concrete nodes in the rest layer
    **/
    private String[] nodesIds;

    /**
     * once {@link #nodesIds} are resolved this will contain the concrete nodes that are part of this request. If set, {@link #nodesIds}
    * will be ignored and this will be used.
    * */
    private DiscoveryNode[] concreteNodes;
    private final TimeValue DEFAULT_TIMEOUT_SECS = TimeValue.timeValueSeconds(30);

    private TimeValue timeout;

    protected ProtobufBaseNodesRequest(byte[] data) throws IOException {}

    protected ProtobufBaseNodesRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    protected ProtobufBaseNodesRequest(DiscoveryNode... concreteNodes) {
        this.nodesIds = null;
        this.concreteNodes = concreteNodes;
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

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

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

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Response from peer nodes
 *
 * @opensearch.internal
 */
public class PeersResponse extends TransportResponse {
    private final Optional<DiscoveryNode> clusterManagerNode;
    private final List<DiscoveryNode> knownPeers;
    private final long term;

    public PeersResponse(Optional<DiscoveryNode> clusterManagerNode, List<DiscoveryNode> knownPeers, long term) {
        assert clusterManagerNode.isPresent() == false || knownPeers.isEmpty();
        this.clusterManagerNode = clusterManagerNode;
        this.knownPeers = knownPeers;
        this.term = term;
    }

    public PeersResponse(StreamInput in) throws IOException {
        clusterManagerNode = Optional.ofNullable(in.readOptionalWriteable(DiscoveryNode::new));
        knownPeers = in.readList(DiscoveryNode::new);
        term = in.readLong();
        assert clusterManagerNode.isPresent() == false || knownPeers.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(clusterManagerNode.orElse(null));
        out.writeList(knownPeers);
        out.writeLong(term);
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     */
    public Optional<DiscoveryNode> getClusterManagerNode() {
        return clusterManagerNode;
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #getClusterManagerNode()}
     */
    @Deprecated
    public Optional<DiscoveryNode> getMasterNode() {
        return getClusterManagerNode();
    }

    /**
     * @return the collection of known peers of the responding node, or an empty collection if the responding node believes there
     * is currently a leader.
     */
    public List<DiscoveryNode> getKnownPeers() {
        return knownPeers;
    }

    /**
     * @return the current term of the responding node. If the responding node is the leader then this is the term in which it is
     * currently leading.
     */
    public long getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "PeersResponse{" + "clusterManagerNode=" + clusterManagerNode + ", knownPeers=" + knownPeers + ", term=" + term + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeersResponse that = (PeersResponse) o;
        return term == that.term
            && Objects.equals(clusterManagerNode, that.clusterManagerNode)
            && Objects.equals(knownPeers, that.knownPeers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterManagerNode, knownPeers, term);
    }
}

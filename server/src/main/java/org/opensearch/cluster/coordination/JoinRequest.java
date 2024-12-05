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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Request for a node to join the cluster
 *
 * @opensearch.internal
 */
public class JoinRequest extends TransportRequest {

    /**
     * The sending (i.e. joining) node.
     */
    private final DiscoveryNode sourceNode;

    /**
     * The minimum term for which the joining node will accept any cluster state publications. If the joining node is in a strictly greater
     * term than the cluster-manager it wants to join then the cluster-manager must enter a new term and hold another election. Doesn't necessarily match
     * {@link JoinRequest#optionalJoin} and may be zero in join requests sent prior to {@code LegacyESVersion#V_7_7_0}.
     */
    private final long minimumTerm;

    /**
     * A vote for the receiving node. This vote is optional since the sending node may have voted for a different cluster-manager in this term.
     * That's ok, the sender likely discovered that the cluster-manager we voted for lost the election and now we're trying to join the winner. Once
     * the sender has successfully joined the cluster-manager, the lack of a vote in its term causes another election (see
     * {@link Publication#onMissingJoin(DiscoveryNode)}).
     */
    private final Optional<Join> optionalJoin;

    public JoinRequest(DiscoveryNode sourceNode, long minimumTerm, Optional<Join> optionalJoin) {
        assert optionalJoin.isPresent() == false || optionalJoin.get().getSourceNode().equals(sourceNode);
        this.sourceNode = sourceNode;
        this.minimumTerm = minimumTerm;
        this.optionalJoin = optionalJoin;
    }

    public JoinRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        minimumTerm = in.readLong();
        optionalJoin = Optional.ofNullable(in.readOptionalWriteable(Join::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeToWithAttribute(out);
        out.writeLong(minimumTerm);
        out.writeOptionalWriteable(optionalJoin.orElse(null));
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public long getMinimumTerm() {
        return minimumTerm;
    }

    public long getTerm() {
        // If the join is also present then its term will normally equal the corresponding term, but we do not require callers to
        // obtain the term and the join in a synchronized fashion so it's possible that they disagree. Also older nodes do not share the
        // minimum term, so for BWC we can take it from the join if present.
        return Math.max(minimumTerm, optionalJoin.map(Join::getTerm).orElse(0L));
    }

    public Optional<Join> getOptionalJoin() {
        return optionalJoin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinRequest)) return false;

        JoinRequest that = (JoinRequest) o;

        if (minimumTerm != that.minimumTerm) return false;
        if (!sourceNode.equals(that.sourceNode)) return false;
        return optionalJoin.equals(that.optionalJoin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, minimumTerm, optionalJoin);
    }

    @Override
    public String toString() {
        return "JoinRequest{" + "sourceNode=" + sourceNode + ", minimumTerm=" + minimumTerm + ", optionalJoin=" + optionalJoin + '}';
    }
}

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

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.decider.Decision.Type;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An abstract class for representing various types of allocation decisions.
 *
 * @opensearch.internal
 */
public abstract class AbstractAllocationDecision implements ToXContentFragment, Writeable {

    @Nullable
    protected final DiscoveryNode targetNode;
    @Nullable
    protected final List<NodeAllocationResult> nodeDecisions;

    protected AbstractAllocationDecision(@Nullable DiscoveryNode targetNode, @Nullable List<NodeAllocationResult> nodeDecisions) {
        this.targetNode = targetNode;
        this.nodeDecisions = nodeDecisions != null ? sortNodeDecisions(nodeDecisions) : null;
    }

    protected AbstractAllocationDecision(StreamInput in) throws IOException {
        targetNode = in.readOptionalWriteable(DiscoveryNode::new);
        nodeDecisions = in.readBoolean() ? Collections.unmodifiableList(in.readList(NodeAllocationResult::new)) : null;
    }

    /**
     * Returns {@code true} if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object cannot be accessed and will
     * throw an {@code IllegalStateException}.
     */
    public abstract boolean isDecisionTaken();

    /**
     * Get the node that the allocator will assign the shard to, returning {@code null} if there is no node to
     * which the shard will be assigned or moved.  If {@link #isDecisionTaken()} returns {@code false}, then
     * invoking this method will throw an {@code IllegalStateException}.
     */
    @Nullable
    public DiscoveryNode getTargetNode() {
        checkDecisionState();
        return targetNode;
    }

    /**
     * Gets the sorted list of individual node-level decisions that went into making the ultimate decision whether
     * to allocate or move the shard.  If {@link #isDecisionTaken()} returns {@code false}, then
     * invoking this method will throw an {@code IllegalStateException}.
     */
    @Nullable
    public List<NodeAllocationResult> getNodeDecisions() {
        checkDecisionState();
        return nodeDecisions;
    }

    /**
     * Gets the explanation for the decision.  If {@link #isDecisionTaken()} returns {@code false}, then invoking
     * this method will throw an {@code IllegalStateException}.
     */
    public abstract String getExplanation();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable((stream, node) -> node.writeToWithAttribute(stream), targetNode);
        if (nodeDecisions != null) {
            out.writeBoolean(true);
            out.writeList(nodeDecisions);
        } else {
            out.writeBoolean(false);
        }
    }

    protected void checkDecisionState() {
        if (isDecisionTaken() == false) {
            throw new IllegalStateException("decision was not taken, individual object fields cannot be accessed");
        }
    }

    /**
     * Generates X-Content for a {@link DiscoveryNode} that leaves off some of the non-critical fields.
     */
    public static XContentBuilder discoveryNodeToXContent(DiscoveryNode node, boolean outerObjectWritten, XContentBuilder builder)
        throws IOException {

        builder.field(outerObjectWritten ? "id" : "node_id", node.getId());
        builder.field(outerObjectWritten ? "name" : "node_name", node.getName());
        builder.field("transport_address", node.getAddress().toString());
        if (node.getAttributes().isEmpty() == false) {
            builder.startObject(outerObjectWritten ? "attributes" : "node_attributes");
            for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        return builder;
    }

    /**
     * Sorts a list of node level decisions by the decision type, then by weight ranking, and finally by node id.
     */
    public List<NodeAllocationResult> sortNodeDecisions(List<NodeAllocationResult> nodeDecisions) {
        return Collections.unmodifiableList(nodeDecisions.stream().sorted().collect(Collectors.toList()));
    }

    /**
     * Generates X-Content for the node-level decisions, creating the outer "node_decisions" object
     * in which they are serialized.
     */
    public XContentBuilder nodeDecisionsToXContent(List<NodeAllocationResult> nodeDecisions, XContentBuilder builder, Params params)
        throws IOException {

        if (nodeDecisions != null && nodeDecisions.isEmpty() == false) {
            builder.startArray("node_allocation_decisions");
            {
                for (NodeAllocationResult explanation : nodeDecisions) {
                    explanation.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
        return builder;
    }

    /**
     * Returns {@code true} if there is at least one node that returned a {@link Type#YES} decision for allocating this shard.
     */
    protected boolean atLeastOneNodeWithYesDecision() {
        if (nodeDecisions == null) {
            return false;
        }
        for (NodeAllocationResult result : nodeDecisions) {
            if (result.getNodeDecision() == AllocationDecision.YES) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other instanceof AbstractAllocationDecision == false) {
            return false;
        }
        AbstractAllocationDecision that = (AbstractAllocationDecision) other;
        return Objects.equals(targetNode, that.targetNode) && Objects.equals(nodeDecisions, that.nodeDecisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetNode, nodeDecisions);
    }

}

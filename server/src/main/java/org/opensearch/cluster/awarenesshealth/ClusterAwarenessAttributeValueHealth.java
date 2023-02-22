/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Cluster Awareness AttributeValue Health information
 */
public class ClusterAwarenessAttributeValueHealth implements Writeable, ToXContentFragment {

    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String NODES = "data_nodes";
    private static final String WEIGHTS = "weight";
    private final String name;
    private int activeShards;
    private int unassignedShards;
    private int initializingShards;
    private int relocatingShards;
    private int nodes;
    private double weight;
    private List<String> nodeList;

    /**
     * Creates Awareness AttributeValue Health information
     *
     * @param name name of awareness attribute
     */
    public ClusterAwarenessAttributeValueHealth(String name, List<String> nodeList) {
        this.name = name;
        this.nodeList = nodeList;
    }

    // Constructor use by Unit test case.
    ClusterAwarenessAttributeValueHealth(
        String name,
        int activeShards,
        int initializingShards,
        int relocatingShards,
        int unassignedShards,
        int nodes,
        double weights
    ) {
        this.name = name;
        this.activeShards = activeShards;
        this.initializingShards = initializingShards;
        this.relocatingShards = relocatingShards;
        this.unassignedShards = unassignedShards;
        this.nodes = nodes;
        this.weight = weights;
    }

    public ClusterAwarenessAttributeValueHealth(final StreamInput in) throws IOException {
        name = in.readString();
        activeShards = in.readVInt();
        initializingShards = in.readVInt();
        relocatingShards = in.readVInt();
        unassignedShards = in.readVInt();
        nodes = in.readVInt();
        weight = in.readDouble();
    }

    public static ClusterAwarenessAttributeValueHealth fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        String attributeName = parser.currentName();
        int active_shards = 0;
        int initializing_shards = 0;
        int relocating_shards = 0;
        int unassigned_shards = 0;
        int nodes = 0;
        double weight = 0.0;
        String currentFieldName;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                switch (currentFieldName) {
                    case ACTIVE_SHARDS:
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new OpenSearchParseException(
                                "failed to parse active shards field, expected number but found unknown type"
                            );
                        }
                        active_shards = parser.intValue();
                        break;
                    case INITIALIZING_SHARDS:
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new OpenSearchParseException(
                                "failed to parse initializing shards field, expected number but found unknown type"
                            );
                        }
                        initializing_shards = parser.intValue();
                        break;
                    case RELOCATING_SHARDS:
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new OpenSearchParseException(
                                "failed to parse relocating shards field, expected number but found unknown type"
                            );
                        }
                        relocating_shards = parser.intValue();
                        break;
                    case UNASSIGNED_SHARDS:
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new OpenSearchParseException("failed to parse unassigned field, expected number but found unknown type");
                        }
                        unassigned_shards = parser.intValue();
                        break;
                    case NODES:
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new OpenSearchParseException("failed to parse node field, expected number but found unknown type");
                        }
                        nodes = parser.intValue();
                        break;
                    case WEIGHTS:
                        if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                            throw new OpenSearchParseException("failed to parse weight field, expected number but found unknown type");
                        }
                        weight = parser.doubleValue();
                        break;
                }
            } else {
                throw new OpenSearchParseException(
                    "failed to parse awareness attribute health, expected [{}] but found [{}]",
                    XContentParser.Token.FIELD_NAME,
                    token
                );
            }
        }
        return new ClusterAwarenessAttributeValueHealth(
            attributeName,
            active_shards,
            initializing_shards,
            relocating_shards,
            unassigned_shards,
            nodes,
            weight
        );
    }

    public int getActiveShards() {
        return activeShards;
    }

    public void setActiveShards(int activeShards) {
        this.activeShards = activeShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public void setUnassignedShards(int unassignedShards) {
        this.unassignedShards = unassignedShards;
    }

    public int getNodes() {
        return nodes;
    }

    public void setNodes(int nodes) {
        this.nodes = nodes;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getName() {
        return name;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public void setInitializingShards(int initializingShards) {
        this.initializingShards = initializingShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public void setRelocatingShards(int relocatingShards) {
        this.relocatingShards = relocatingShards;
    }

    public List<String> getNodeList() {
        return nodeList;
    }

    public void setNodeList(List<String> nodeList) {
        this.nodeList = nodeList;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(activeShards);
        out.writeVInt(initializingShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(unassignedShards);
        out.writeVInt(nodes);
        out.writeDouble(weight);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(NODES, getNodes());
        builder.field(WEIGHTS, getWeight());
        builder.endObject();
        return builder;
    }

    void computeAttributeValueLevelInfo(ClusterState clusterState, boolean displayUnassignedShardLevelInfo, int shardsPerAttributeValue) {
        // computing nodes info
        nodes = nodeList.size();

        // computing shards into
        setShardLevelInfo(clusterState, displayUnassignedShardLevelInfo, shardsPerAttributeValue);

        // compute weight info
        setWeightInfo(clusterState);
    }

    private void setShardLevelInfo(ClusterState clusterState, boolean displayUnassignedShardLevelInfo, int shardsPerAttributeValue) {

        for (String nodeId : nodeList) {
            RoutingNode node = clusterState.getRoutingNodes().node(nodeId);
            activeShards += node.numberOfShardsWithState(ShardRoutingState.STARTED);
            relocatingShards += node.numberOfShardsWithState(ShardRoutingState.RELOCATING);
            initializingShards += node.numberOfShardsWithState(ShardRoutingState.INITIALIZING);
        }

        // computing unassigned shards info
        if (displayUnassignedShardLevelInfo) {
            int unassignedShardsPerAttribute = shardsPerAttributeValue - getActiveShards() - getInitializingShards();
            setUnassignedShards(unassignedShardsPerAttribute);
        } else {
            setUnassignedShards(-1);
        }
    }

    private void setWeightInfo(ClusterState clusterState) {
        WeightedRoutingMetadata weightedRoutingMetadata = clusterState.getMetadata().weightedRoutingMetadata();
        double attributeWeight = 1.0;
        if (weightedRoutingMetadata != null) {
            WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
            attributeWeight = weightedRouting.weights().getOrDefault(name, 1.0);
        }
        setWeight(attributeWeight);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterAwarenessAttributeValueHealth)) return false;
        ClusterAwarenessAttributeValueHealth that = (ClusterAwarenessAttributeValueHealth) o;
        return name.equals(that.name)
            && activeShards == that.activeShards
            && relocatingShards == that.relocatingShards
            && initializingShards == that.initializingShards
            && unassignedShards == that.unassignedShards
            && nodes == that.nodes
            && weight == that.weight;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, activeShards, relocatingShards, initializingShards, unassignedShards, nodes, weight);
    }
}

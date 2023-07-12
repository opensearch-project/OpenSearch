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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Cluster Awareness health information
 *
 */
public class ClusterAwarenessAttributesHealth implements Iterable<ClusterAwarenessAttributeValueHealth>, Writeable, ToXContentFragment {

    private final String awarenessAttributeName;
    private Map<String, ClusterAwarenessAttributeValueHealth> awarenessAttributeValueHealthMap;

    /**
     * Creates Awareness AttributeValue Health information
     *
     * @param awarenessAttributeValue Awareness Attribute value ie zone, rack etc
     * @param displayUnassignedShardLevelInfo Governs if unassigned info should be visible or not
     * @param clusterState cluster state
     */
    public ClusterAwarenessAttributesHealth(
        String awarenessAttributeValue,
        boolean displayUnassignedShardLevelInfo,
        ClusterState clusterState
    ) {
        awarenessAttributeName = awarenessAttributeValue;

        // This is the Map which is storing the per attribute node list.
        Map<String, List<String>> attributesNodeList = new HashMap<>();

        // Getting the node map for cluster
        final Map<String, DiscoveryNode> nodeMap = clusterState.nodes().getDataNodes();

        // This is the map that would store all the stats per attribute ie
        // health stats for rack-1, rack-2 etc.
        awarenessAttributeValueHealthMap = new HashMap<>();
        String attributeValue;

        if (!nodeMap.isEmpty()) {
            Iterator<String> iter = nodeMap.keySet().iterator();
            while (iter.hasNext()) {
                List<String> clusterAwarenessAttributeNodeList;
                String node = iter.next();
                DiscoveryNode nodeDiscovery = nodeMap.get(node);
                Map<String, String> nodeAttributes = nodeDiscovery.getAttributes();
                if (!nodeAttributes.isEmpty()) {
                    if (nodeAttributes.containsKey(awarenessAttributeName)) {
                        attributeValue = nodeAttributes.get(awarenessAttributeName);

                        if (!attributesNodeList.containsKey(attributeValue)) {
                            clusterAwarenessAttributeNodeList = new ArrayList<>();
                            attributesNodeList.put(attributeValue, clusterAwarenessAttributeNodeList);
                        } else {
                            clusterAwarenessAttributeNodeList = attributesNodeList.get(attributeValue);
                        }
                        clusterAwarenessAttributeNodeList.add(node);
                    }
                }
            }
        }

        setClusterAwarenessAttributeValue(attributesNodeList, displayUnassignedShardLevelInfo, clusterState);
    }

    private void setClusterAwarenessAttributeValue(
        Map<String, List<String>> perAttributeValueNodeList,
        boolean displayUnassignedShardLevelInfo,
        ClusterState clusterState
    ) {
        int numAttributes = perAttributeValueNodeList.size();
        int shardsPerAttributeValue = 0;

        // Can happen customer has defined weights as well as awareness attribute but no node level attribute was there
        // So to avoid divide-by-zero error checking this
        if (numAttributes != 0) {
            shardsPerAttributeValue = clusterState.getMetadata().getTotalNumberOfShards() / numAttributes;
        }

        Map<String, ClusterAwarenessAttributeValueHealth> clusterAwarenessAttributeValueHealthMap = new HashMap<>();

        for (String attributeValueKey : perAttributeValueNodeList.keySet()) {
            ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth = new ClusterAwarenessAttributeValueHealth(
                attributeValueKey,
                perAttributeValueNodeList.get(attributeValueKey)
            );
            // computing attribute info
            clusterAwarenessAttributeValueHealth.computeAttributeValueLevelInfo(
                clusterState,
                displayUnassignedShardLevelInfo,
                shardsPerAttributeValue
            );
            clusterAwarenessAttributeValueHealthMap.put(attributeValueKey, clusterAwarenessAttributeValueHealth);
        }
        awarenessAttributeValueHealthMap = clusterAwarenessAttributeValueHealthMap;
    }

    public ClusterAwarenessAttributesHealth(final StreamInput in) throws IOException {
        awarenessAttributeName = in.readString();
        int size = in.readVInt();
        if (size > 0) {
            awarenessAttributeValueHealthMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth = new ClusterAwarenessAttributeValueHealth(in);
                awarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth.getName(), clusterAwarenessAttributeValueHealth);
            }
        } else {
            awarenessAttributeValueHealthMap = Collections.emptyMap();
        }
    }

    ClusterAwarenessAttributesHealth(
        String awarenessAttributeName,
        Map<String, ClusterAwarenessAttributeValueHealth> awarenessAttributeValueHealthMap
    ) {
        this.awarenessAttributeName = awarenessAttributeName;
        this.awarenessAttributeValueHealthMap = awarenessAttributeValueHealthMap;
    }

    public String getAwarenessAttributeName() {
        return awarenessAttributeName;
    }

    public Map<String, ClusterAwarenessAttributeValueHealth> getAwarenessAttributeHealthMap() {
        return awarenessAttributeValueHealthMap;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(awarenessAttributeName);
        int size = awarenessAttributeValueHealthMap.size();
        out.writeVInt(size);
        if (size > 0) {
            for (ClusterAwarenessAttributeValueHealth attributeHealthMapPerValue : this) {
                attributeHealthMapPerValue.writeTo(out);
            }
        }
    }

    @Override
    public Iterator<ClusterAwarenessAttributeValueHealth> iterator() {
        return awarenessAttributeValueHealthMap.values().iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getAwarenessAttributeName());
        for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : this) {
            clusterAwarenessAttributeValueHealth.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    public static ClusterAwarenessAttributesHealth fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String attributeName = parser.currentName();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        Map<String, ClusterAwarenessAttributeValueHealth> clusterAwarenessAttributeValueHealthMap = new HashMap<>();
        String currentFieldName;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String attributeValue = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                int active_shards = 0;
                int initializing_shards = 0;
                int relocating_shards = 0;
                int unassigned_shards = 0;
                int nodes = 0;
                double weight = 0.0;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        switch (currentFieldName) {
                            case "active_shards":
                                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                    throw new OpenSearchParseException(
                                        "failed to parse active shards field, expected number but found unknown type"
                                    );
                                }
                                active_shards = parser.intValue();
                                break;
                            case "initializing_shards":
                                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                    throw new OpenSearchParseException(
                                        "failed to parse initializing shards field, expected number but found unknown type"
                                    );
                                }
                                initializing_shards = parser.intValue();
                                break;
                            case "relocating_shards":
                                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                    throw new OpenSearchParseException(
                                        "failed to parse relocating shards field, expected number but found unknown type"
                                    );
                                }
                                relocating_shards = parser.intValue();
                                break;
                            case "unassigned_shards":
                                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                    throw new OpenSearchParseException(
                                        "failed to parse unassigned field, expected number but found unknown type"
                                    );
                                }
                                unassigned_shards = parser.intValue();
                                break;
                            case "data_nodes":
                                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                    throw new OpenSearchParseException(
                                        "failed to parse node field, expected number but found unknown type"
                                    );
                                }
                                nodes = parser.intValue();
                                break;
                            case "weight":
                                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                    throw new OpenSearchParseException(
                                        "failed to parse weight field, expected number but found unknown type"
                                    );
                                }
                                weight = parser.doubleValue();
                                break;
                        }
                    } else {
                        throw new OpenSearchParseException("failed to parse attribute health map");
                    }
                }
                ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth = new ClusterAwarenessAttributeValueHealth(
                    attributeValue,
                    active_shards,
                    initializing_shards,
                    relocating_shards,
                    unassigned_shards,
                    nodes,
                    weight
                );
                clusterAwarenessAttributeValueHealthMap.put(
                    clusterAwarenessAttributeValueHealth.getName(),
                    clusterAwarenessAttributeValueHealth
                );
            } else {
                throw new OpenSearchParseException("failed to parse awareness attribute value health");
            }
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return new ClusterAwarenessAttributesHealth(attributeName, clusterAwarenessAttributeValueHealthMap);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterAwarenessAttributesHealth)) return false;
        ClusterAwarenessAttributesHealth that = (ClusterAwarenessAttributesHealth) o;
        return awarenessAttributeName.equals(that.awarenessAttributeName)
            && awarenessAttributeValueHealthMap.size() == that.awarenessAttributeValueHealthMap.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(awarenessAttributeName, awarenessAttributeValueHealthMap);
    }
}

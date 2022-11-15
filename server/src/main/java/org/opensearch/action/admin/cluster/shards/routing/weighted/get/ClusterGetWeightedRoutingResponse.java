/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.get;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionResponse;

import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response from fetching weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWeightedRoutingResponse extends ActionResponse implements ToXContentObject {
    private WeightedRouting weightedRouting;
    private String localNodeWeight;
    private static final String NODE_WEIGHT = "node_weight";
    private long version;

    public String getLocalNodeWeight() {
        return localNodeWeight;
    }

    ClusterGetWeightedRoutingResponse() {
        this.weightedRouting = null;
    }

    public ClusterGetWeightedRoutingResponse(String localNodeWeight, WeightedRouting weightedRouting, long version) {
        this.localNodeWeight = localNodeWeight;
        this.weightedRouting = weightedRouting;
        this.version = version;
    }

    ClusterGetWeightedRoutingResponse(StreamInput in) throws IOException {
        if (in.available() != 0) {
            this.weightedRouting = new WeightedRouting(in);
            this.version = in.readLong();
        }
    }

    /**
     * List of weights to return
     *
     * @return list or weights
     */
    public WeightedRouting weights() {
        return this.weightedRouting;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (weightedRouting != null) {
            weightedRouting.writeTo(out);
            out.writeLong(version);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.weightedRouting != null) {
            for (Map.Entry<String, Double> entry : weightedRouting.weights().entrySet()) {
                builder.field(entry.getKey(), entry.getValue().toString());
            }
            if (localNodeWeight != null) {
                builder.field(NODE_WEIGHT, localNodeWeight);
            }
            builder.field(WeightedRoutingMetadata.VERSION, version);
        }
        builder.endObject();
        return builder;
    }

    public static ClusterGetWeightedRoutingResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        String attrKey = null, attrValue = null;
        String localNodeWeight = null;
        Map<String, Double> weights = new HashMap<>();
        long version = -1;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                attrKey = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                attrValue = parser.text();
                if (attrKey != null && attrKey.equals(NODE_WEIGHT)) {
                    localNodeWeight = attrValue;
                } else if (attrKey != null && attrKey.equals(WeightedRoutingMetadata.VERSION)) {
                    version = Long.parseLong(attrValue);
                } else if (attrKey != null) {
                    weights.put(attrKey, Double.parseDouble(attrValue));
                }
            } else {
                throw new OpenSearchParseException("failed to parse weighted routing response");
            }
        }
        WeightedRouting weightedRouting = new WeightedRouting("", weights);
        return new ClusterGetWeightedRoutingResponse(localNodeWeight, weightedRouting, version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterGetWeightedRoutingResponse that = (ClusterGetWeightedRoutingResponse) o;
        return weightedRouting.equals(that.weightedRouting) && localNodeWeight.equals(that.localNodeWeight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weightedRouting, localNodeWeight);
    }
}

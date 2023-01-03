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
    public WeightedRouting getWeightedRouting() {
        return weightedRouting;
    }

    private final WeightedRouting weightedRouting;

    public Boolean getDiscoveredMaster() {
        return discoveredMaster;
    }

    private final Boolean discoveredMaster;

    private static final String DISCOVERED_MASTER = "discovered_master";

    ClusterGetWeightedRoutingResponse() {
        this.weightedRouting = null;
        this.discoveredMaster = null;
    }

    public ClusterGetWeightedRoutingResponse(WeightedRouting weightedRouting, Boolean discoveredMaster) {
        this.discoveredMaster = discoveredMaster;
        this.weightedRouting = weightedRouting;
    }

    ClusterGetWeightedRoutingResponse(StreamInput in) throws IOException {
        if (in.available() != 0) {
            this.weightedRouting = new WeightedRouting(in);
            this.discoveredMaster = in.readOptionalBoolean();
        } else {
            this.weightedRouting = null;
            this.discoveredMaster = null;
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
        }
        out.writeOptionalBoolean(discoveredMaster);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.weightedRouting != null) {
            for (Map.Entry<String, Double> entry : weightedRouting.weights().entrySet()) {
                builder.field(entry.getKey(), entry.getValue().toString());
            }
            if (discoveredMaster != null) {
                builder.field(DISCOVERED_MASTER, discoveredMaster);
            }
        }
        builder.endObject();
        return builder;
    }

    public static ClusterGetWeightedRoutingResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        String attrKey = null, attrValue = null;
        Boolean discoveredMaster = null;
        Map<String, Double> weights = new HashMap<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                attrKey = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                attrValue = parser.text();
                if (attrKey != null && attrKey.equals(DISCOVERED_MASTER)) {
                    discoveredMaster = Boolean.parseBoolean(attrValue);
                } else if (attrKey != null) {
                    weights.put(attrKey, Double.parseDouble(attrValue));
                }
            } else {
                throw new OpenSearchParseException("failed to parse weighted routing response");
            }
        }
        WeightedRouting weightedRouting = new WeightedRouting("", weights);
        return new ClusterGetWeightedRoutingResponse(weightedRouting, discoveredMaster);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterGetWeightedRoutingResponse that = (ClusterGetWeightedRoutingResponse) o;
        return weightedRouting.equals(that.weightedRouting) && discoveredMaster.equals(that.discoveredMaster);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weightedRouting, discoveredMaster);
    }
}

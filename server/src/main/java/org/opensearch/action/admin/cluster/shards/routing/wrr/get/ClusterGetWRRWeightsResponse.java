/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionResponse;

import org.opensearch.cluster.routing.WRRWeights;
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
public class ClusterGetWRRWeightsResponse extends ActionResponse implements ToXContentObject {
    private WRRWeights wrrWeight;
    private String localNodeWeight;
    private static final String NODE_WEIGHT = "node_weight";

    public Object getLocalNodeWeight() {
        return localNodeWeight;
    }

    ClusterGetWRRWeightsResponse() {
        this.wrrWeight = null;
    }

    public ClusterGetWRRWeightsResponse(String localNodeWeight, WRRWeights wrrWeight) {
        this.localNodeWeight = localNodeWeight;
        this.wrrWeight = wrrWeight;
    }

    ClusterGetWRRWeightsResponse(StreamInput in) throws IOException {
        if (in.available() != 0) {
            this.wrrWeight = new WRRWeights(in);

        }
    }

    /**
     * List of weights to return
     *
     * @return list or weights
     */
    public WRRWeights weights() {
        return this.wrrWeight;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (wrrWeight != null) {
            wrrWeight.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.wrrWeight != null) {
            for (Map.Entry<String, Object> entry : wrrWeight.weights().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            if (localNodeWeight != null) {
                builder.field(NODE_WEIGHT, localNodeWeight.toString());
            }
        }
        builder.endObject();
        return builder;
    }

    public static ClusterGetWRRWeightsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        String attrKey = null, attrValue = null;
        String localNodeWeight = null;
        Map<String, Object> weights = new HashMap<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                attrKey = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                attrValue = parser.text();
                if (attrKey != null && attrKey.equals(NODE_WEIGHT)) {
                    localNodeWeight = attrValue;
                } else if (attrKey != null) {
                    weights.put(attrKey, attrValue);
                }
            } else {
                throw new OpenSearchParseException("failed to parse wrr response");
            }

        }
        WRRWeights wrrWeights = new WRRWeights("", weights);
        return new ClusterGetWRRWeightsResponse(localNodeWeight, wrrWeights);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterGetWRRWeightsResponse that = (ClusterGetWRRWeightsResponse) o;
        return wrrWeight.equals(that.wrrWeight) && localNodeWeight.equals(that.localNodeWeight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrrWeight, localNodeWeight);
    }
}

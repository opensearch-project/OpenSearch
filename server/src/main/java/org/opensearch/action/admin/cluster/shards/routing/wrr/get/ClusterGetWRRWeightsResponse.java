/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionResponse;

import org.opensearch.cluster.metadata.WeightedRoundRobinMetadata;
import org.opensearch.cluster.routing.WRRWeight;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response from fetching weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWRRWeightsResponse extends ActionResponse implements ToXContentObject {
    private WRRWeight wrrWeight;
    private Object localNodeWeight;

    ClusterGetWRRWeightsResponse() {
        this.wrrWeight = null;
    }

    ClusterGetWRRWeightsResponse(Object localNodeWeight, WRRWeight wrrWeight) {
        this.localNodeWeight = localNodeWeight;
        this.wrrWeight = wrrWeight;

    }

    ClusterGetWRRWeightsResponse(WRRWeight wrrWeights) {
        this.wrrWeight = wrrWeights;
    }

    ClusterGetWRRWeightsResponse(WeightedRoundRobinMetadata metadata) {

        this.wrrWeight = metadata.getWrrWeight();
    }

    ClusterGetWRRWeightsResponse(StreamInput in) throws IOException {
        if (in.available() != 0) {
            this.wrrWeight = new WRRWeight(in);

        }
    }

    /**
     * List of weights to return
     *
     * @return list or weights
     */
    public WRRWeight weights() {
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
                builder.field("node_weight", localNodeWeight.toString());
            }
        } else if (localNodeWeight != null) {
            builder.field("node_weight", localNodeWeight.toString());
        } else {
            builder.field("msg", "Weights are not set");
        }

        builder.endObject();

        return builder;
    }

    public static ClusterGetWRRWeightsResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return new ClusterGetWRRWeightsResponse(WeightedRoundRobinMetadata.fromXContent(parser));
    }
}

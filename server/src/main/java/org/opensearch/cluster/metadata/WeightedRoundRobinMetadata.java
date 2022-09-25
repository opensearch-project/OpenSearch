/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.routing.WRRWeights;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains metadata for weighted round-robin shard routing weights
 *
 * @opensearch.internal
 */
public class WeightedRoundRobinMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private static final Logger logger = LogManager.getLogger(WeightedRoundRobinMetadata.class);
    public static final String TYPE = "wrr_shard_routing";
    private WRRWeights wrrWeight;

    public WRRWeights getWrrWeight() {
        return wrrWeight;
    }

    public void setWrrWeight(WRRWeights wrrWeight) {
        this.wrrWeight = wrrWeight;
    }

    public WeightedRoundRobinMetadata(StreamInput in) throws IOException {
        this.wrrWeight = new WRRWeights(in);
    }

    public WeightedRoundRobinMetadata(WRRWeights wrrWeight) {
        this.wrrWeight = wrrWeight;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_2_3_0;

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        wrrWeight.writeTo(out);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    public static WeightedRoundRobinMetadata fromXContent(XContentParser parser) throws IOException {
        String attrKey = null;
        Object attrValue;
        String attributeName = null;
        Map<String, Object> weights = new HashMap<>();
        WRRWeights wrrWeight = null;
        XContentParser.Token token;
        // move to the first alias
        parser.nextToken();
        String awarenessKeyword = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                // parses awareness object
                awarenessKeyword = parser.currentName();
                // awareness object contains object with awareness attribute name and its details
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new OpenSearchParseException("failed to parse wrr metadata  [{}], expected object", awarenessKeyword);
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    attributeName = parser.currentName();
                    // awareness attribute object contain wrr weight details
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new OpenSearchParseException("failed to parse wrr metadata  [{}], expected object", attributeName);
                    }
                    // parse weights, corresponding attribute key and puts it in a map
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            attrKey = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            attrValue = parser.text();
                            weights.put(attrKey, attrValue);
                        } else {
                            throw new OpenSearchParseException("failed to parse wrr metadata attribute [{}], unknown type", attributeName);
                        }
                    }
                }
            } else {
                throw new OpenSearchParseException("failed to parse wrr metadata attribute [{}]", attributeName);
            }
        }
        wrrWeight = new WRRWeights(attributeName, weights);
        return new WeightedRoundRobinMetadata(wrrWeight);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedRoundRobinMetadata that = (WeightedRoundRobinMetadata) o;
        return wrrWeight.equals(that.wrrWeight);
    }

    @Override
    public int hashCode() {
        return wrrWeight.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        toXContent(wrrWeight, builder);
        return builder;
    }

    public static void toXContent(WRRWeights wrrWeight, XContentBuilder builder) throws IOException {
        builder.startObject("awareness");
        builder.startObject(wrrWeight.attributeName());
        for (Map.Entry<String, Object> entry : wrrWeight.weights().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

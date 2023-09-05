/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to update weights for weighted round-robin shard routing policy.
 *
 * @opensearch.internal
 */
public class ClusterPutWeightedRoutingRequest extends ClusterManagerNodeRequest<ClusterPutWeightedRoutingRequest> {
    private static final Logger logger = LogManager.getLogger(ClusterPutWeightedRoutingRequest.class);

    private WeightedRouting weightedRouting;
    private String attributeName;
    private long version;

    public void version(long version) {
        this.version = version;
    }

    public long getVersion() {
        return this.version;
    }

    public ClusterPutWeightedRoutingRequest() {}

    public WeightedRouting getWeightedRouting() {
        return weightedRouting;
    }

    public ClusterPutWeightedRoutingRequest setWeightedRouting(WeightedRouting weightedRouting) {
        this.weightedRouting = weightedRouting;
        return this;
    }

    public void attributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public ClusterPutWeightedRoutingRequest(StreamInput in) throws IOException {
        super(in);
        weightedRouting = new WeightedRouting(in);
        version = in.readLong();
    }

    public ClusterPutWeightedRoutingRequest(String attributeName) {
        this.attributeName = attributeName;
    }

    public void setWeightedRouting(Map<String, Object> source) {
        try {
            if (source.isEmpty()) {
                throw new OpenSearchParseException(("Empty request body"));
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            setWeightedRouting(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public void setWeightedRouting(BytesReference source, MediaType contentType) {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                contentType
            )
        ) {
            String attrValue = null;
            Map<String, Double> weights = new HashMap<>();
            Double attrWeight = null;
            XContentParser.Token token;
            // move to the first alias
            parser.nextToken();
            String versionAttr = null;
            String weightsAttr;
            long version = WeightedRoutingMetadata.VERSION_UNSET_VALUE;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if (fieldName != null && fieldName.equals(WeightedRoutingMetadata.VERSION)) {
                        versionAttr = parser.currentName();
                        continue;
                    } else if (fieldName != null && fieldName.equals("weights")) {
                        weightsAttr = parser.currentName();
                    } else {
                        throw new OpenSearchParseException("failed to parse weighted routing request object [{}]", fieldName);
                    }
                    if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                        throw new OpenSearchParseException(
                            "failed to parse weighted routing request object  [{}], expected object",
                            weightsAttr
                        );
                    }

                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            attrValue = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            attrWeight = Double.parseDouble(parser.text());
                            weights.put(attrValue, attrWeight);
                        } else {
                            throw new OpenSearchParseException(
                                "failed to parse weighted routing request attribute [{}], " + "unknown type",
                                attrWeight
                            );
                        }
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (versionAttr != null && versionAttr.equals(WeightedRoutingMetadata.VERSION)) {
                        version = parser.longValue();
                    }
                } else {
                    throw new OpenSearchParseException(
                        "failed to parse weighted routing request " + "[{}], unknown " + "type",
                        attributeName
                    );
                }
            }
            this.weightedRouting = new WeightedRouting(this.attributeName, weights);
            this.version = version;
        } catch (IOException e) {
            logger.error("error while parsing put weighted routing request object", e);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (weightedRouting == null) {
            validationException = addValidationError("Weighted routing request object is null", validationException);
        }
        if (weightedRouting.attributeName() == null || weightedRouting.attributeName().isEmpty()) {
            validationException = addValidationError("Attribute name is missing", validationException);
        }
        if (weightedRouting.weights() == null || weightedRouting.weights().isEmpty()) {
            validationException = addValidationError("Weights are missing", validationException);
        }
        if (version == WeightedRoutingMetadata.VERSION_UNSET_VALUE) {
            validationException = addValidationError("Version is missing", validationException);
        }
        int countValueWithZeroWeights = 0;
        double weight;
        try {
            for (Object value : weightedRouting.weights().values()) {
                if (value == null) {
                    validationException = addValidationError(("Weight is null"), validationException);
                } else {
                    weight = Double.parseDouble(value.toString());
                    countValueWithZeroWeights = (weight == 0) ? countValueWithZeroWeights + 1 : countValueWithZeroWeights;
                }
            }
        } catch (NumberFormatException e) {
            validationException = addValidationError(("Weight is not a number"), validationException);
        }
        // Returning validation exception here itself if it is not null, so we can have a descriptive message for the count check
        if (validationException != null) {
            return validationException;
        }
        if (countValueWithZeroWeights > weightedRouting.weights().size() / 2) {
            validationException = addValidationError(
                (String.format(
                    Locale.ROOT,
                    "There are too many attribute values [%s] given zero weight [%d]. Maximum expected number of routing weights having zero weight is [%d]",
                    weightedRouting.weights().toString(),
                    countValueWithZeroWeights,
                    weightedRouting.weights().size() / 2
                )),
                null
            );
        }
        return validationException;
    }

    /**
     * @param source weights definition from request body
     * @return this request
     */
    public ClusterPutWeightedRoutingRequest source(Map<String, Object> source) {
        setWeightedRouting(source);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        weightedRouting.writeTo(out);
        out.writeLong(version);
    }

    @Override
    public String toString() {
        return "ClusterPutWeightedRoutingRequest{" + "weightedRouting= " + weightedRouting.toString() + "version= " + version + "}";
    }

}

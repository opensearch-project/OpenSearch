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
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
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
    }

    public ClusterPutWeightedRoutingRequest(String attributeName) {
        this.attributeName = attributeName;
    }

    public void setWeightedRouting(Map<String, String> source) {
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

    public void setWeightedRouting(BytesReference source, XContentType contentType) {
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
            this.weightedRouting = new WeightedRouting(this.attributeName, weights);
        } catch (IOException e) {
            logger.error("error while parsing put for weighted routing request object", e);
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
        try {
            for (Object value : weightedRouting.weights().values()) {
                if (value == null) {
                    validationException = addValidationError(("Weight is null"), validationException);
                } else {
                    Double.parseDouble(value.toString());
                }
            }
        } catch (NumberFormatException e) {
            validationException = addValidationError(("Weight is not a number"), validationException);
        }
        return validationException;
    }

    /**
     * @param source weights definition from request body
     * @return this request
     */
    public ClusterPutWeightedRoutingRequest source(Map<String, String> source) {
        setWeightedRouting(source);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        weightedRouting.writeTo(out);
    }

    @Override
    public String toString() {
        return "ClusterPutWeightedRoutingRequest{" + "weightedRouting= " + weightedRouting.toString() + "}";
    }

}

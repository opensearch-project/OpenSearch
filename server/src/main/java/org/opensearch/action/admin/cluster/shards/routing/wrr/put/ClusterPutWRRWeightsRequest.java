/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.routing.WRRWeights;
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
public class ClusterPutWRRWeightsRequest extends ClusterManagerNodeRequest<ClusterPutWRRWeightsRequest> {
    private static final Logger logger = LogManager.getLogger(ClusterPutWRRWeightsRequest.class);

    private WRRWeights wrrWeight;
    private String attributeName;

    public WRRWeights wrrWeight() {
        return wrrWeight;
    }

    public ClusterPutWRRWeightsRequest wrrWeight(WRRWeights wrrWeight) {
        this.wrrWeight = wrrWeight;
        return this;
    }

    public void attributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public ClusterPutWRRWeightsRequest(StreamInput in) throws IOException {
        super(in);
        wrrWeight = new WRRWeights(in);
    }

    public ClusterPutWRRWeightsRequest() {

    }

    public void setWRRWeight(Map<String, String> source) {
        try {
            if (source.isEmpty()) {
                throw new OpenSearchParseException(("Empty request body"));
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            setWRRWeight(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public void setWRRWeight(BytesReference source, XContentType contentType) {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                contentType
            )
        ) {
            String attrValue = null;
            Map<String, Object> weights = new HashMap<>();
            Object attrWeight = null;
            XContentParser.Token token;
            // move to the first alias
            parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    attrValue = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    attrWeight = parser.text();
                    weights.put(attrValue, attrWeight);
                } else {
                    throw new OpenSearchParseException("failed to parse wrr request attribute [{}], unknown type", attrWeight);
                }
            }
            this.wrrWeight = new WRRWeights(this.attributeName, weights);
        } catch (IOException e) {
            logger.error("error while parsing put wrr weights request object", e);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (wrrWeight == null) {
            validationException = addValidationError("WRRWeights object is null", validationException);
        }
        if (wrrWeight.attributeName() == null || wrrWeight.attributeName().isEmpty()) {
            validationException = addValidationError("Attribute name is missing", validationException);
        }
        if (wrrWeight.weights() == null || wrrWeight.weights().isEmpty()) {
            validationException = addValidationError("Weights are missing", validationException);
        }
        int countValueWithZeroWeights = 0;
        int weight;
        try {
            for (Object value : wrrWeight.weights().values()) {
                if (value == null) {
                    validationException = addValidationError(("Weight is null"), validationException);
                } else {
                    weight = Integer.parseInt(value.toString());
                    countValueWithZeroWeights = (weight == 0) ? countValueWithZeroWeights + 1 : countValueWithZeroWeights;
                }
            }
        } catch (NumberFormatException e) {
            validationException = addValidationError(("Weight is non-integer"), validationException);
        }
        if (countValueWithZeroWeights > 1) {
            validationException = addValidationError(("More than one value has weight set as 0 "), validationException);
        }

        return validationException;
    }

    /**
     * @param source weights definition from request body
     * @return this request
     */
    public ClusterPutWRRWeightsRequest source(Map<String, String> source) {
        setWRRWeight(source);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        wrrWeight.writeTo(out);
    }

    @Override
    public String toString() {
        return "ClusterPutWRRWeightsRequest{" + "wrrWeight= " + wrrWeight.toString() + "}";
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.routing.WRRWeight;
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

    private WRRWeight wrrWeight;
    private String attributeName;

    public WRRWeight wrrWeight() {
        return wrrWeight;
    }

    public ClusterPutWRRWeightsRequest wrrWeight(WRRWeight wrrWeight) {
        this.wrrWeight = wrrWeight;
        return this;
    }

    public void attributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public ClusterPutWRRWeightsRequest(StreamInput in) throws IOException {
        super(in);
        wrrWeight = new WRRWeight(in);
    }

    public ClusterPutWRRWeightsRequest() {

    }

    public ClusterPutWRRWeightsRequest setWRRWeight(Map<String, String> source) {
        try {
            if (source.isEmpty()) {
                throw new OpenSearchParseException(("Empty request body"));
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return setWRRWeight(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public ClusterPutWRRWeightsRequest setWRRWeight(BytesReference source, XContentType contentType) {
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
            this.wrrWeight = new WRRWeight(this.attributeName, weights);
            return this;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (wrrWeight == null) {
            validationException = addValidationError("Weights are missing", validationException);
        }
        int countValueWithZeroWeights = 0;
        for (Object value : wrrWeight.weights().values()) {
            if (value.toString().equals("0")) {
                countValueWithZeroWeights++;
            }
        }
        if (countValueWithZeroWeights > 1) {
            validationException = addValidationError(("More than one value has weight set as 0 "), validationException);
        }

        return validationException;
    }

    /**
     *
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

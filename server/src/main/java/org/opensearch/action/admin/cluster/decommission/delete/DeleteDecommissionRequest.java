/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.delete;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.decommission.put.PutDecommissionRequest;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.cluster.decommission.DecommissionAttribute;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeleteDecommissionRequest extends AcknowledgedRequest<DeleteDecommissionRequest> {
    private String name;
    private DecommissionAttribute decommissionAttribute;

    public DeleteDecommissionRequest() {
    }

    public DeleteDecommissionRequest(String name, DecommissionAttribute decommissionAttribute) {
        this.name = name;
        this.decommissionAttribute = decommissionAttribute;
    }

    public DeleteDecommissionRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        decommissionAttribute = new DecommissionAttribute(in);
    }

    /**
     * Sets the decommission attribute name for decommission request
     *
     * @param name of the decommission attribute
     * @return the current object
     */
    public DeleteDecommissionRequest setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @return Returns the name of the decommission attribute
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets decommission attribute for decommission request
     *
     * @param decommissionAttribute values that needs to be decommissioned
     * @return the current object
     */
    public DeleteDecommissionRequest setDecommissionAttribute(DecommissionAttribute decommissionAttribute) {
        this.decommissionAttribute = decommissionAttribute;
        return this;
    }

    public DeleteDecommissionRequest setDecommissionAttribute(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return setDecommissionAttribute(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public DeleteDecommissionRequest setDecommissionAttribute(BytesReference source, XContentType contentType) {
        try (
                XContentParser parser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        source,
                        contentType
                )
        ) {
            XContentParser.Token token;
            if ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    String value;
                    token = parser.nextToken();
                    if (token == XContentParser.Token.VALUE_STRING) {
                        value = parser.text();
                    } else {
                        throw new OpenSearchParseException("failed to parse attribute [{}], expected string for attribute value", fieldName);
                    }
                    decommissionAttribute = new DecommissionAttribute(fieldName, value);
                } else {
                    throw new OpenSearchParseException("failed to parse attribute type, unexpected type");
                }
            }
            return this;
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse decommission attribute", e);
        }
    }

    /**
     * @return Returns the decommission attribute values
     */
    public DecommissionAttribute getDecommissionAttribute() {
        return this.decommissionAttribute;
    }

    @SuppressWarnings("unchecked")
    public DeleteDecommissionRequest source(Map<String, Object> source) {
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            setName(entry.getKey());
            if (!(entry.getValue() instanceof Map)) {
                throw new OpenSearchParseException("key [decommissionAttribute] must be an object");
            }
            setDecommissionAttribute((Map<String, Object>) entry.getValue());
        }
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        // TODO - Add request validators here
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        decommissionAttribute.writeTo(out);
    }

    @Override
    public String toString() {
        return "DeleteDecommissionRequest{" +
                "name='" + name + '\'' +
                ", decommissionAttribute=" + decommissionAttribute +
                '}';
    }
}

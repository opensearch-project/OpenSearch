/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.put;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.cluster.decommission.DecommissionedAttribute;
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

public class PutDecommissionRequest extends AcknowledgedRequest<PutDecommissionRequest> {

    private String name;
    private DecommissionedAttribute decommissionedAttribute;
    // TODO - What all request params needed? dry_run, timeout, master_timeout, retry_failed?

    public PutDecommissionRequest() {
    }

    public PutDecommissionRequest(String name, DecommissionedAttribute decommissionedAttribute) {
        this.name = name;
        this.decommissionedAttribute = decommissionedAttribute;
    }

    public PutDecommissionRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        decommissionedAttribute = new DecommissionedAttribute(in);
    }

    /**
     * Sets the decommission attribute name for decommission request
     *
     * @param name of the decommission attribute
     * @return the current object
     */
    public PutDecommissionRequest setName(String name) {
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
     * @param decommissionedAttribute values that needs to be decommissioned
     * @return the current object
     */
    public PutDecommissionRequest setDecommissionAttribute(DecommissionedAttribute decommissionedAttribute) {
        this.decommissionedAttribute = decommissionedAttribute;
        return this;
    }

    public PutDecommissionRequest setDecommissionAttribute(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(source);
            return setDecommissionAttribute(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public PutDecommissionRequest setDecommissionAttribute(BytesReference source, XContentType contentType) {
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                source,
                contentType
            )
        ) {
            XContentParser.Token token;
            // move to the first alias
            parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    List<String> values = new ArrayList<>();
                    token = parser.nextToken();
                    if (token == XContentParser.Token.VALUE_STRING) {
                        values.add(parser.text());
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                values.add(parser.text());
                            } else {
                                parser.skipChildren();
                            }
                        }
                    } else throw new OpenSearchParseException("failed to parse attribute [{}], unknown type", fieldName);
                    decommissionedAttribute = new DecommissionedAttribute(fieldName, values);
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
    public DecommissionedAttribute getDecommissionAttribute() {
        return this.decommissionedAttribute;
    }

    @SuppressWarnings("unchecked")
    public PutDecommissionRequest source(Map<String, Object> source) {
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            setName(entry.getKey());
            if (!(entry.getValue() instanceof Map)) {
                throw new OpenSearchParseException("key [decommissionedAttribute] must be an object");
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
        decommissionedAttribute.writeTo(out);
    }

    @Override
    public String toString() {
        return "PutDecommissionRequest{" +
            "name='" + name + '\'' +
            ", decommissionedAttribute=" + decommissionedAttribute +
            '}';
    }
}

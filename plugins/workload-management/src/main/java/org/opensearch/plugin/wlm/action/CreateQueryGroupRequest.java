/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.QueryGroup.ResiliencyMode;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A request for create QueryGroup
 * User input schema:
 *  {
 *    "name": "analytics",
 *    "resiliency_mode": "enforced",
 *    "resourceLimits": {
 *           "cpu" : 0.4,
 *           "memory" : 0.2
 *      }
 *  }
 *
 * @opensearch.experimental
 */
public class CreateQueryGroupRequest extends ActionRequest implements Writeable.Reader<CreateQueryGroupRequest> {
    private String name;
    private String _id;
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Object> resourceLimits;
    private long updatedAtInMillis;

    /**
     * Default constructor for CreateQueryGroupRequest
     */
    public CreateQueryGroupRequest() {}

    /**
     * Constructor for CreateQueryGroupRequest
     * @param queryGroup - A {@link QueryGroup} object
     */
    public CreateQueryGroupRequest(QueryGroup queryGroup) {
        this.name = queryGroup.getName();
        this._id = queryGroup.get_id();
        this.resourceLimits = queryGroup.getResourceLimits();
        this.resiliencyMode = queryGroup.getResiliencyMode();
        this.updatedAtInMillis = queryGroup.getUpdatedAtInMillis();
    }

    /**
     * Constructor for CreateQueryGroupRequest
     * @param name - QueryGroup name for CreateQueryGroupRequest
     * @param _id - QueryGroup _id for CreateQueryGroupRequest
     * @param mode - QueryGroup mode for CreateQueryGroupRequest
     * @param resourceLimits - QueryGroup resourceLimits for CreateQueryGroupRequest
     * @param updatedAtInMillis - QueryGroup updated time in millis for CreateQueryGroupRequest
     */
    public CreateQueryGroupRequest(
        String name,
        String _id,
        ResiliencyMode mode,
        Map<ResourceType, Object> resourceLimits,
        long updatedAtInMillis
    ) {
        this.name = name;
        this._id = _id;
        this.resourceLimits = resourceLimits;
        this.resiliencyMode = mode;
        this.updatedAtInMillis = updatedAtInMillis;
    }

    /**
     * Constructor for CreateQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public CreateQueryGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        _id = in.readString();
        resiliencyMode = ResiliencyMode.fromName(in.readString());
        resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readGenericValue);
        updatedAtInMillis = in.readLong();
    }

    @Override
    public CreateQueryGroupRequest read(StreamInput in) throws IOException {
        return new CreateQueryGroupRequest(in);
    }

    /**
     * Generate a CreateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static CreateQueryGroupRequest fromXContent(XContentParser parser) throws IOException {

        while (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException("expected start object but got a " + parser.currentToken());
        }

        XContentParser.Token token;
        String fieldName = "";
        String name = null;
        ResiliencyMode mode = null;

        // Map to hold resources
        final Map<ResourceType, Object> resourceLimits = new HashMap<>();
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (fieldName.equals("name")) {
                    name = parser.text();
                } else if (fieldName.equals("resiliency_mode")) {
                    mode = ResiliencyMode.fromName(parser.text());
                } else {
                    throw new XContentParseException("unrecognised [field=" + fieldName + " in QueryGroup");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (!fieldName.equals("resourceLimits")) {
                    throw new XContentParseException("Invalid field passed. QueryGroup does not support " + fieldName + ".");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else {
                        resourceLimits.put(ResourceType.fromName(fieldName), parser.doubleValue());
                    }
                }
            }
        }
        return new CreateQueryGroupRequest(name, UUIDs.randomBase64UUID(), mode, resourceLimits, Instant.now().getMillis());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * name setter
     * @param name - name to be set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * resourceLimits getter
     */
    public Map<ResourceType, Object> getResourceLimits() {
        return resourceLimits;
    }

    /**
     * resourceLimits setter
     * @param resourceLimits - resourceLimit to be set
     */
    public void setResourceLimits(Map<ResourceType, Object> resourceLimits) {
        this.resourceLimits = resourceLimits;
    }

    /**
     * mode getter
     */
    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    /**
     * mode setter
     * @param resiliencyMode - mode to be set
     */
    public void setResiliencyMode(ResiliencyMode resiliencyMode) {
        this.resiliencyMode = resiliencyMode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(_id);
        out.writeString(resiliencyMode.getName());
        out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeGenericValue);
        out.writeLong(updatedAtInMillis);
    }

    /**
     * _id getter
     */
    public String get_id() {
        return _id;
    }

    /**
     * UUID setter
     * @param _id - _id to be set
     */
    public void set_id(String _id) {
        this._id = _id;
    }

    /**
     * updatedAtInMillis getter
     */
    public long getUpdatedAtInMillis() {
        return updatedAtInMillis;
    }

    /**
     * updatedAtInMillis setter
     * @param updatedAtInMillis - updatedAtInMillis to be set
     */
    public void setUpdatedAtInMillis(long updatedAtInMillis) {
        this.updatedAtInMillis = updatedAtInMillis;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.metadata.QueryGroup.ResiliencyMode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A request for update QueryGroup
 *
 * @opensearch.internal
 */
public class UpdateQueryGroupRequest extends ActionRequest implements Writeable.Reader<UpdateQueryGroupRequest> {
    String name;
    Map<ResourceType, Double> resourceLimits;
    private ResiliencyMode resiliencyMode;
    long updatedAtInMillis;

    /**
     * Default constructor for UpdateQueryGroupRequest
     */
    public UpdateQueryGroupRequest() {}

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param queryGroup - A {@link QueryGroup} object
     */
    public UpdateQueryGroupRequest(QueryGroup queryGroup) {
        this.name = queryGroup.getName();
        this.resiliencyMode = queryGroup.getResiliencyMode();
        this.resourceLimits = queryGroup.getResourceLimits();
        this.updatedAtInMillis = queryGroup.getUpdatedAtInMillis();
    }

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param name - QueryGroup name for UpdateQueryGroupRequest
     * @param resiliencyMode - QueryGroup mode for UpdateQueryGroupRequest
     * @param resourceLimits - QueryGroup resourceLimits for UpdateQueryGroupRequest
     * @param updatedAtInMillis - QueryGroup updated time in millis for UpdateQueryGroupRequest
     */
    public UpdateQueryGroupRequest(
        String name,
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        long updatedAtInMillis
    ) {
        this.name = name;
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
        this.updatedAtInMillis = updatedAtInMillis;
    }

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public UpdateQueryGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        if (in.readBoolean()) {
            resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble);
        } else {
            resourceLimits = new HashMap<>();
        }
        if (in.readBoolean()) {
            resiliencyMode = ResiliencyMode.fromName(in.readString());
        }
        updatedAtInMillis = in.readLong();
    }

    @Override
    public UpdateQueryGroupRequest read(StreamInput in) throws IOException {
        return new UpdateQueryGroupRequest(in);
    }

    /**
     * Generate a UpdateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the QueryGroup to be updated
     */
    public static UpdateQueryGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        while (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected start object but got a " + parser.currentToken());
        }

        XContentParser.Token token;
        String fieldName = "";
        ResiliencyMode mode = null;

        // Map to hold resources
        final Map<ResourceType, Double> resourceLimits = new HashMap<>();
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (fieldName.equals("resiliency_mode")) {
                    mode = ResiliencyMode.fromName(parser.text());
                } else {
                    throw new IllegalArgumentException("unrecognised [field=" + fieldName + " in QueryGroup");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (!fieldName.equals("resourceLimits")) {
                    throw new IllegalArgumentException(
                        "QueryGroup.resourceLimits is an object and expected token was { " + " but found " + token
                    );
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
        return new UpdateQueryGroupRequest(name, mode, resourceLimits, Instant.now().getMillis());
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
     * ResourceLimits getter
     */
    public Map<ResourceType, Double> getResourceLimits() {
        return resourceLimits;
    }

    /**
     * ResourceLimits setter
     * @param resourceLimits - ResourceLimit to be set
     */
    public void setResourceLimits(Map<ResourceType, Double> resourceLimits) {
        this.resourceLimits = resourceLimits;
    }

    /**
     * resiliencyMode getter
     */
    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    /**
     * resiliencyMode setter
     * @param resiliencyMode - mode to be set
     */
    public void setResiliencyMode(ResiliencyMode resiliencyMode) {
        this.resiliencyMode = resiliencyMode;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeGenericValue);
        }
        if (resiliencyMode == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(resiliencyMode.getName());
        }
        out.writeLong(updatedAtInMillis);
    }
}

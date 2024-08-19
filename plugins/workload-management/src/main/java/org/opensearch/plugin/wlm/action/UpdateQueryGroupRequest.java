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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A request for update QueryGroup
 *
 * @opensearch.experimental
 */
public class UpdateQueryGroupRequest extends ActionRequest {
    private final String name;
    private final Map<ResourceType, Double> resourceLimits;
    private final ResiliencyMode resiliencyMode;
    private final long updatedAtInMillis;

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
        } else {
            resiliencyMode = null;
        }
        updatedAtInMillis = in.readLong();
    }

    /**
     * Generate a UpdateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the QueryGroup to be updated
     */
    public static UpdateQueryGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        QueryGroup.Builder builder = QueryGroup.Builder.fromXContent(parser);
        return new UpdateQueryGroupRequest(name, builder.getResiliencyMode(), builder.getResourceLimits(), Instant.now().getMillis());
    }

    @Override
    public ActionRequestValidationException validate() {
        QueryGroup.validateName(name);
        if (resourceLimits != null) {
            QueryGroup.validateResourceLimits(resourceLimits);
        }
        assert QueryGroup.isValid(updatedAtInMillis);
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * ResourceLimits getter
     */
    public Map<ResourceType, Double> getResourceLimits() {
        return resourceLimits;
    }

    /**
     * resiliencyMode getter
     */
    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    /**
     * updatedAtInMillis getter
     */
    public long getUpdatedAtInMillis() {
        return updatedAtInMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeDouble);
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

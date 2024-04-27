/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.metadata.ResourceLimitGroup.ResourceLimit;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * A request for update Resource Limit Group
 *
 * @opensearch.internal
 */
public class UpdateResourceLimitGroupRequest extends ActionRequest implements Writeable.Reader<UpdateResourceLimitGroupRequest> {
    String name;
    List<ResourceLimit> resourceLimits;
    String enforcement;
    String updatedAt;

    /**
     * Default constructor for UpdateResourceLimitGroupRequest
     * @param name - name for Resource Limit Group
     */
    public UpdateResourceLimitGroupRequest(String name) {
        this.name = name;
    }

    /**
     * Constructor for UpdateResourceLimitGroupRequest
     * @param resourceLimitGroup - A {@link ResourceLimitGroup} object
     */
    public UpdateResourceLimitGroupRequest(ResourceLimitGroup resourceLimitGroup) {
        this.name = resourceLimitGroup.getName();
        this.resourceLimits = resourceLimitGroup.getResourceLimits();
        this.enforcement = resourceLimitGroup.getEnforcement();
        this.updatedAt = resourceLimitGroup.getUpdatedAt();
    }

    /**
     * Constructor for UpdateResourceLimitGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public UpdateResourceLimitGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        if (in.readBoolean()) {
            resourceLimits = in.readList(ResourceLimit::new);
        }
        enforcement = in.readOptionalString();
        updatedAt = in.readString();
    }

    @Override
    public UpdateResourceLimitGroupRequest read(StreamInput in) throws IOException {
        return new UpdateResourceLimitGroupRequest(in);
    }

    /**
     * Generate a UpdateResourceLimitGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static UpdateResourceLimitGroupRequest fromXContent(XContentParser parser) throws IOException {
        ResourceLimitGroup resourceLimitGroup = ResourceLimitGroup.fromXContentOptionalFields(parser);
        return new UpdateResourceLimitGroupRequest(resourceLimitGroup);
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
    public List<ResourceLimit> getResourceLimits() {
        return resourceLimits;
    }

    /**
     * ResourceLimits setter
     * @param resourceLimits - ResourceLimit to be set
     */
    public void setResourceLimits(List<ResourceLimit> resourceLimits) {
        this.resourceLimits = resourceLimits;
    }

    /**
     * Enforcement getter
     */
    public String getEnforcement() {
        return enforcement;
    }

    /**
     * Enforcement setter
     * @param enforcement - enforcement to be set
     */
    public void setEnforcement(String enforcement) {
        this.enforcement = enforcement;
    }

    /**
     * updatedAt getter
     */
    public String getUpdatedAt() {
        return updatedAt;
    }

    /**
     * updatedAt setter
     * @param updatedAt - updatedAt to be set
     */
    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(resourceLimits);
        }
        out.writeOptionalString(enforcement);
        out.writeString(updatedAt);
    }
}

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
    String existingName;
    String updatingName;
    List<ResourceLimit> resourceLimits;
    String enforcement;

    /**
     * Default constructor for UpdateResourceLimitGroupRequest
     * @param existingName - existing name for Resource Limit Group
     */
    public UpdateResourceLimitGroupRequest(String existingName) {
        this.existingName = existingName;
    }

    /**
     * Constructor for UpdateResourceLimitGroupRequest
     * @param resourceLimitGroup - A {@link ResourceLimitGroup} object
     */
    public UpdateResourceLimitGroupRequest(ResourceLimitGroup resourceLimitGroup) {
        this.updatingName = resourceLimitGroup.getName();
        this.resourceLimits = resourceLimitGroup.getResourceLimits();
        this.enforcement = resourceLimitGroup.getEnforcement();
    }

    /**
     * Constructor for UpdateResourceLimitGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public UpdateResourceLimitGroupRequest(StreamInput in) throws IOException {
        super(in);
        existingName = in.readOptionalString();
        updatingName = in.readOptionalString();
        if (in.readBoolean()) {
            resourceLimits = in.readList(ResourceLimit::new);
        }
        enforcement = in.readOptionalString();
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
     * existingName getter
     */
    public String getExistingName() {
        return existingName;
    }

    /**
     * existingName setter
     * @param existingName - name to be set
     */
    public void setExistingName(String existingName) {
        this.existingName = existingName;
    }

    /**
     * updatingName getter
     */
    public String getUpdatingName() {
        return updatingName;
    }

    /**
     * updatingName setter
     * @param updatingName - name to be set
     */
    public void setUpdatingName(String updatingName) {
        this.updatingName = updatingName;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(existingName);
        out.writeOptionalString(updatingName);
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(resourceLimits);
        }
        out.writeOptionalString(enforcement);
    }
}

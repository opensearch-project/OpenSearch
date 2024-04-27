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
 * A request for create Resource Limit Group
 *
 * @opensearch.internal
 */
public class CreateResourceLimitGroupRequest extends ActionRequest implements Writeable.Reader<CreateResourceLimitGroupRequest> {
    private String name;
    private String uuid;
    private List<ResourceLimit> resourceLimits;
    private String enforcement;
    private String createdAt;
    private String updatedAt;

    /**
     * Default constructor for CreateResourceLimitGroupRequest
     */
    public CreateResourceLimitGroupRequest() {}

    /**
     * Constructor for CreateResourceLimitGroupRequest
     * @param resourceLimitGroup - A {@link ResourceLimitGroup} object
     */
    public CreateResourceLimitGroupRequest(ResourceLimitGroup resourceLimitGroup) {
        this.name = resourceLimitGroup.getName();
        this.uuid = resourceLimitGroup.getUUID();
        this.resourceLimits = resourceLimitGroup.getResourceLimits();
        this.enforcement = resourceLimitGroup.getEnforcement();
        this.createdAt = resourceLimitGroup.getCreatedAt();
        this.updatedAt = resourceLimitGroup.getUpdatedAt();
    }

    /**
     * Constructor for CreateResourceLimitGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public CreateResourceLimitGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        uuid = in.readString();
        resourceLimits = in.readList(ResourceLimit::new);
        enforcement = in.readString();
        createdAt = in.readString();
        updatedAt = in.readString();
    }

    @Override
    public CreateResourceLimitGroupRequest read(StreamInput in) throws IOException {
        return new CreateResourceLimitGroupRequest(in);
    }

    /**
     * Generate a CreateResourceLimitGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static CreateResourceLimitGroupRequest fromXContent(XContentParser parser) throws IOException {
        ResourceLimitGroup resourceLimitGroup = ResourceLimitGroup.fromXContent(parser);
        return new CreateResourceLimitGroupRequest(resourceLimitGroup);
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
    public List<ResourceLimit> getResourceLimits() {
        return resourceLimits;
    }

    /**
     * resourceLimits setter
     * @param resourceLimits - resourceLimit to be set
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
        ResourceLimitGroup.writeToOutputStream(out, name, uuid, resourceLimits, enforcement, createdAt, updatedAt);
    }

    /**
     * UUID getter
     */
    public String getUUID() {
        return uuid;
    }

    /**
     * UUID setter
     * @param uuid - uuid to be set
     */
    public void setUUID(String uuid) {
        this.uuid = uuid;
    }

    /**
     * createdAt getter
     */
    public String getCreatedAt() {
        return createdAt;
    }

    /**
     * createdAt setter
     * @param createdAt - createdAt to be set
     */
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
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
}

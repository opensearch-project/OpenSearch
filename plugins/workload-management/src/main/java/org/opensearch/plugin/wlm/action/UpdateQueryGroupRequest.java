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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.ChangeableQueryGroup;
import org.opensearch.wlm.ChangeableQueryGroup.ResiliencyMode;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.Map;

/**
 * A request for update QueryGroup
 *
 * @opensearch.experimental
 */
public class UpdateQueryGroupRequest extends ActionRequest {
    private final String name;
    private final ChangeableQueryGroup changeableQueryGroup;

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param name - QueryGroup name for UpdateQueryGroupRequest
     * @param changeableQueryGroup - ChangeableQueryGroup for UpdateQueryGroupRequest
     */
    public UpdateQueryGroupRequest(String name, ChangeableQueryGroup changeableQueryGroup) {
        this.name = name;
        this.changeableQueryGroup = changeableQueryGroup;
    }

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public UpdateQueryGroupRequest(StreamInput in) throws IOException {
        this(in.readString(), new ChangeableQueryGroup(in));
    }

    /**
     * Generate a UpdateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the QueryGroup to be updated
     */
    public static UpdateQueryGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        QueryGroup.Builder builder = QueryGroup.Builder.fromXContent(parser);
        return new UpdateQueryGroupRequest(name, builder.getChangeableQueryGroup());
    }

    @Override
    public ActionRequestValidationException validate() {
        QueryGroup.validateName(name);
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * resourceLimits getter
     */
    public Map<ResourceType, Double> getResourceLimits() {
        return getChangeableQueryGroup().getResourceLimits();
    }

    /**
     * resiliencyMode getter
     */
    public ResiliencyMode getResiliencyMode() {
        return getChangeableQueryGroup().getResiliencyMode();
    }

    /**
     * changeableQueryGroup getter
     */
    public ChangeableQueryGroup getChangeableQueryGroup() {
        return changeableQueryGroup;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        changeableQueryGroup.writeTo(out);
    }
}

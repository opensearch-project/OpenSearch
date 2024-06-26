/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.qg;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for the create API for QueryGroup
 *
 * @opensearch.internal
 */
public class CreateQueryGroupResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final QueryGroup queryGroup;
    private RestStatus restStatus;

    /**
     * Constructor for CreateQueryGroupResponse
     */
    public CreateQueryGroupResponse() {
        this.queryGroup = null;
    }

    /**
     * Constructor for CreateQueryGroupResponse
     * @param queryGroup - The resource limit group to be created
     */
    public CreateQueryGroupResponse(final QueryGroup queryGroup) {
        this.queryGroup = queryGroup;
    }

    /**
     * Constructor for CreateQueryGroupResponse
     * @param in - A {@link StreamInput} object
     */
    public CreateQueryGroupResponse(StreamInput in) throws IOException {
        queryGroup = new QueryGroup(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        queryGroup.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", queryGroup.getName());
        builder.field("mode", queryGroup.getMode().getName());
        builder.field("updatedAt", queryGroup.getUpdatedAtInMillis());
        builder.mapContents(queryGroup.getResourceLimits());
        builder.endObject();
        return builder;
    }

    /**
     * queryGroup getter
     */
    public QueryGroup getQueryGroup() {
        return queryGroup;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }

    /**
     * restStatus setter
     * @param restStatus - A {@link RestStatus} object
     */
    public void setRestStatus(RestStatus restStatus) {
        this.restStatus = restStatus;
    }
}

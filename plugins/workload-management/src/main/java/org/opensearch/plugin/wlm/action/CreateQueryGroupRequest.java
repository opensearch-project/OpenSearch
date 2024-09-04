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
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * A request for create QueryGroup
 * User input schema:
 *  {
 *    "name": "analytics",
 *    "resiliency_mode": "enforced",
 *    "resource_limits": {
 *           "cpu" : 0.4,
 *           "memory" : 0.2
 *      }
 *  }
 *
 * @opensearch.experimental
 */
public class CreateQueryGroupRequest extends ActionRequest {
    private final QueryGroup queryGroup;

    /**
     * Constructor for CreateQueryGroupRequest
     * @param queryGroup - A {@link QueryGroup} object
     */
    CreateQueryGroupRequest(QueryGroup queryGroup) {
        this.queryGroup = queryGroup;
    }

    /**
     * Constructor for CreateQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    CreateQueryGroupRequest(StreamInput in) throws IOException {
        super(in);
        queryGroup = new QueryGroup(in);
    }

    /**
     * Generate a CreateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     */
    public static CreateQueryGroupRequest fromXContent(XContentParser parser) throws IOException {
        QueryGroup.Builder builder = QueryGroup.Builder.fromXContent(parser);
        return new CreateQueryGroupRequest(builder._id(UUIDs.randomBase64UUID()).updatedAt(Instant.now().getMillis()).build());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        queryGroup.writeTo(out);
    }

    /**
     * QueryGroup getter
     */
    public QueryGroup getQueryGroup() {
        return queryGroup;
    }
}

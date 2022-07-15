/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.management.decommission;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A response to {@link PutDecommissionRequest} indicating that requested nodes has been decommissioned
 *
 * @opensearch.internal
 */
public class PutDecommissionResponse extends ActionResponse implements ToXContentObject {

    public PutDecommissionResponse() {}

    public PutDecommissionResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return builder;
    }
}

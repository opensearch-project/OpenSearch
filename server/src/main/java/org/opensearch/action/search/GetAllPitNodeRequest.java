/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Inner node get all pits request
 */
public class GetAllPitNodeRequest extends BaseNodeRequest {
    GetAllPitNodesRequest request;

    @Inject
    public GetAllPitNodeRequest(GetAllPitNodesRequest request) {
        this.request = request;
    }

    public GetAllPitNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new GetAllPitNodesRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}

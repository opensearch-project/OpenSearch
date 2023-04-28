/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.filecache.clear;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request object for clearing filecache on nodes
 *
 * @opensearch.internal
 */
public class ClearNodesFileCacheRequest extends BaseNodesRequest<ClearNodesFileCacheRequest> {

    public ClearNodesFileCacheRequest() {
        super((String[]) null);
    }

    public ClearNodesFileCacheRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}

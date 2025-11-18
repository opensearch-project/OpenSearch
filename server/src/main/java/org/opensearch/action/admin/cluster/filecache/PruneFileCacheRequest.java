/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for pruning remote file cache across multiple nodes.
 * Supports node targeting for efficient cache management.
 *
 * @opensearch.internal
 */
public class PruneFileCacheRequest extends BaseNodesRequest<PruneFileCacheRequest> {

    public PruneFileCacheRequest() {
        super((String[]) null);
    }

    public PruneFileCacheRequest(String... nodesIds) {
        super(nodesIds);
    }

    public PruneFileCacheRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

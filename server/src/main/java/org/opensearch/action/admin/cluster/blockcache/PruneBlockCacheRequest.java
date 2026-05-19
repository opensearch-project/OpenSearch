/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for pruning all registered block caches across warm nodes.
 *
 * @opensearch.internal
 */
public class PruneBlockCacheRequest extends BaseNodesRequest<PruneBlockCacheRequest> {

    public PruneBlockCacheRequest() {
        super((String[]) null);
    }

    public PruneBlockCacheRequest(String... nodeIds) {
        super(nodeIds);
    }

    public PruneBlockCacheRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PruneBlockCacheRequest)) return false;
        PruneBlockCacheRequest other = (PruneBlockCacheRequest) o;
        String[] thisIds = nodesIds() == null ? new String[0] : nodesIds();
        String[] otherIds = other.nodesIds() == null ? new String[0] : other.nodesIds();
        return java.util.Arrays.equals(thisIds, otherIds) && java.util.Objects.equals(timeout(), other.timeout());
    }

    @Override
    public int hashCode() {
        String[] ids = nodesIds() == null ? new String[0] : nodesIds();
        return java.util.Objects.hash(java.util.Arrays.hashCode(ids), timeout());
    }
}

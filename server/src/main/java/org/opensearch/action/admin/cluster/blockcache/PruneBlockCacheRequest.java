/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for pruning a named block cache across warm nodes.
 *
 * @opensearch.internal
 */
public class PruneBlockCacheRequest extends BaseNodesRequest<PruneBlockCacheRequest> {

    private final String cacheName;

    public PruneBlockCacheRequest(String cacheName) {
        super((String[]) null);
        this.cacheName = cacheName;
    }

    public PruneBlockCacheRequest(String cacheName, String... nodeIds) {
        super(nodeIds);
        this.cacheName = cacheName;
    }

    public PruneBlockCacheRequest(StreamInput in) throws IOException {
        super(in);
        this.cacheName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cacheName);
    }

    public String getCacheName() {
        return cacheName;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

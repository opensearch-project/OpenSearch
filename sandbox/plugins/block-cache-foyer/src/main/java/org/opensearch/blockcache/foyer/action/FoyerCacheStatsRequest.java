/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer.action;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for {@code GET /_nodes/foyer_cache/stats}.
 *
 * <p>Targets all nodes by default ({@code nodeIds = new String[0]});
 * the standard {@code /_nodes/{nodeId}/foyer_cache/stats} URI pattern
 * allows targeting specific nodes.
 *
 * @opensearch.internal
 */
public class FoyerCacheStatsRequest extends BaseNodesRequest<FoyerCacheStatsRequest> {

    /** Targets all nodes. */
    public FoyerCacheStatsRequest() {
        super((String[]) null);
    }

    /** Targets specific nodes by ID or name. */
    public FoyerCacheStatsRequest(String... nodeIds) {
        super(nodeIds);
    }

    public FoyerCacheStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}

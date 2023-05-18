/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.unit.TimeValue;

/**
 * Builder for RemoteStoreStatsRequest
 *
 * @opensearch.internal
 */
public class RemoteStoreStatsRequestBuilder extends BroadcastOperationRequestBuilder<
    RemoteStoreStatsRequest,
    RemoteStoreStatsResponse,
    RemoteStoreStatsRequestBuilder> {

    public RemoteStoreStatsRequestBuilder(OpenSearchClient client, RemoteStoreStatsAction action) {
        super(client, action, new RemoteStoreStatsRequest());
    }

    /**
     * Sets timeout of request.
     */
    public final RemoteStoreStatsRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Sets shards preference of request.
     */
    public final RemoteStoreStatsRequestBuilder setShards(String... shards) {
        request.shards(shards);
        return this;
    }

    /**
     * Sets local shards preference of request.
     */
    public final RemoteStoreStatsRequestBuilder setLocal(boolean local) {
        request.local(local);
        return this;
    }
}

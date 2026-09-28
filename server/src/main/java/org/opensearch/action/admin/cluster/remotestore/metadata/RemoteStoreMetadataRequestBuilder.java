/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Builder for RemoteStoreMetadataRequest
 *
 * @opensearch.api
 */
@ExperimentalApi
public class RemoteStoreMetadataRequestBuilder extends BroadcastOperationRequestBuilder<
    RemoteStoreMetadataRequest,
    RemoteStoreMetadataResponse,
    RemoteStoreMetadataRequestBuilder> {

    public RemoteStoreMetadataRequestBuilder(OpenSearchClient client, RemoteStoreMetadataAction action) {
        super(client, action, new RemoteStoreMetadataRequest());
    }

    /**
     * Sets timeout of request.
     */
    public final RemoteStoreMetadataRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * Sets shards preference of request.
     */
    public final RemoteStoreMetadataRequestBuilder setShards(String... shards) {
        request.shards(shards);
        return this;
    }
}

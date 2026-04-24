/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to publish shard data to the catalog registered on each node. Carries only the
 * index name — there is one catalog destination per node, configured via node settings at
 * startup.
 *
 * @opensearch.experimental
 */
public class PublishShardRequest extends BroadcastRequest<PublishShardRequest> {

    public PublishShardRequest(String indexName) {
        super(indexName);
    }

    public PublishShardRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public String toString() {
        return "PublishShardRequest{indices=" + String.join(",", indices()) + "}";
    }
}

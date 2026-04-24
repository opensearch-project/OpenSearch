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
 * Request to publish shard data to an external catalog. Carries the index name and
 * catalog repository name so the shard-level handler can build the
 * {@code RemoteSegmentStoreDirectory} and call {@code MetadataClient.publish()}.
 *
 * @opensearch.experimental
 */
public class PublishShardRequest extends BroadcastRequest<PublishShardRequest> {

    private final String catalogRepoName;

    public PublishShardRequest(String indexName, String catalogRepoName) {
        super(indexName);
        this.catalogRepoName = catalogRepoName;
    }

    public PublishShardRequest(StreamInput in) throws IOException {
        super(in);
        this.catalogRepoName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(catalogRepoName);
    }

    public String getCatalogRepoName() {
        return catalogRepoName;
    }

    @Override
    public String toString() {
        return "PublishShardRequest{indices=" + String.join(",", indices()) + ", catalogRepo=" + catalogRepoName + "}";
    }
}

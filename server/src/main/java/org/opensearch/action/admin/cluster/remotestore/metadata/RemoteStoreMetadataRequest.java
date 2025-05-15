/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request object for fetching remote store metadata of shards for a given index.
 *
 * @opensearch.internal
 */
@PublicApi(since = "3.0.0")
public class RemoteStoreMetadataRequest extends BroadcastRequest<RemoteStoreMetadataRequest> {
    private String[] shards;
    private boolean local = false;

    public RemoteStoreMetadataRequest() {
        super((String[]) null);
        shards = new String[0];
    }

    public RemoteStoreMetadataRequest(StreamInput in) throws IOException {
        super(in);
        shards = in.readStringArray();
        local = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(shards);
        out.writeBoolean(local);
    }

    public RemoteStoreMetadataRequest shards(String... shards) {
        this.shards = shards;
        return this;
    }

    public String[] shards() {
        return this.shards;
    }

    public void local(boolean local) {
        this.local = local;
    }

    public boolean local() {
        return local;
    }
}

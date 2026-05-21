/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Per-shard result containing the shard routing and catalog snapshot XContent bytes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CatalogSnapshotShardResult implements Writeable {

    private final ShardRouting shardRouting;
    private final BytesReference snapshotBytes;

    public CatalogSnapshotShardResult(ShardRouting shardRouting, BytesReference snapshotBytes) {
        this.shardRouting = shardRouting;
        this.snapshotBytes = snapshotBytes;
    }

    public CatalogSnapshotShardResult(StreamInput in) throws IOException {
        this.shardRouting = new ShardRouting(in);
        this.snapshotBytes = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeBytesReference(snapshotBytes);
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public BytesReference getSnapshotBytes() {
        return snapshotBytes;
    }
}

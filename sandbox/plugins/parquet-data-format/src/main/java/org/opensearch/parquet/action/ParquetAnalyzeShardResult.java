/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Per-shard result containing the shard routing and parquet analyze XContent bytes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetAnalyzeShardResult implements Writeable {

    private final ShardRouting shardRouting;
    private final BytesReference analyzeBytes;

    public ParquetAnalyzeShardResult(ShardRouting shardRouting, BytesReference analyzeBytes) {
        this.shardRouting = shardRouting;
        this.analyzeBytes = analyzeBytes;
    }

    public ParquetAnalyzeShardResult(StreamInput in) throws IOException {
        this.shardRouting = new ShardRouting(in);
        this.analyzeBytes = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeBytesReference(analyzeBytes);
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public BytesReference getAnalyzeBytes() {
        return analyzeBytes;
    }
}

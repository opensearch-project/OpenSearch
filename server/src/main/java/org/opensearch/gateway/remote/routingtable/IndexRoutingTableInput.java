/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Iterator;

public class IndexRoutingTableInput {

    private final IndexRoutingTableHeader indexRoutingTableHeader;
    private final Iterator<IndexShardRoutingTable> shardIter;
    private int shardCount;

    public IndexRoutingTableInput(IndexRoutingTable indexRoutingTable) {
        this.shardIter = indexRoutingTable.iterator();
        this.indexRoutingTableHeader = new IndexRoutingTableHeader(indexRoutingTable.getIndex().getName());
        this.shardCount = indexRoutingTable.shards().size();
    }

    public BytesReference write() throws IOException {
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(bytesStreamOutput);
        indexRoutingTableHeader.write(out);
        out.writeVInt(shardCount);
        while (shardIter.hasNext()) {
            IndexShardRoutingTable next = shardIter.next();
            IndexShardRoutingTable.Builder.writeTo(next, out);
        }
        out.writeLong(out.getChecksum());
        out.flush();
        return bytesStreamOutput.bytes();
    }

}

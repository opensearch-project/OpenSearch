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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.index.Index;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class RemoteIndexRoutingTableObject {

    private final IndexRoutingTableHeader indexRoutingTableHeader;
    private final Iterator<IndexShardRoutingTable> shardIter;
    private final int shardCount;

    public RemoteIndexRoutingTableObject(IndexRoutingTable indexRoutingTable) {
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

    public static IndexRoutingTable read(InputStream inputStream, Index index) throws IOException {
        try {
            try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(new InputStreamStreamInput(inputStream), "assertion")) {
                // Read the Table Header first and confirm the index
                IndexRoutingTableHeader indexRoutingTableHeader = IndexRoutingTableHeader.read(in);
                assert indexRoutingTableHeader.getIndexName().equals(index.getName());

                int numberOfShardRouting = in.readVInt();
                IndexRoutingTable.Builder indicesRoutingTable = IndexRoutingTable.builder(index);
                for (int idx = 0; idx < numberOfShardRouting; idx++) {
                    IndexShardRoutingTable indexShardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
                    indicesRoutingTable.addIndexShard(indexShardRoutingTable);
                }
                verifyCheckSum(in);
                return indicesRoutingTable.build();
            }
        } catch (EOFException e) {
            throw new IOException("Indices Routing table is corrupted", e);
        }
    }

    public static void verifyCheckSum(BufferedChecksumStreamInput in) throws IOException {
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readLong();
        if (readChecksum != expectedChecksum) {
            throw new IOException(
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(readChecksum)
            );
        }
    }
}

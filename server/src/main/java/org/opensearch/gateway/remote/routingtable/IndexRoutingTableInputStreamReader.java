/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class IndexRoutingTableInputStreamReader {

    private final StreamInput streamInput;

    private static final Logger logger = LogManager.getLogger(IndexRoutingTableInputStreamReader.class);

    public IndexRoutingTableInputStreamReader(InputStream inputStream) throws IOException {
        streamInput = new InputStreamStreamInput(inputStream);
    }

    public IndexRoutingTable readIndexRoutingTable(Index index) throws IOException {
        try {
            try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(streamInput, "assertion")) {
                // Read the Table Header first and confirm the index
                IndexRoutingTableHeader indexRoutingTableHeader = IndexRoutingTableHeader.read(in);
                assert indexRoutingTableHeader.getIndexName().equals(index.getName());

                int numberOfShardRouting = in.readVInt();
                logger.debug("Number of Index Routing Table {}", numberOfShardRouting);
                IndexRoutingTable.Builder indicesRoutingTable = IndexRoutingTable.builder(index);
                for (int idx = 0; idx < numberOfShardRouting; idx++) {
                    IndexShardRoutingTable indexShardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
                    logger.debug("Index Shard Routing Table reading {}", indexShardRoutingTable);
                    indicesRoutingTable.addIndexShard(indexShardRoutingTable);

                }
                verifyCheckSum(in);
                return indicesRoutingTable.build();
            }
        } catch (EOFException e) {
            throw new IOException("Indices Routing table is corrupted", e);
        }
    }

    private void verifyCheckSum(BufferedChecksumStreamInput in) throws IOException {
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

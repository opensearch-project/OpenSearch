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
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexRoutingTableInputStreamReader {

    private final StreamInput streamInput;

    private static final Logger logger = LogManager.getLogger(IndexRoutingTableInputStreamReader.class);

    public IndexRoutingTableInputStreamReader(InputStream inputStream) throws IOException {
        this.streamInput = new InputStreamStreamInput(inputStream);
    }

    public Map<String, IndexShardRoutingTable> read() throws IOException {
        try {
            try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(streamInput, "assertion")) {
                // Read the Table Header first
                IndexRoutingTableHeader.read(in);
                int shards = in.readVInt();
                logger.info("Number of Index Routing Table {}", shards);
                Map<String, IndexShardRoutingTable> indicesRouting = new HashMap<String, IndexShardRoutingTable>(Collections.EMPTY_MAP);
                for(int i=0; i<shards; i++)
                {
                    IndexShardRoutingTable indexShardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
                    logger.info("Index Shard Routing Table reading {}", indexShardRoutingTable);
                    indicesRouting.put(indexShardRoutingTable.getShardId().getIndexName(), indexShardRoutingTable);

                }
                verifyCheckSum(in);
                // Return indices Routing table
                return indicesRouting;
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.block;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.translog.BufferedChecksumStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.cluster.block.ClusterBlockTests.randomClusterBlock;

public class ClusterBlocksTests extends OpenSearchTestCase {

    public void testWriteVerifiableTo() throws Exception {
        ClusterBlock clusterBlock1 = randomClusterBlock();
        ClusterBlock clusterBlock2 = randomClusterBlock();
        ClusterBlock clusterBlock3 = randomClusterBlock();

        ClusterBlocks clusterBlocks = ClusterBlocks.builder()
            .addGlobalBlock(clusterBlock1)
            .addGlobalBlock(clusterBlock2)
            .addGlobalBlock(clusterBlock3)
            .addIndexBlock("index-1", clusterBlock1)
            .addIndexBlock("index-2", clusterBlock2)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        clusterBlocks.writeVerifiableTo(checksumOut);
        StreamInput in = out.bytes().streamInput();
        ClusterBlocks result = ClusterBlocks.readFrom(in);

        assertEquals(clusterBlocks.global().size(), result.global().size());
        assertEquals(clusterBlocks.global(), result.global());
        assertEquals(clusterBlocks.indices().size(), result.indices().size());
        assertEquals(clusterBlocks.indices(), result.indices());

        ClusterBlocks clusterBlocks2 = ClusterBlocks.builder()
            .addGlobalBlock(clusterBlock3)
            .addGlobalBlock(clusterBlock1)
            .addGlobalBlock(clusterBlock2)
            .addIndexBlock("index-2", clusterBlock2)
            .addIndexBlock("index-1", clusterBlock1)
            .build();
        BytesStreamOutput out2 = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut2 = new BufferedChecksumStreamOutput(out2);
        clusterBlocks2.writeVerifiableTo(checksumOut2);
        assertEquals(checksumOut.getChecksum(), checksumOut2.getChecksum());
    }
}

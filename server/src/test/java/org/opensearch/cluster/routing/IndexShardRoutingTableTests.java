/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class IndexShardRoutingTableTests extends OpenSearchTestCase {
    public void testEqualsAttributesKey() {
        List<String> attr1 = Arrays.asList("a");
        List<String> attr2 = Arrays.asList("b");
        IndexShardRoutingTable.AttributesKey attributesKey1 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey2 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey3 = new IndexShardRoutingTable.AttributesKey(attr2);
        String s = "Some random other object";
        assertEquals(attributesKey1, attributesKey1);
        assertEquals(attributesKey1, attributesKey2);
        assertNotEquals(attributesKey1, null);
        assertNotEquals(attributesKey1, s);
        assertNotEquals(attributesKey1, attributesKey3);
    }

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table3 = new IndexShardRoutingTable(shardId2, new ArrayList<>());
        String s = "Some other random object";
        assertEquals(table1, table1);
        assertEquals(table1, table2);
        assertNotEquals(table1, null);
        assertNotEquals(table1, s);
        assertNotEquals(table1, table3);

        ShardRouting primary = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED);
        IndexShardRoutingTable table4 = new IndexShardRoutingTable(shardId, Arrays.asList(primary, replica));
        IndexShardRoutingTable table5 = new IndexShardRoutingTable(shardId, Arrays.asList(replica, primary));
        assertEquals(table4, table5);
    }

    public void testWriteVerifiableTo() throws IOException {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardRouting shard1 = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        ShardRouting shard2 = TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED);
        ShardRouting shard3 = TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED);

        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, Arrays.asList(shard1, shard2, shard3));
        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        IndexShardRoutingTable.Builder.writeVerifiableTo(table1, checksumOut);

        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, Arrays.asList(shard3, shard1, shard2));
        BytesStreamOutput out2 = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut2 = new BufferedChecksumStreamOutput(out2);
        IndexShardRoutingTable.Builder.writeVerifiableTo(table2, checksumOut2);
        assertEquals(checksumOut.getChecksum(), checksumOut2.getChecksum());
    }

    public void testShardsMatchingPredicate() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        ShardRouting primary = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED);
        ShardRouting unassignedReplica = ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        ShardRouting relocatingReplica1 = TestShardRouting.newShardRouting(
            shardId,
            "node-3",
            "node-4",
            false,
            ShardRoutingState.RELOCATING
        );
        ShardRouting relocatingReplica2 = TestShardRouting.newShardRouting(
            shardId,
            "node-4",
            "node-5",
            false,
            ShardRoutingState.RELOCATING
        );

        IndexShardRoutingTable table = new IndexShardRoutingTable(
            shardId,
            Arrays.asList(primary, replica, unassignedReplica, relocatingReplica1, relocatingReplica2)
        );
        assertEquals(List.of(primary), table.shardsMatchingPredicate(ShardRouting::primary));
        assertEquals(
            List.of(replica, unassignedReplica, relocatingReplica1, relocatingReplica2),
            table.shardsMatchingPredicate(shardRouting -> !shardRouting.primary())
        );
        assertEquals(
            List.of(unassignedReplica),
            table.shardsMatchingPredicate(shardRouting -> !shardRouting.primary() && shardRouting.unassigned())
        );
        assertEquals(
            Arrays.asList(relocatingReplica1, relocatingReplica2),
            table.shardsMatchingPredicate(shardRouting -> !shardRouting.primary() && shardRouting.relocating())
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteIndexRoutingTableObjectTests extends OpenSearchTestCase {

    public void testRoutingTableInput() {
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexShardRoutingTables -> {
            try {
                RemoteIndexRoutingTableObject indexRouting = new RemoteIndexRoutingTableObject(indexShardRoutingTables);
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                indexRouting.writeTo(streamOutput);
                RemoteIndexRoutingTableObject remoteIndexRoutingTable = new RemoteIndexRoutingTableObject(
                    streamOutput.bytes().streamInput(),
                    metadata.index("test").getIndex()
                );
                IndexRoutingTable indexRoutingTable = remoteIndexRoutingTable.getIndexRoutingTable();
                assertEquals(numberOfShards, indexRoutingTable.getShards().size());
                assertEquals(indexRoutingTable.getIndex(), metadata.index("test").getIndex());
                assertEquals(
                    indexRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size(),
                    numberOfShards * (1 + numberOfReplicas)
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testRoutingTableInputStreamWithInvalidIndex() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("invalid-index").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        AtomicInteger assertionError = new AtomicInteger();
        initialRoutingTable.getIndicesRouting().values().forEach(indexShardRoutingTables -> {
            try {
                RemoteIndexRoutingTableObject indexRouting = new RemoteIndexRoutingTableObject(indexShardRoutingTables);
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                indexRouting.writeTo(streamOutput);
                RemoteIndexRoutingTableObject remoteIndexRoutingTable = new RemoteIndexRoutingTableObject(
                    streamOutput.bytes().streamInput(),
                    metadata.index("invalid-index").getIndex()
                );
            } catch (AssertionError e) {
                assertionError.getAndIncrement();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, assertionError.get());
    }

}

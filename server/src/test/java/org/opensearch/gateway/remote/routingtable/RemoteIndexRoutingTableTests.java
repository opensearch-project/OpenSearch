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

public class RemoteIndexRoutingTableTests extends OpenSearchTestCase {

    public void testRoutingTableInput() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexShardRoutingTables -> {
            RemoteIndexRoutingTable indexRouting = new RemoteIndexRoutingTable(indexShardRoutingTables);
            try (BytesStreamOutput streamOutput = new BytesStreamOutput();) {
                indexRouting.writeTo(streamOutput);
                RemoteIndexRoutingTable remoteIndexRoutingTable = new RemoteIndexRoutingTable(
                    streamOutput.bytes().streamInput(),
                    metadata.index(indexName).getIndex()
                );
                IndexRoutingTable indexRoutingTable = remoteIndexRoutingTable.getIndexRoutingTable();
                assertEquals(numberOfShards, indexRoutingTable.getShards().size());
                assertEquals(metadata.index(indexName).getIndex(), indexRoutingTable.getIndex());
                assertEquals(
                    numberOfShards * (1 + numberOfReplicas),
                    indexRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size()
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
            RemoteIndexRoutingTable indexRouting = new RemoteIndexRoutingTable(indexShardRoutingTables);
            try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                indexRouting.writeTo(streamOutput);
                RemoteIndexRoutingTable remoteIndexRoutingTable = new RemoteIndexRoutingTable(
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

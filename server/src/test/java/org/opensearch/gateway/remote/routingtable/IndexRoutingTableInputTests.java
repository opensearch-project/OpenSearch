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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexRoutingTableInputTests extends OpenSearchTestCase {

    public void testRoutingTableInput() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexShardRoutingTables -> {
            try {
                IndexRoutingTableInput indexRouting = new IndexRoutingTableInput(indexShardRoutingTables);

                IndexRoutingTableInputStreamReader reader = new IndexRoutingTableInputStreamReader(indexRouting.write().streamInput());
                IndexRoutingTable indexRoutingTable = reader.readIndexRoutingTable(metadata.index("test").getIndex());

                assertEquals(1, indexRoutingTable.getShards().size());
                assertEquals(indexRoutingTable.getIndex(), metadata.index("test").getIndex());
                assertEquals(indexRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size(), 2);
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
                IndexRoutingTableInput indexRouting = new IndexRoutingTableInput(indexShardRoutingTables);

                IndexRoutingTableInputStreamReader reader = new IndexRoutingTableInputStreamReader(indexRouting.write().streamInput());
                reader.readIndexRoutingTable(metadata.index("invalid-index").getIndex());

            } catch (AssertionError e) {
                assertionError.getAndIncrement();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, assertionError.get());
    }

}

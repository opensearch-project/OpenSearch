/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;

public class IndexBasedPaginationStrategyTests extends OpenSearchTestCase {

    public void testRetrieveAllIndicesInAscendingOrder() {
        ClusterState clusterState = getRandomClusterState();
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(null, 1, "ascending", clusterState);

        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getNextToken());

        paginationStrategy = new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationStrategy.getNextToken(),
            1,
            "ascending",
            clusterState
        );
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getNextToken());

        paginationStrategy = new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationStrategy.getNextToken(),
            1,
            "ascending",
            clusterState
        );
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getNextToken());

        paginationStrategy = new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationStrategy.getNextToken(),
            1,
            "ascending",
            clusterState
        );
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getNextToken());
    }

    public void testRetrieveAllIndicesInDescendingOrder() {
        ClusterState clusterState = getRandomClusterState();
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(null, 1, "descending", clusterState);

        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getNextToken());

        paginationStrategy = new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationStrategy.getNextToken(),
            1,
            "descending",
            clusterState
        );
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getNextToken());

        paginationStrategy = new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationStrategy.getNextToken(),
            1,
            "descending",
            clusterState
        );
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getNextToken());

        paginationStrategy = new IndexBasedPaginationStrategy(
            (IndexBasedPaginationStrategy.IndexStrategyPageToken) paginationStrategy.getNextToken(),
            1,
            "descending",
            clusterState
        );
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getNextToken());
    }

    private ClusterState getRandomClusterState() {
        final IndexMetadata indexMetadata1 = IndexMetadata.builder("test-index-1")
            .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 1))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();
        final IndexRoutingTable.Builder indexRoutingTable1 = new IndexRoutingTable.Builder(indexMetadata1.getIndex());

        final IndexMetadata indexMetadata2 = IndexMetadata.builder("test-index-2")
            .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 2))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();
        final IndexRoutingTable.Builder indexRoutingTable2 = new IndexRoutingTable.Builder(indexMetadata2.getIndex());

        final IndexMetadata indexMetadata3 = IndexMetadata.builder("test-index-3")
            .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 3))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();
        final IndexRoutingTable.Builder indexRoutingTable3 = new IndexRoutingTable.Builder(indexMetadata3.getIndex());

        final IndexMetadata indexMetadata4 = IndexMetadata.builder("test-index-4")
            .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 4))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();
        final IndexRoutingTable.Builder indexRoutingTable4 = new IndexRoutingTable.Builder(indexMetadata4.getIndex());

        Metadata metadata = Metadata.builder()
            .put(indexMetadata1, true)
            .put(indexMetadata2, true)
            .put(indexMetadata3, true)
            .put(indexMetadata4, true)
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .add(indexRoutingTable1)
            .add(indexRoutingTable2)
            .add(indexRoutingTable3)
            .add(indexRoutingTable4)
            .build();

        return ClusterState.builder(new ClusterName("test")).metadata(metadata).routingTable(routingTable).build();
    }
}

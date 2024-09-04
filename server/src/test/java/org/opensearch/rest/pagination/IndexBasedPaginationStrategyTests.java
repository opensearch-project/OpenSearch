/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.OpenSearchParseException;
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

    private final IndexMetadata indexMetadata1 = IndexMetadata.builder("test-index-1")
        .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 1))
        .numberOfShards(between(1, 10))
        .numberOfReplicas(randomInt(20))
        .build();
    private final IndexRoutingTable.Builder indexRoutingTable1 = new IndexRoutingTable.Builder(indexMetadata1.getIndex());
    private final IndexMetadata indexMetadata2 = IndexMetadata.builder("test-index-2")
        .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 2))
        .numberOfShards(between(1, 10))
        .numberOfReplicas(randomInt(20))
        .build();
    private final IndexRoutingTable.Builder indexRoutingTable2 = new IndexRoutingTable.Builder(indexMetadata2.getIndex());
    private final IndexMetadata indexMetadata3 = IndexMetadata.builder("test-index-3")
        .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 3))
        .numberOfShards(between(1, 10))
        .numberOfReplicas(randomInt(20))
        .build();
    private final IndexRoutingTable.Builder indexRoutingTable3 = new IndexRoutingTable.Builder(indexMetadata3.getIndex());
    private final IndexMetadata indexMetadata4 = IndexMetadata.builder("test-index-4")
        .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 4))
        .numberOfShards(between(1, 10))
        .numberOfReplicas(randomInt(20))
        .build();
    private final IndexRoutingTable.Builder indexRoutingTable4 = new IndexRoutingTable.Builder(indexMetadata4.getIndex());
    private final IndexMetadata indexMetadata5 = IndexMetadata.builder("test-index-5")
        .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, 5))
        .numberOfShards(between(1, 10))
        .numberOfReplicas(randomInt(20))
        .build();
    private final IndexRoutingTable.Builder indexRoutingTable5 = new IndexRoutingTable.Builder(indexMetadata5.getIndex());

    public void testRetrieveAllIndicesInAscendingOrder() {
        ClusterState clusterState = getRandomClusterState();
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "ascending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testRetrieveAllIndicesInDescendingOrder() {
        ClusterState clusterState = getRandomClusterState();
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "descending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testRetrieveAllIndicesWhenIndicesGetDeletedAndCreatedInBetween() {
        // Query1 with 4 indices in clusterState (test-index1,2,3,4)
        ClusterState clusterState = getRandomClusterState();
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "ascending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Query2 adding index5 to clusterState
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata1, true)
                    .put(indexMetadata2, true)
                    .put(indexMetadata3, true)
                    .put(indexMetadata4, true)
                    .put(indexMetadata5, true)
                    .build()
            )
            .routingTable(
                RoutingTable.builder()
                    .add(indexRoutingTable1)
                    .add(indexRoutingTable2)
                    .add(indexRoutingTable3)
                    .add(indexRoutingTable4)
                    .add(indexRoutingTable5)
                    .build()
            )
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting index2 which has already been displayed, still index3 should get displayed
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata1, true)
                    .put(indexMetadata3, true)
                    .put(indexMetadata4, true)
                    .put(indexMetadata5, true)
                    .build()
            )
            .routingTable(
                RoutingTable.builder()
                    .add(indexRoutingTable1)
                    .add(indexRoutingTable3)
                    .add(indexRoutingTable4)
                    .add(indexRoutingTable5)
                    .build()
            )
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting index4 which is not yet displayed which otherwise should have been displayed in the following query
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata1, true).put(indexMetadata3, true).put(indexMetadata5, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable1).add(indexRoutingTable3).add(indexRoutingTable5).build())
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(0, paginationStrategy.getElementsFromRequestedToken().size());
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testRetrieveAllIndicesWhenIndicesGetDeletedAndCreatedInBetweenWithDescOrder() {
        // Query1 with 4 indices in clusterState (test-index1,2,3,4)
        ClusterState clusterState = getRandomClusterState();
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "descending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Query2 adding index5 to clusterState
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata1, true)
                    .put(indexMetadata2, true)
                    .put(indexMetadata3, true)
                    .put(indexMetadata4, true)
                    .put(indexMetadata5, true)
                    .build()
            )
            .routingTable(
                RoutingTable.builder()
                    .add(indexRoutingTable1)
                    .add(indexRoutingTable2)
                    .add(indexRoutingTable3)
                    .add(indexRoutingTable4)
                    .add(indexRoutingTable5)
                    .build()
            )
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting index3 which has already been displayed, still index2 should get displayed
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata1, true)
                    .put(indexMetadata2, true)
                    .put(indexMetadata4, true)
                    .put(indexMetadata5, true)
                    .build()
            )
            .routingTable(
                RoutingTable.builder()
                    .add(indexRoutingTable1)
                    .add(indexRoutingTable2)
                    .add(indexRoutingTable4)
                    .add(indexRoutingTable5)
                    .build()
            )
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting index1 which is not yet displayed which otherwise should have been displayed in the following query
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata2, true).put(indexMetadata4, true).put(indexMetadata5, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable2).add(indexRoutingTable4).add(indexRoutingTable5).build())
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(0, paginationStrategy.getElementsFromRequestedToken().size());
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testRetrieveAllIndicesWhenMultipleIndicesGetDeletedInBetweenAtOnce() {
        // Query1 with 5 indices in clusterState (test-index1,2,3,4,5)
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(indexMetadata1, true)
                    .put(indexMetadata2, true)
                    .put(indexMetadata3, true)
                    .put(indexMetadata4, true)
                    .put(indexMetadata5, true)
                    .build()
            )
            .routingTable(
                RoutingTable.builder()
                    .add(indexRoutingTable1)
                    .add(indexRoutingTable2)
                    .add(indexRoutingTable3)
                    .add(indexRoutingTable4)
                    .add(indexRoutingTable5)
                    .build()
            )
            .build();
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "ascending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Query2, no changes to clusterState
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting index1,index2 & index3. index4 should get displayed
        clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata4, true).put(indexMetadata5, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable4).add(indexRoutingTable5).build())
            .build();
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting index1 which is not yet displayed which otherwise should have been displayed in the following query
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-5", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testCreatingIndexStrategyPageTokenWithRequestedTokenNull() {
        try {
            new IndexBasedPaginationStrategy.IndexStrategyTokenParser(null);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("requestedTokenString can not be null");
        }
    }

    public void testIndexStrategyPageTokenWithWronglyEncryptedRequestToken() {
        try {
            // % is not allowed in base64 encoding
            new IndexBasedPaginationStrategy.IndexStrategyTokenParser("3%4%5");
            fail("expected exception");
        } catch (OpenSearchParseException e) {
            assert e.getMessage().contains("[next_token] has been tainted");
        }
    }

    public void testIndexStrategyPageTokenWithLessElementsInRequestedToken() {
        String encryptedRequestToken = PaginationStrategy.encryptStringToken("1$1725361543$1725361543");
        try {
            // should throw exception as it expects 4 elements separated by $
            new IndexBasedPaginationStrategy.IndexStrategyTokenParser(encryptedRequestToken);
            fail("expected exception");
        } catch (OpenSearchParseException e) {
            assert e.getMessage().contains("[next_token] has been tainted");
        }
    }

    public void testIndexStrategyPageTokenWithInvalidValuesInRequestedToken() {
        {
            String encryptedRequestToken = PaginationStrategy.encryptStringToken("1$-1725361543$-1725361543$index");
            try {
                // should not accept invalid long values (negative)
                new IndexBasedPaginationStrategy.IndexStrategyTokenParser(encryptedRequestToken);
                fail("expected exception");
            } catch (OpenSearchParseException e) {
                assert e.getMessage().contains("[next_token] has been tainted");
            }
        }

        {
            String encryptedRequestToken = PaginationStrategy.encryptStringToken("-1$1725361543$1725361543$index");
            try {
                // should not accept invalid int values (negative)
                new IndexBasedPaginationStrategy.IndexStrategyTokenParser(encryptedRequestToken);
                fail("expected exception");
            } catch (OpenSearchParseException e) {
                assert e.getMessage().contains("[next_token] has been tainted");
            }
        }
    }

    public void testCreatingIndexStrategyPageTokenWithNameOfLastRespondedIndexNull() {
        try {
            new IndexBasedPaginationStrategy.IndexStrategyTokenParser(1, 1234l, 1234l, null);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("index name should be provided");
        }
    }

    private ClusterState getRandomClusterState() {
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

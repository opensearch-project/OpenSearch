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

import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;

public class IndexBasedPaginationStrategyTests extends OpenSearchTestCase {

    public void testRetrieveAllIndicesInAscendingOrder() {
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4));

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
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4));

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
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4));
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "ascending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Adding index5 to clusterState, before executing next query.
        clusterState = addIndexToClusterState(clusterState, 5);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting test-index-2 which has already been displayed, still test-index-2 should get displayed
        clusterState = deleteIndexFromClusterState(clusterState, 2);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting test-index-4 which is not yet displayed which otherwise should have been displayed in the following query
        // instead test-index-5 should now get displayed.
        clusterState = deleteIndexFromClusterState(clusterState, 4);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-5", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testRetrieveAllIndicesWhenIndicesGetDeletedAndCreatedInBetweenWithDescOrder() {
        // Query1 with 4 indices in clusterState (test-index1,2,3,4).
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4));
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "descending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // adding test-index-5 to clusterState, before executing next query.
        clusterState = addIndexToClusterState(clusterState, 5);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-3", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting test-index-3 which has already been displayed, still index2 should get displayed.
        clusterState = deleteIndexFromClusterState(clusterState, 3);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting test-index-1 which is not yet displayed which otherwise should have been displayed in the following query.
        clusterState = deleteIndexFromClusterState(clusterState, 1);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "descending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(0, paginationStrategy.getElementsFromRequestedToken().size());
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testRetrieveAllIndicesWhenMultipleIndicesGetDeletedInBetweenAtOnce() {
        // Query1 with 5 indices in clusterState (test-index1,2,3,4,5).
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4, 5));
        PaginatedQueryRequest paginatedQueryRequest = new PaginatedQueryRequest(null, "ascending", 1);
        IndexBasedPaginationStrategy paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-1", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // executing next query without any changes to clusterState
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-2", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Deleting test-index-1, test-index-2 & test-index-3 and executing next query. test-index-4 should get displayed.
        clusterState = deleteIndexFromClusterState(clusterState, 1);
        clusterState = deleteIndexFromClusterState(clusterState, 2);
        clusterState = deleteIndexFromClusterState(clusterState, 3);
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-4", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNotNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());

        // Executing the last query without any further change. Should result in test-index-5 and nextToken as null.
        paginatedQueryRequest = new PaginatedQueryRequest(paginationStrategy.getPaginatedQueryResponse().getNextToken(), "ascending", 1);
        paginationStrategy = new IndexBasedPaginationStrategy(paginatedQueryRequest, "test", clusterState);
        assertEquals(1, paginationStrategy.getElementsFromRequestedToken().size());
        assertEquals("test-index-5", paginationStrategy.getElementsFromRequestedToken().get(0));
        assertNull(paginationStrategy.getPaginatedQueryResponse().getNextToken());
    }

    public void testCreatingIndexStrategyPageTokenWithRequestedTokenNull() {
        try {
            new IndexBasedPaginationStrategy.IndexStrategyToken(null);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("requestedTokenString can not be null");
        }
    }

    public void testIndexStrategyPageTokenWithWronglyEncryptedRequestToken() {
        assertThrows(OpenSearchParseException.class, () -> new IndexBasedPaginationStrategy.IndexStrategyToken("3%4%5"));
    }

    public void testIndexStrategyPageTokenWithIncorrectNumberOfElementsInRequestedToken() {
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexBasedPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("1$1725361543"))
        );
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexBasedPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("1$1725361543$index$12345"))
        );
    }

    public void testIndexStrategyPageTokenWithInvalidValuesInRequestedToken() {
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexBasedPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("1$-1725361543$index"))
        );
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexBasedPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("-1$1725361543$index"))
        );
    }

    public void testCreatingIndexStrategyPageTokenWithNameOfLastRespondedIndexNull() {
        try {
            new IndexBasedPaginationStrategy.IndexStrategyToken(1, 1234l, null);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("index name should be provided");
        }
    }

    /**
     * @param indexNumbers would be used to create indices having names with integer appended after foo, like foo1, foo2.
     * @return random clusterState consisting of indices having their creation times set to the integer used to name them.
     */
    private ClusterState getRandomClusterState(List<Integer> indexNumbers) {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .build();
        for (Integer indexNumber : indexNumbers) {
            clusterState = addIndexToClusterState(clusterState, indexNumber);
        }
        return clusterState;
    }

    private ClusterState addIndexToClusterState(ClusterState clusterState, int indexNumber) {
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index-" + indexNumber)
            .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, indexNumber))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();
        IndexRoutingTable.Builder indexRoutingTableBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex());
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder(clusterState.routingTable()).add(indexRoutingTableBuilder).build())
            .build();
    }

    private ClusterState deleteIndexFromClusterState(ClusterState clusterState, int indexNumber) {
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).remove("test-index-" + indexNumber))
            .routingTable(RoutingTable.builder(clusterState.routingTable()).remove("test-index-" + indexNumber).build())
            .build();
    }

}

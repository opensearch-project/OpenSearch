/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;
import static org.opensearch.action.pagination.PageParams.PARAM_DESC_SORT_VALUE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;

public class IndexPaginationStrategyTests extends OpenSearchTestCase {

    public void testRetrieveAllIndicesWithVaryingPageSize() {
        List<Integer> indexNumberList = new ArrayList<>();
        final int totalIndices = 100;
        for (int indexNumber = 1; indexNumber <= 100; indexNumber++) {
            indexNumberList.add(indexNumber);
        }
        // creating a cluster state with 100 indices
        Collections.shuffle(indexNumberList, getRandom());
        ClusterState clusterState = getRandomClusterState(indexNumberList);

        // Checking pagination response for different pageSizes, which has a mix of even and odd numbers
        // to ensure number of indices in last page is not always equal to pageSize.
        List<Integer> pageSizeList = List.of(1, 6, 10, 13);
        List<String> sortOrderList = List.of(PARAM_ASC_SORT_VALUE, PARAM_DESC_SORT_VALUE);
        for (String sortOrder : sortOrderList) {
            for (int pageSize : pageSizeList) {
                String requestedToken = null;
                int totalPagesToFetch = (int) Math.ceil(totalIndices / (pageSize * 1.0));
                int indicesRemaining = totalIndices;
                for (int pageNumber = 1; pageNumber <= totalPagesToFetch; pageNumber++) {
                    PageParams pageParams = new PageParams(requestedToken, sortOrder, pageSize);
                    IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
                    if (pageNumber < totalPagesToFetch) {
                        assertNotNull(paginationStrategy.getResponseToken().getNextToken());
                    } else {
                        assertNull(paginationStrategy.getResponseToken().getNextToken());
                    }
                    requestedToken = paginationStrategy.getResponseToken().getNextToken();
                    // Asserting all the indices received
                    int responseItr = 0;
                    if (PARAM_ASC_SORT_VALUE.equals(sortOrder)) {
                        for (int indexNumber = (pageNumber - 1) * pageSize; indexNumber < Math.min(
                            100,
                            pageNumber * pageSize
                        ); indexNumber++) {
                            assertEquals("test-index-" + (indexNumber + 1), paginationStrategy.getRequestedEntities().get(responseItr));
                            responseItr++;
                        }
                    } else {
                        int endIndexNumberForPage = Math.max(indicesRemaining - pageSize, 0);
                        for (; indicesRemaining > endIndexNumberForPage; indicesRemaining--) {
                            assertEquals("test-index-" + indicesRemaining, paginationStrategy.getRequestedEntities().get(responseItr));
                            responseItr++;
                        }
                    }
                    assertEquals(responseItr, paginationStrategy.getRequestedEntities().size());
                }
            }
        }
    }

    public void testRetrieveAllIndicesInAscOrderWhileIndicesGetCreatedAndDeleted() {
        List<Integer> indexNumberList = new ArrayList<>();
        List<Integer> deletedIndices = new ArrayList<>();
        final int totalIndices = 100;
        final int numIndicesToDelete = 10;
        final int numIndicesToCreate = 5;
        List<String> indicesFetched = new ArrayList<>();
        for (int indexNumber = 1; indexNumber <= 100; indexNumber++) {
            indexNumberList.add(indexNumber);
        }
        ClusterState clusterState = getRandomClusterState(indexNumberList);

        int pageSize = 6;
        String requestedToken = null;
        int numPages = 0;
        do {
            numPages++;
            PageParams pageParams = new PageParams(requestedToken, PARAM_ASC_SORT_VALUE, pageSize);
            IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
            assertNotNull(paginationStrategy);
            assertNotNull(paginationStrategy.getResponseToken());
            requestedToken = paginationStrategy.getResponseToken().getNextToken();
            // randomly deleting 10 indices after 3rd call
            if (numPages == 3) {
                deletedIndices = indexNumberList.subList(20, indexNumberList.size());
                Collections.shuffle(deletedIndices, getRandom());
                for (int pos = 0; pos < numIndicesToDelete; pos++) {
                    clusterState = deleteIndexFromClusterState(clusterState, deletedIndices.get(pos));
                }
            }
            // creating 5 indices after 5th call
            if (numPages == 5) {
                for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
                    clusterState = addIndexToClusterState(clusterState, indexNumber);
                }
            }
            if (requestedToken == null) {
                assertEquals(paginationStrategy.getRequestedEntities().size(), 5);
            } else {
                assertEquals(paginationStrategy.getRequestedEntities().size(), pageSize);
            }

            indicesFetched.addAll(paginationStrategy.getRequestedEntities());
        } while (Objects.nonNull(requestedToken));

        assertEquals((int) Math.ceil((double) (totalIndices + numIndicesToCreate - numIndicesToDelete) / pageSize), numPages);
        assertEquals(totalIndices + numIndicesToCreate - numIndicesToDelete, indicesFetched.size());

        // none of the deleted index should appear in the list of fetched indices
        for (int deletedIndexPos = 0; deletedIndexPos < numIndicesToDelete; deletedIndexPos++) {
            assertFalse(indicesFetched.contains("test-index-" + deletedIndices.get(deletedIndexPos)));
        }

        // all the newly created indices should be present in the list of fetched indices
        for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
            assertTrue(indicesFetched.contains("test-index-" + indexNumber));
        }
    }

    public void testRetrieveAllIndicesInDescOrderWhileIndicesGetCreatedAndDeleted() {
        List<Integer> indexNumberList = new ArrayList<>();
        List<Integer> deletedIndices = new ArrayList<>();
        final int totalIndices = 100;
        final int numIndicesToDelete = 9;
        final int numIndicesToCreate = 5;
        List<String> indicesFetched = new ArrayList<>();
        for (int indexNumber = 1; indexNumber <= 100; indexNumber++) {
            indexNumberList.add(indexNumber);
        }
        ClusterState clusterState = getRandomClusterState(indexNumberList);

        int pageSize = 6;
        String requestedToken = null;
        int numPages = 0;
        do {
            numPages++;
            PageParams pageParams = new PageParams(requestedToken, PARAM_DESC_SORT_VALUE, pageSize);
            IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
            assertNotNull(paginationStrategy);
            assertNotNull(paginationStrategy.getResponseToken());
            requestedToken = paginationStrategy.getResponseToken().getNextToken();
            // randomly deleting 10 indices after 3rd call
            if (numPages == 3) {
                deletedIndices = indexNumberList.subList(0, 80);
                Collections.shuffle(deletedIndices, getRandom());
                for (int pos = 0; pos < numIndicesToDelete; pos++) {
                    clusterState = deleteIndexFromClusterState(clusterState, deletedIndices.get(pos));
                }
            }
            // creating 5 indices after 5th call
            if (numPages == 5) {
                for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
                    clusterState = addIndexToClusterState(clusterState, indexNumber);
                }
            }
            if (requestedToken == null) {
                assertEquals(paginationStrategy.getRequestedEntities().size(), (totalIndices - numIndicesToDelete) % pageSize);
            } else {
                assertEquals(paginationStrategy.getRequestedEntities().size(), pageSize);
            }

            indicesFetched.addAll(paginationStrategy.getRequestedEntities());
        } while (Objects.nonNull(requestedToken));

        assertEquals((int) Math.ceil((double) (totalIndices - numIndicesToDelete) / pageSize), numPages);
        assertEquals(totalIndices - numIndicesToDelete, indicesFetched.size());

        // none of the deleted index should appear in the list of fetched indices
        for (int deletedIndexPos = 0; deletedIndexPos < numIndicesToDelete; deletedIndexPos++) {
            assertFalse(indicesFetched.contains("test-index-" + deletedIndices.get(deletedIndexPos)));
        }

        // none of the newly created indices should be present in the list of fetched indices
        for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
            assertFalse(indicesFetched.contains("test-index-" + indexNumber));
        }
    }

    public void testRetrieveIndicesWithSizeOneAndCurrentIndexGetsDeletedAscOrder() {
        // Query1 with 4 indices in clusterState (test-index1,2,3,4)
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4));
        PageParams pageParams = new PageParams(null, PARAM_ASC_SORT_VALUE, 1);
        IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, true);
        assertEquals("test-index-1", paginationStrategy.getRequestedEntities().get(0));

        // Adding index5 to clusterState, before executing next query.
        clusterState = addIndexToClusterState(clusterState, 5);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, true);
        assertEquals("test-index-2", paginationStrategy.getRequestedEntities().get(0));

        // Deleting test-index-2 which has already been displayed, still test-index-3 should get displayed
        clusterState = deleteIndexFromClusterState(clusterState, 2);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, true);
        assertEquals("test-index-3", paginationStrategy.getRequestedEntities().get(0));

        // Deleting test-index-4 which is not yet displayed which otherwise should have been displayed in the following query
        // instead test-index-5 should now get displayed.
        clusterState = deleteIndexFromClusterState(clusterState, 4);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, false);
        assertEquals("test-index-5", paginationStrategy.getRequestedEntities().get(0));

    }

    public void testRetrieveIndicesWithSizeOneAndCurrentIndexGetsDeletedDescOrder() {
        // Query1 with 4 indices in clusterState (test-index1,2,3,4).
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4));
        PageParams pageParams = new PageParams(null, PARAM_DESC_SORT_VALUE, 1);
        IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, true);
        assertEquals("test-index-4", paginationStrategy.getRequestedEntities().get(0));

        // adding test-index-5 to clusterState, before executing next query.
        clusterState = addIndexToClusterState(clusterState, 5);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_DESC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, true);
        assertEquals("test-index-3", paginationStrategy.getRequestedEntities().get(0));

        // Deleting test-index-3 which has already been displayed, still index2 should get displayed.
        clusterState = deleteIndexFromClusterState(clusterState, 3);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_DESC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 1, true);
        assertEquals("test-index-2", paginationStrategy.getRequestedEntities().get(0));

        // Deleting test-index-1 which is not yet displayed which otherwise should have been displayed in the following query.
        clusterState = deleteIndexFromClusterState(clusterState, 1);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_DESC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertPaginationResult(paginationStrategy, 0, false);
    }

    public void testRetrieveIndicesWithMultipleDeletionsAtOnceAscOrder() {
        // Query1 with 5 indices in clusterState (test-index1,2,3,4,5).
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4, 5));
        PageParams pageParams = new PageParams(null, PARAM_ASC_SORT_VALUE, 1);
        IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertEquals(1, paginationStrategy.getRequestedEntities().size());
        assertEquals("test-index-1", paginationStrategy.getRequestedEntities().get(0));
        assertNotNull(paginationStrategy.getResponseToken().getNextToken());

        // executing next query without any changes to clusterState
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertEquals(1, paginationStrategy.getRequestedEntities().size());
        assertEquals("test-index-2", paginationStrategy.getRequestedEntities().get(0));
        assertNotNull(paginationStrategy.getResponseToken().getNextToken());

        // Deleting test-index-1, test-index-2 & test-index-3 and executing next query. test-index-4 should get displayed.
        clusterState = deleteIndexFromClusterState(clusterState, 1);
        clusterState = deleteIndexFromClusterState(clusterState, 2);
        clusterState = deleteIndexFromClusterState(clusterState, 3);
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertEquals(1, paginationStrategy.getRequestedEntities().size());
        assertEquals("test-index-4", paginationStrategy.getRequestedEntities().get(0));
        assertNotNull(paginationStrategy.getResponseToken().getNextToken());

        // Executing the last query without any further change. Should result in test-index-5 and nextToken as null.
        pageParams = new PageParams(paginationStrategy.getResponseToken().getNextToken(), PARAM_ASC_SORT_VALUE, 1);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertEquals(1, paginationStrategy.getRequestedEntities().size());
        assertEquals("test-index-5", paginationStrategy.getRequestedEntities().get(0));
        assertNull(paginationStrategy.getResponseToken().getNextToken());
    }

    public void testRetrieveIndicesWithTokenModifiedToQueryBeyondTotal() {
        ClusterState clusterState = getRandomClusterState(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        PageParams pageParams = new PageParams(null, PARAM_ASC_SORT_VALUE, 10);
        IndexPaginationStrategy paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertEquals(10, paginationStrategy.getRequestedEntities().size());
        assertNull(paginationStrategy.getResponseToken().getNextToken());
        // creating a token with last sent index as test-index-10
        String token = clusterState.metadata().indices().get("test-index-10").getCreationDate() + "|" + "test-index-10";
        pageParams = new PageParams(Base64.getEncoder().encodeToString(token.getBytes(UTF_8)), PARAM_ASC_SORT_VALUE, 10);
        paginationStrategy = new IndexPaginationStrategy(pageParams, clusterState);
        assertEquals(0, paginationStrategy.getRequestedEntities().size());
        assertNull(paginationStrategy.getResponseToken().getNextToken());
    }

    public void testCreatingIndexStrategyPageTokenWithRequestedTokenNull() {
        try {
            new IndexPaginationStrategy.IndexStrategyToken(null);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("requestedTokenString can not be null");
        }
    }

    public void testIndexStrategyPageTokenWithWronglyEncryptedRequestToken() {
        assertThrows(OpenSearchParseException.class, () -> new IndexPaginationStrategy.IndexStrategyToken("3%4%5"));
    }

    public void testIndexStrategyPageTokenWithIncorrectNumberOfElementsInRequestedToken() {
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("1725361543"))
        );
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("1|1725361543|index|12345"))
        );
    }

    public void testIndexStrategyPageTokenWithInvalidValuesInRequestedToken() {
        assertThrows(
            OpenSearchParseException.class,
            () -> new IndexPaginationStrategy.IndexStrategyToken(PaginationStrategy.encryptStringToken("-1725361543|index"))
        );
    }

    public void testCreatingIndexStrategyPageTokenWithNameOfLastRespondedIndexNull() {
        try {
            new IndexPaginationStrategy.IndexStrategyToken(1234l, null);
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
            .settings(
                settings(Version.CURRENT).put(SETTING_CREATION_DATE, Instant.now().plus(indexNumber, ChronoUnit.SECONDS).toEpochMilli())
            )
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

    private void assertPaginationResult(IndexPaginationStrategy paginationStrategy, int expectedEntities, boolean tokenExpected) {
        assertNotNull(paginationStrategy);
        assertEquals(expectedEntities, paginationStrategy.getRequestedEntities().size());
        assertNotNull(paginationStrategy.getResponseToken());
        assertEquals(tokenExpected, Objects.nonNull(paginationStrategy.getResponseToken().getNextToken()));
    }

}

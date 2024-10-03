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
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;
import static org.opensearch.action.pagination.PageParams.PARAM_DESC_SORT_VALUE;
import static org.opensearch.action.pagination.PaginationStrategy.INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;

public class ShardPaginationStrategyTests extends OpenSearchTestCase {

    private static final String TEST_INDEX_PREFIX = "test-index-";
    private static final int DEFAULT_NUMBER_OF_SHARDS = 5;
    private static final int DEFAULT_NUMBER_OF_REPLICAS = 2;

    /**
     * Test validates fetching all the shards for 100 indices in a paginated fashion using {@link ShardPaginationStrategy},
     * while no indices are created or deleted during the fetch.
     */
    public void testRetrieveAllShardsWithVaryingPageSize() {
        List<Integer> indexNumberList = new ArrayList<>();
        final int totalIndices = 100;
        final int totalShards = totalIndices * DEFAULT_NUMBER_OF_SHARDS * (DEFAULT_NUMBER_OF_REPLICAS + 1);
        for (int indexNumber = 1; indexNumber <= 100; indexNumber++) {
            indexNumberList.add(indexNumber);
        }
        // creating a cluster state with 100 indices
        Collections.shuffle(indexNumberList, getRandom());
        ClusterState clusterState = getRandomClusterState(indexNumberList);

        // Checking pagination response for different pageSizes, which has a mix of even and odd numbers
        // to ensure number of shards in last page is not always equal to pageSize.
        List<Integer> pageSizeList = List.of(3, 7, 10, 13);

        List<String> sortOrderList = List.of(PARAM_ASC_SORT_VALUE, PARAM_DESC_SORT_VALUE);
        for (String sortOrder : sortOrderList) {
            for (int pageSize : pageSizeList) {
                List<ShardRouting> shardRoutings = new ArrayList<>();
                Set<String> indices = new HashSet<>();
                String requestedToken = null;
                // Since each shardID will have number of (replicas + 1) shards, thus number
                // of shards in a page should always be a multiple of (replicas + 1).
                int totalPagesToFetch = (int) Math.ceil(totalShards * 1.0 / (pageSize - pageSize % (DEFAULT_NUMBER_OF_REPLICAS + 1)));

                long lastPageIndexTime = 0;
                for (int pageNum = 1; pageNum <= totalPagesToFetch; pageNum++) {
                    PageParams pageParams = new PageParams(requestedToken, sortOrder, pageSize);
                    ShardPaginationStrategy strategy = new ShardPaginationStrategy(pageParams, clusterState);
                    List<ShardRouting> pageShards = strategy.getRequestedEntities();
                    List<String> pageIndices = strategy.getRequestedIndices();
                    if (pageNum < totalPagesToFetch) {
                        assertNotNull(strategy.getResponseToken().getNextToken());
                    } else {
                        assertNull(strategy.getResponseToken().getNextToken());
                    }
                    shardRoutings.addAll(pageShards);
                    indices.addAll(pageIndices);
                    long currentLastIndexTime = clusterState.metadata()
                        .indices()
                        .get(pageShards.get(pageShards.size() - 1).getIndexName())
                        .getCreationDate();
                    if (lastPageIndexTime > 0) {
                        if (sortOrder.equals(PARAM_ASC_SORT_VALUE)) {
                            assertTrue(lastPageIndexTime <= currentLastIndexTime);
                        } else {
                            assertTrue(lastPageIndexTime >= currentLastIndexTime);
                        }
                    }
                    lastPageIndexTime = currentLastIndexTime;
                    requestedToken = strategy.getResponseToken().getNextToken();
                }
                assertEquals(totalShards, shardRoutings.size());
                assertEquals(totalIndices, indices.size());

                // verifying the order of shards in the response.
                verifyOrderForAllShards(clusterState, shardRoutings, sortOrder);
            }
        }
    }

    /**
     * Test validates fetching all the shards (in asc order) for 100 indices in a paginated fashion using {@link ShardPaginationStrategy}.
     * While executing queries, it tries to delete an index for which some of the shards have already been fetched along
     * with some other indices which are yet to be fetched. It also creates new indices in between the queries.
     * Shards corresponding to indices deleted, should not be fetched while for the indices which were newly created,
     * should appear in the response.
     */
    public void testRetrieveAllShardsInAscOrderWhileIndicesGetCreatedAndDeleted() {
        List<Integer> indexNumberList = new ArrayList<>();
        List<Integer> deletedIndices = new ArrayList<>();
        List<String> newIndices = new ArrayList<>();
        final int totalIndices = 100;
        final int numIndicesToDelete = 10;
        final int numIndicesToCreate = 5;
        final int fixedIndexNumToDelete = 7;
        final int numShardsForNewIndices = 4;
        final int numReplicasForNewIndices = 5;
        final int totalInitialShards = totalIndices * DEFAULT_NUMBER_OF_SHARDS * (DEFAULT_NUMBER_OF_REPLICAS + 1);
        for (int indexNumber = 1; indexNumber <= 100; indexNumber++) {
            indexNumberList.add(indexNumber);
        }
        ClusterState clusterState = getRandomClusterState(indexNumberList);

        int pageSize = 10;
        String requestedToken = null;
        int numPages = 0;
        List<ShardRouting> shardRoutings = new ArrayList<>();
        Set<String> indicesFetched = new HashSet<>();
        do {
            numPages++;
            PageParams pageParams = new PageParams(requestedToken, PARAM_ASC_SORT_VALUE, pageSize);
            ShardPaginationStrategy paginationStrategy = new ShardPaginationStrategy(pageParams, clusterState);
            assertNotNull(paginationStrategy);
            assertNotNull(paginationStrategy.getResponseToken());
            requestedToken = paginationStrategy.getResponseToken().getNextToken();
            // deleting test-index-7 & 10 more random indices after 11th call. By that time, shards for first 6
            // indices would have been completely fetched. 11th call, would fetch first 3 shardsIDs for test-index-7 and
            // if it gets deleted, rest 2 shardIDs for it should not appear.
            if (numPages == 11) {
                // asserting last elements of current response list, which should be the last shard of test-index-6
                assertEquals(TEST_INDEX_PREFIX + 6, shardRoutings.get(shardRoutings.size() - 1).getIndexName());
                assertEquals(DEFAULT_NUMBER_OF_SHARDS - 1, shardRoutings.get(shardRoutings.size() - 1).getId());

                // asserting the result of 11th query, which should only contain 3 shardIDs belonging to test-index-7
                assertEquals(3 * (DEFAULT_NUMBER_OF_REPLICAS + 1), paginationStrategy.getRequestedEntities().size());
                assertEquals(1, paginationStrategy.getRequestedIndices().size());
                assertEquals(TEST_INDEX_PREFIX + 7, paginationStrategy.getRequestedIndices().get(0));
                assertEquals(2, paginationStrategy.getRequestedEntities().get(8).id());

                clusterState = deleteIndexFromClusterState(clusterState, fixedIndexNumToDelete);
                deletedIndices = indexNumberList.subList(20, indexNumberList.size());
                Collections.shuffle(deletedIndices, getRandom());
                deletedIndices = deletedIndices.subList(0, numIndicesToDelete);
                for (Integer index : deletedIndices) {
                    clusterState = deleteIndexFromClusterState(clusterState, index);
                }
            }
            // creating 5 indices after 5th call
            if (numPages == 5) {
                for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
                    newIndices.add(TEST_INDEX_PREFIX + indexNumber);
                    clusterState = addIndexToClusterState(clusterState, indexNumber, numShardsForNewIndices, numReplicasForNewIndices);
                }
            }
            assertTrue(paginationStrategy.getRequestedEntities().size() <= pageSize);
            shardRoutings.addAll(paginationStrategy.getRequestedEntities());
            indicesFetched.addAll(paginationStrategy.getRequestedIndices());
        } while (Objects.nonNull(requestedToken));

        assertEquals(totalIndices + numIndicesToCreate - numIndicesToDelete, indicesFetched.size());
        // finalTotalShards = InitialTotal - 2ShardIdsForIndex7 - ShardsFor10RandomlyDeletedIndices + ShardsForNewlyCreatedIndices
        final int totalShards = totalInitialShards - 2 * (DEFAULT_NUMBER_OF_REPLICAS + 1) - numIndicesToDelete * DEFAULT_NUMBER_OF_SHARDS
            * (DEFAULT_NUMBER_OF_REPLICAS + 1) + numIndicesToCreate * numShardsForNewIndices * (numReplicasForNewIndices + 1);
        assertEquals(totalShards, shardRoutings.size());

        // deleted test-index-7, should appear in the response shards and indices
        assertTrue(indicesFetched.contains(TEST_INDEX_PREFIX + 7));
        assertEquals(
            shardRoutings.stream().filter(shard -> shard.getIndexName().equals(TEST_INDEX_PREFIX + 7)).count(),
            3 * (DEFAULT_NUMBER_OF_REPLICAS + 1)
        );

        // none of the randomly deleted index should appear in the list of fetched indices
        for (Integer index : deletedIndices) {
            String indexName = TEST_INDEX_PREFIX + index;
            assertFalse(indicesFetched.contains(indexName));
            assertEquals(shardRoutings.stream().filter(shard -> shard.getIndexName().equals(indexName)).count(), 0);
        }

        // all the newly created indices should be present in the list of fetched indices
        verifyOrderForAllShards(
            clusterState,
            shardRoutings.stream().filter(shard -> newIndices.contains(shard.getIndexName())).collect(Collectors.toList()),
            PARAM_ASC_SORT_VALUE
        );
        for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
            String indexName = TEST_INDEX_PREFIX + indexNumber;
            assertTrue(indicesFetched.contains(indexName));
            assertEquals(
                numShardsForNewIndices * (numReplicasForNewIndices + 1),
                shardRoutings.stream().filter(shard -> shard.getIndexName().equals(indexName)).count()
            );
        }
    }

    /**
     * Test validates fetching all the shards (in desc order) for 100 indices in a paginated fashion using {@link ShardPaginationStrategy}.
     * While executing queries, it tries to delete an index for which some of the shards have already been fetched along
     * with some other indices which are yet to be fetched. It also creates new indices in between the queries.
     * Shards corresponding to indices deleted, should not be fetched while for the indices which were newly created,
     * should appear in the response.
     */
    public void testRetrieveAllShardsInDescOrderWhileIndicesGetCreatedAndDeleted() {
        List<Integer> indexNumberList = new ArrayList<>();
        List<Integer> deletedIndices = new ArrayList<>();
        final int totalIndices = 100;
        final int numIndicesToDelete = 10;
        final int numIndicesToCreate = 5;
        final int fixedIndexNumToDelete = 94;
        final int numShardsForNewIndices = 4;
        final int numReplicasForNewIndices = 5;
        final int totalInitialShards = totalIndices * DEFAULT_NUMBER_OF_SHARDS * (DEFAULT_NUMBER_OF_REPLICAS + 1);
        for (int indexNumber = 1; indexNumber <= 100; indexNumber++) {
            indexNumberList.add(indexNumber);
        }
        ClusterState clusterState = getRandomClusterState(indexNumberList);

        int pageSize = 10;
        String requestedToken = null;
        int numPages = 0;
        List<ShardRouting> shardRoutings = new ArrayList<>();
        Set<String> indicesFetched = new HashSet<>();
        do {
            PageParams pageParams = new PageParams(requestedToken, PARAM_DESC_SORT_VALUE, pageSize);
            ShardPaginationStrategy paginationStrategy = new ShardPaginationStrategy(pageParams, clusterState);
            numPages++;
            assertNotNull(paginationStrategy);
            assertNotNull(paginationStrategy.getResponseToken());
            requestedToken = paginationStrategy.getResponseToken().getNextToken();
            // deleting test-index-94 & 10 more random indices after 11th call. By that time, shards for last 6
            // indices would have been completely fetched. 11th call, would fetch first 3 shardsIDs for test-index-94 and
            // if it gets deleted, rest 2 shardIDs for it should not appear.
            if (numPages == 11) {
                // asserting last elements of current response list, which should be the last shard of test-index-95
                assertEquals(TEST_INDEX_PREFIX + 95, shardRoutings.get(shardRoutings.size() - 1).getIndexName());
                assertEquals(DEFAULT_NUMBER_OF_SHARDS - 1, shardRoutings.get(shardRoutings.size() - 1).getId());

                // asserting the result of 11th query, which should only contain 3 shardIDs belonging to test-index-7
                assertEquals(3 * (DEFAULT_NUMBER_OF_REPLICAS + 1), paginationStrategy.getRequestedEntities().size());
                assertEquals(1, paginationStrategy.getRequestedIndices().size());
                assertEquals(TEST_INDEX_PREFIX + 94, paginationStrategy.getRequestedIndices().get(0));
                assertEquals(2, paginationStrategy.getRequestedEntities().get(8).id());

                clusterState = deleteIndexFromClusterState(clusterState, fixedIndexNumToDelete);
                deletedIndices = indexNumberList.subList(0, 80);
                Collections.shuffle(deletedIndices, getRandom());
                deletedIndices = deletedIndices.subList(0, numIndicesToDelete);
                for (Integer index : deletedIndices) {
                    clusterState = deleteIndexFromClusterState(clusterState, index);
                }
            }
            // creating 5 indices after 5th call
            if (numPages == 5) {
                for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
                    clusterState = addIndexToClusterState(clusterState, indexNumber, numShardsForNewIndices, numReplicasForNewIndices);
                }
            }
            assertTrue(paginationStrategy.getRequestedEntities().size() <= pageSize);
            shardRoutings.addAll(paginationStrategy.getRequestedEntities());
            indicesFetched.addAll(paginationStrategy.getRequestedIndices());
        } while (Objects.nonNull(requestedToken));
        assertEquals(totalIndices - numIndicesToDelete, indicesFetched.size());
        // finalTotalShards = InitialTotal - 2ShardIdsForIndex7 - ShardsFor10RandomlyDeletedIndices
        final int totalShards = totalInitialShards - 2 * (DEFAULT_NUMBER_OF_REPLICAS + 1) - numIndicesToDelete * DEFAULT_NUMBER_OF_SHARDS
            * (DEFAULT_NUMBER_OF_REPLICAS + 1);
        assertEquals(totalShards, shardRoutings.size());

        // deleted test-index-94, should appear in the response shards and indices
        assertTrue(indicesFetched.contains(TEST_INDEX_PREFIX + fixedIndexNumToDelete));
        assertEquals(
            shardRoutings.stream().filter(shard -> shard.getIndexName().equals(TEST_INDEX_PREFIX + fixedIndexNumToDelete)).count(),
            3 * (DEFAULT_NUMBER_OF_REPLICAS + 1)
        );

        // none of the randomly deleted index should appear in the list of fetched indices
        for (int deletedIndex : deletedIndices) {
            String indexName = TEST_INDEX_PREFIX + deletedIndex;
            assertFalse(indicesFetched.contains(indexName));
            assertEquals(shardRoutings.stream().filter(shard -> shard.getIndexName().equals(indexName)).count(), 0);
        }

        // none of the newly created indices should be present in the list of fetched indices
        for (int indexNumber = totalIndices + 1; indexNumber <= totalIndices + numIndicesToCreate; indexNumber++) {
            String indexName = TEST_INDEX_PREFIX + indexNumber;
            assertFalse(indicesFetched.contains(indexName));
            assertEquals(0, shardRoutings.stream().filter(shard -> shard.getIndexName().equals(indexName)).count());
        }
    }

    /**
     * Validates strategy fails with IllegalArgumentException when requests size in pageParam is smaller
     * than #(replicas + 1) for any of the index.
     */
    public void testIllegalSizeArgumentRequestedFromStrategy() {
        int numIndices = 6;
        int numShards = 5;
        int numReplicas = 8;
        int pageSize = numReplicas + 1;
        ClusterState clusterState = getRandomClusterState(Collections.emptyList());
        for (int index = 1; index < numIndices; index++) {
            clusterState = addIndexToClusterState(clusterState, index, numShards, numReplicas);
        }
        clusterState = addIndexToClusterState(clusterState, numIndices, numShards + 1, numReplicas + 1);

        try {
            String requestedToken = null;
            ShardPaginationStrategy strategy;
            do {
                PageParams pageParams = new PageParams(requestedToken, PARAM_ASC_SORT_VALUE, pageSize);
                strategy = new ShardPaginationStrategy(pageParams, clusterState);
                requestedToken = strategy.getResponseToken().getNextToken();
            } while (requestedToken != null);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("size value should be greater than the replica count of all indices"));
        }
    }

    public void testRetrieveShardsWhenLastIndexGetsDeletedAndReCreated() {
        final int totalIndices = 3;
        final int numShards = 3;
        final int numReplicas = 3;
        final int pageSize = (numReplicas + 1) * 2;
        final int totalPages = (int) Math.ceil((totalIndices * numShards * (numReplicas + 1) * 1.0) / pageSize);
        ClusterState clusterState = getRandomClusterState(Collections.emptyList());
        for (int indexNum = 1; indexNum <= totalIndices; indexNum++) {
            clusterState = addIndexToClusterState(clusterState, indexNum, numShards, numReplicas);
        }

        String requestedToken = null;
        ShardPaginationStrategy strategy = null;
        for (int page = 1; page < totalPages; page++) {
            PageParams pageParams = new PageParams(requestedToken, PARAM_ASC_SORT_VALUE, pageSize);
            strategy = new ShardPaginationStrategy(pageParams, clusterState);
            requestedToken = strategy.getResponseToken().getNextToken();
        }
        // The last page was supposed to fetch the remaining two indices for test-index-3
        assertEquals(8, strategy.getRequestedEntities().size());
        assertEquals(1, strategy.getRequestedIndices().size());
        assertEquals(TEST_INDEX_PREFIX + 3, strategy.getRequestedIndices().get(0));

        // Delete the index and re-create
        clusterState = deleteIndexFromClusterState(clusterState, 3);
        clusterState = addIndexToClusterState(
            clusterState,
            3,
            numShards,
            numReplicas,
            Instant.now().plus(4, ChronoUnit.SECONDS).toEpochMilli()
        );

        // since test-index-3 should now be considered a new index, all shards for it should appear
        PageParams pageParams = new PageParams(requestedToken, PARAM_ASC_SORT_VALUE, pageSize);
        strategy = new ShardPaginationStrategy(pageParams, clusterState);
        assertEquals(8, strategy.getRequestedEntities().size());
        for (ShardRouting shardRouting : strategy.getRequestedEntities()) {
            assertTrue(
                (shardRouting.getId() == 0 || shardRouting.getId() == 1)
                    && Objects.equals(shardRouting.getIndexName(), TEST_INDEX_PREFIX + 3)
            );
        }
        assertEquals(1, strategy.getRequestedIndices().size());
        assertEquals(TEST_INDEX_PREFIX + 3, strategy.getRequestedIndices().get(0));
        assertNotNull(strategy.getResponseToken().getNextToken());

        requestedToken = strategy.getResponseToken().getNextToken();
        pageParams = new PageParams(requestedToken, PARAM_ASC_SORT_VALUE, pageSize);
        strategy = new ShardPaginationStrategy(pageParams, clusterState);
        assertEquals(4, strategy.getRequestedEntities().size());
        for (ShardRouting shardRouting : strategy.getRequestedEntities()) {
            assertTrue(shardRouting.getId() == 2 && Objects.equals(shardRouting.getIndexName(), TEST_INDEX_PREFIX + 3));
        }
        assertEquals(1, strategy.getRequestedIndices().size());
        assertEquals(TEST_INDEX_PREFIX + 3, strategy.getRequestedIndices().get(0));
        assertNull(strategy.getResponseToken().getNextToken());
    }

    public void testCreatingShardStrategyPageTokenWithRequestedTokenNull() {
        try {
            new ShardPaginationStrategy.ShardStrategyToken(null);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("requestedTokenString can not be null");
        }
    }

    public void testIndexStrategyPageTokenWithWronglyEncryptedRequestToken() {
        assertThrows(OpenSearchParseException.class, () -> new ShardPaginationStrategy.ShardStrategyToken("3%4%5"));
    }

    public void testIndexStrategyPageTokenWithIncorrectNumberOfElementsInRequestedToken() {
        assertThrows(
            OpenSearchParseException.class,
            () -> new ShardPaginationStrategy.ShardStrategyToken(PaginationStrategy.encryptStringToken("1|1725361543"))
        );
        assertThrows(
            OpenSearchParseException.class,
            () -> new ShardPaginationStrategy.ShardStrategyToken(PaginationStrategy.encryptStringToken("1|1725361543|index|12345"))
        );
    }

    public void testIndexStrategyPageTokenWithInvalidValuesInRequestedToken() {
        assertThrows(
            OpenSearchParseException.class,
            () -> new ShardPaginationStrategy.ShardStrategyToken(PaginationStrategy.encryptStringToken("-1725361543|1725361543|index"))
        );
    }

    public void testCreatingIndexStrategyPageTokenWithNameOfLastRespondedIndexNull() {
        try {
            new ShardPaginationStrategy.ShardStrategyToken(null, 0, 1234l);
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains("index name should be provided");
        }
    }

    public void testCreatingIndexStrategyPageTokenWithNonParseableShardID() {
        try {
            new ShardPaginationStrategy.ShardStrategyToken(PaginationStrategy.encryptStringToken("shardID|1725361543|index"));
            fail("expected exception");
        } catch (Exception e) {
            assert e.getMessage().contains(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
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
            clusterState = addIndexToClusterState(clusterState, indexNumber, DEFAULT_NUMBER_OF_SHARDS, DEFAULT_NUMBER_OF_REPLICAS);
        }
        return clusterState;
    }

    private ClusterState addIndexToClusterState(
        ClusterState clusterState,
        final int indexNumber,
        final int numShards,
        final int numReplicas
    ) {
        return addIndexToClusterState(
            clusterState,
            indexNumber,
            numShards,
            numReplicas,
            Instant.now().plus(indexNumber, ChronoUnit.SECONDS).toEpochMilli()
        );
    }

    private ClusterState addIndexToClusterState(
        ClusterState clusterState,
        final int indexNumber,
        final int numShards,
        final int numReplicas,
        final long creationTime
    ) {
        IndexMetadata indexMetadata = IndexMetadata.builder(TEST_INDEX_PREFIX + indexNumber)
            .settings(settings(Version.CURRENT).put(SETTING_CREATION_DATE, creationTime))
            .numberOfShards(numShards)
            .numberOfReplicas(numReplicas)
            .build();
        IndexRoutingTable.Builder indexRoutingTableBuilder = new IndexRoutingTable.Builder(indexMetadata.getIndex()).initializeAsNew(
            indexMetadata
        );
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder(clusterState.routingTable()).add(indexRoutingTableBuilder).build())
            .build();
    }

    private ClusterState deleteIndexFromClusterState(ClusterState clusterState, int indexNumber) {
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).remove(TEST_INDEX_PREFIX + indexNumber))
            .routingTable(RoutingTable.builder(clusterState.routingTable()).remove(TEST_INDEX_PREFIX + indexNumber).build())
            .build();
    }

    private void verifyOrderForAllShards(ClusterState clusterState, List<ShardRouting> shardRoutings, String sortOrder) {
        ShardRouting prevShard = shardRoutings.get(0);
        for (ShardRouting shard : shardRoutings.subList(1, shardRoutings.size())) {
            if (Objects.equals(shard.getIndexName(), prevShard.getIndexName())) {
                assertTrue(shard.getId() >= prevShard.getId());
            } else {
                if (sortOrder.equals(PARAM_ASC_SORT_VALUE)) {
                    assertTrue(
                        clusterState.metadata().index(shard.getIndexName()).getCreationDate() > clusterState.metadata()
                            .index(prevShard.getIndexName())
                            .getCreationDate()
                    );
                } else {
                    assertTrue(
                        clusterState.metadata().index(shard.getIndexName()).getCreationDate() < clusterState.metadata()
                            .index(prevShard.getIndexName())
                            .getCreationDate()
                    );
                }
                prevShard = shard;
            }
        }
    }

}

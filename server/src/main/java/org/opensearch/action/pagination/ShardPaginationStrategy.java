/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.opensearch.action.pagination.IndexPaginationStrategy.ASC_COMPARATOR;
import static org.opensearch.action.pagination.IndexPaginationStrategy.DESC_COMPARATOR;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Shards.
 * The strategy considers create timestamps of indices and shardID as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class ShardPaginationStrategy implements PaginationStrategy<ShardRouting> {

    private static final String DEFAULT_SHARDS_PAGINATED_ENTITY = "shards";

    private PageData pageData;

    public ShardPaginationStrategy(PageParams pageParams, ClusterState clusterState) {
        ShardStrategyToken shardStrategyToken = getShardStrategyToken(pageParams.getRequestedToken());
        // Get list of indices metadata sorted by their creation time and filtered by the last sent index
        List<IndexMetadata> filteredIndices = getEligibleIndices(
            clusterState,
            pageParams.getSort(),
            Objects.isNull(shardStrategyToken) ? null : shardStrategyToken.lastIndexName,
            Objects.isNull(shardStrategyToken) ? null : shardStrategyToken.lastIndexCreationTime
        );
        // Get the list of shards and indices belonging to current page.
        this.pageData = getPageData(
            filteredIndices,
            clusterState.getRoutingTable().getIndicesRouting(),
            shardStrategyToken,
            pageParams.getSize()
        );
    }

    private static List<IndexMetadata> getEligibleIndices(
        ClusterState clusterState,
        String sortOrder,
        String lastIndexName,
        Long lastIndexCreationTime
    ) {
        if (Objects.isNull(lastIndexName) || Objects.isNull(lastIndexCreationTime)) {
            return PaginationStrategy.getSortedIndexMetadata(
                clusterState,
                PageParams.PARAM_ASC_SORT_VALUE.equals(sortOrder) ? ASC_COMPARATOR : DESC_COMPARATOR
            );
        } else {
            return PaginationStrategy.getSortedIndexMetadata(
                clusterState,
                getMetadataFilter(sortOrder, lastIndexName, lastIndexCreationTime),
                PageParams.PARAM_ASC_SORT_VALUE.equals(sortOrder) ? ASC_COMPARATOR : DESC_COMPARATOR
            );
        }
    }

    private static Predicate<IndexMetadata> getMetadataFilter(String sortOrder, String lastIndexName, Long lastIndexCreationTime) {
        if (Objects.isNull(lastIndexName) || Objects.isNull(lastIndexCreationTime)) {
            return indexMetadata -> true;
        }
        return indexNameFilter(lastIndexName).or(
            IndexPaginationStrategy.getIndexCreateTimeFilter(sortOrder, lastIndexName, lastIndexCreationTime)
        );
    }

    private static Predicate<IndexMetadata> indexNameFilter(String lastIndexName) {
        return metadata -> metadata.getIndex().getName().equals(lastIndexName);
    }

    /**
     * Will be used to get the list of shards and respective indices to which they belong,
     * which are to be displayed in a page.
     * Note: All shards for a shardID will always be present in the same page.
     */
    private PageData getPageData(
        List<IndexMetadata> filteredIndices,
        Map<String, IndexRoutingTable> indicesRouting,
        final ShardStrategyToken token,
        final int numShardsRequired
    ) {
        List<ShardRouting> shardRoutings = new ArrayList<>();
        List<String> indices = new ArrayList<>();
        int shardCount = 0;
        IndexMetadata lastAddedIndex = null;

        // iterate over indices until shardCount is less than numShardsRequired
        for (IndexMetadata indexMetadata : filteredIndices) {
            String indexName = indexMetadata.getIndex().getName();
            boolean indexShardsAdded = false;
            // Always start from shardID 0 for all indices except for the first one which might be same as the last sent
            // index. To identify if an index is same as last sent index, verify both the index name and creaton time.
            int startShardId = shardCount == 0 ? getStartShardIdForPageIndex(token, indexName, indexMetadata.getCreationDate()) : 0;
            Map<Integer, IndexShardRoutingTable> indexShardRoutingTable = indicesRouting.get(indexName).getShards();
            for (; startShardId < indexShardRoutingTable.size(); startShardId++) {
                if (indexShardRoutingTable.get(startShardId).size() > numShardsRequired) {
                    throw new IllegalArgumentException("size value should be greater than the replica count of all indices");
                }
                shardCount += indexShardRoutingTable.get(startShardId).size();
                if (shardCount > numShardsRequired) {
                    break;
                }
                shardRoutings.addAll(indexShardRoutingTable.get(startShardId).shards());
                indexShardsAdded = true;
            }

            if (indexShardsAdded) {
                lastAddedIndex = indexMetadata;
                indices.add(indexName);
            }

            if (shardCount > numShardsRequired) {
                return new PageData(
                    shardRoutings,
                    indices,
                    new PageToken(
                        new ShardStrategyToken(
                            lastAddedIndex.getIndex().getName(),
                            shardRoutings.get(shardRoutings.size() - 1).id(),
                            lastAddedIndex.getCreationDate()
                        ).generateEncryptedToken(),
                        DEFAULT_SHARDS_PAGINATED_ENTITY
                    )
                );
            }
        }

        // If all the shards of filtered indices list have been included in pageShardRoutings, then no more
        // shards are remaining to be fetched, and the next_token should thus be null.
        return new PageData(shardRoutings, indices, new PageToken(null, DEFAULT_SHARDS_PAGINATED_ENTITY));
    }

    private ShardStrategyToken getShardStrategyToken(String requestedToken) {
        return Objects.isNull(requestedToken) || requestedToken.isEmpty() ? null : new ShardStrategyToken(requestedToken);
    }

    /**
     * Provides the shardId to start an index which is to be included in the page. If the index is same as
     * lastIndex, start from the shardId next to lastShardId, else always start from 0.
     */
    private int getStartShardIdForPageIndex(ShardStrategyToken token, final String pageIndexName, final long pageIndexCreationTime) {
        return Objects.isNull(token) ? 0
            : token.lastIndexName.equals(pageIndexName) && token.lastIndexCreationTime == pageIndexCreationTime ? token.lastShardId + 1
            : 0;
    }

    @Override
    public PageToken getResponseToken() {
        return pageData.pageToken;
    }

    @Override
    public List<ShardRouting> getRequestedEntities() {
        return pageData.shardRoutings;
    }

    public List<String> getRequestedIndices() {
        return pageData.indices;
    }

    /**
     * TokenParser to be used by {@link ShardPaginationStrategy}.
     * TToken would look like: LastShardIdOfPage + | + CreationTimeOfLastRespondedIndex + | + NameOfLastRespondedIndex
     */
    public static class ShardStrategyToken {

        private static final String JOIN_DELIMITER = "|";
        private static final String SPLIT_REGEX = "\\|";
        private static final int SHARD_ID_POS_IN_TOKEN = 0;
        private static final int CREATE_TIME_POS_IN_TOKEN = 1;
        private static final int INDEX_NAME_POS_IN_TOKEN = 2;

        /**
         * Denotes the shardId of the last shard in the response.
         * Will be used to identify the next shard to start the page from, in case the shards of an index
         * get split across pages.
         */
        private final int lastShardId;
        /**
         * Represents creation times of last index which was displayed in the page.
         * Used to identify the new start point in case the indices get created/deleted while queries are executed.
         */
        private final long lastIndexCreationTime;

        /**
         * Represents name of the last index which was displayed in the page.
         * Used to identify whether the sorted list of indices has changed or not.
         */
        private final String lastIndexName;

        public ShardStrategyToken(String requestedTokenString) {
            validateShardStrategyToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split(SPLIT_REGEX);
            this.lastShardId = Integer.parseInt(decryptedTokenElements[SHARD_ID_POS_IN_TOKEN]);
            this.lastIndexCreationTime = Long.parseLong(decryptedTokenElements[CREATE_TIME_POS_IN_TOKEN]);
            this.lastIndexName = decryptedTokenElements[INDEX_NAME_POS_IN_TOKEN];
        }

        public ShardStrategyToken(String lastIndexName, int lastShardId, long lastIndexCreationTime) {
            Objects.requireNonNull(lastIndexName, "index name should be provided");
            this.lastIndexName = lastIndexName;
            this.lastShardId = lastShardId;
            this.lastIndexCreationTime = lastIndexCreationTime;
        }

        public String generateEncryptedToken() {
            return PaginationStrategy.encryptStringToken(
                String.join(JOIN_DELIMITER, String.valueOf(lastShardId), String.valueOf(lastIndexCreationTime), lastIndexName)
            );
        }

        /**
         * Will perform simple validations on token received in the request.
         * Token should be base64 encoded, and should contain the expected number of elements separated by "|".
         * Timestamps should also be a valid long.
         *
         * @param requestedTokenStr string denoting the encoded token requested by the user.
         */
        public static void validateShardStrategyToken(String requestedTokenStr) {
            Objects.requireNonNull(requestedTokenStr, "requestedTokenString can not be null");
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenStr);
            final String[] decryptedTokenElements = decryptedToken.split(SPLIT_REGEX);
            if (decryptedTokenElements.length != 3) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                int shardId = Integer.parseInt(decryptedTokenElements[SHARD_ID_POS_IN_TOKEN]);
                long creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[CREATE_TIME_POS_IN_TOKEN]);
                if (shardId < 0 || creationTimeOfLastRespondedIndex <= 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }
    }

    /**
     * Private utility class to consolidate the page details of shard strategy. Will be used to set all the details at once,
     * to avoid parsing collections multiple times.
     */
    private static class PageData {
        private final List<ShardRouting> shardRoutings;
        private final List<String> indices;
        private final PageToken pageToken;

        PageData(List<ShardRouting> shardRoutings, List<String> indices, PageToken pageToken) {
            this.shardRoutings = shardRoutings;
            this.indices = indices;
            this.pageToken = pageToken;
        }
    }
}

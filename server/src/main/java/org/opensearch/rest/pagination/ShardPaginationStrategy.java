/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.rest.pagination.PageParams.PARAM_ASC_SORT_VALUE;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Shards.
 * The strategy considers create timestamps of indices and shardID as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class ShardPaginationStrategy implements PaginationStrategy<ShardRouting> {

    private static final String DEFAULT_SHARDS_PAGINATED_ENTITY = "shards";
    private static final Comparator<IndexMetadata> ASC_COMPARATOR = (metadata1, metadata2) -> {
        if (metadata1.getCreationDate() == metadata2.getCreationDate()) {
            return metadata1.getIndex().getName().compareTo(metadata2.getIndex().getName());
        }
        return Long.compare(metadata1.getCreationDate(), metadata2.getCreationDate());
    };
    private static final Comparator<IndexMetadata> DESC_COMPARATOR = (metadata1, metadata2) -> {
        if (metadata1.getCreationDate() == metadata2.getCreationDate()) {
            return metadata2.getIndex().getName().compareTo(metadata1.getIndex().getName());
        }
        return Long.compare(metadata2.getCreationDate(), metadata1.getCreationDate());
    };

    private PageToken pageToken;
    private List<ShardRouting> pageShardRoutings = new ArrayList<>();
    private List<String> pageIndices = new ArrayList<>();

    public ShardPaginationStrategy(PageParams pageParams, ClusterState clusterState) {
        ShardStrategyToken shardStrategyToken = getShardStrategyToken(pageParams.getRequestedToken());
        // Get list of indices metadata sorted by their creation time and filtered by the last sent index
        List<IndexMetadata> filteredIndices = PaginationStrategy.getSortedIndexMetadata(
            clusterState,
            getIndexFilter(shardStrategyToken, pageParams.getSort()),
            PARAM_ASC_SORT_VALUE.equals(pageParams.getSort()) ? ASC_COMPARATOR : DESC_COMPARATOR
        );
        // Get the list of shards and indices belonging to current page.
        Tuple<List<ShardRouting>, List<IndexMetadata>> tuple = getPageData(
            clusterState.getRoutingTable().getIndicesRouting(),
            filteredIndices,
            shardStrategyToken,
            pageParams.getSize()
        );
        List<ShardRouting> pageShardRoutings = tuple.v1();
        List<IndexMetadata> pageIndices = tuple.v2();
        this.pageShardRoutings = pageShardRoutings;
        // Get list of index names from the trimmed metadataSublist
        this.pageIndices = pageIndices.stream().map(metadata -> metadata.getIndex().getName()).collect(Collectors.toList());
        this.pageToken = getResponseToken(
            pageIndices.isEmpty() ? null : pageIndices.get(pageIndices.size() - 1),
            filteredIndices.isEmpty() ? null : filteredIndices.get(filteredIndices.size() - 1).getIndex().getName(),
            pageShardRoutings.isEmpty() ? -1 : pageShardRoutings.get(pageShardRoutings.size() - 1).id()
        );
    }

    private static Predicate<IndexMetadata> getIndexFilter(ShardStrategyToken token, String sortOrder) {
        if (Objects.isNull(token)) {
            return indexMetadata -> true;
        }
        boolean isAscendingSort = sortOrder.equals(PARAM_ASC_SORT_VALUE);
        return metadata -> {
            if (metadata.getIndex().getName().equals(token.lastIndexName)) {
                return true;
            } else if (metadata.getCreationDate() == token.lastIndexCreationTime) {
                return isAscendingSort
                    ? metadata.getIndex().getName().compareTo(token.lastIndexName) > 0
                    : metadata.getIndex().getName().compareTo(token.lastIndexName) < 0;
            }
            return isAscendingSort
                ? metadata.getCreationDate() > token.lastIndexCreationTime
                : metadata.getCreationDate() < token.lastIndexCreationTime;
        };
    }

    /**
     * Will be used to get the list of shards and respective indices to which they belong,
     * which are to be displayed in a page.
     * Note: All shards for a shardID will always be present in the same page.
     */
    private Tuple<List<ShardRouting>, List<IndexMetadata>> getPageData(
        Map<String, IndexRoutingTable> indicesRouting,
        List<IndexMetadata> filteredIndices,
        final ShardStrategyToken token,
        final int numShardsRequired
    ) {
        List<ShardRouting> shardRoutings = new ArrayList<>();
        List<IndexMetadata> indexMetadataList = new ArrayList<>();
        int shardCount = 0;

        // iterate over indices until shardCount is less than numShardsRequired
        for (IndexMetadata indexMetadata : filteredIndices) {
            String indexName = indexMetadata.getIndex().getName();
            int startShardId = getStartShardIdForPageIndex(token, indexName);
            boolean indexShardsAdded = false;
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
            // Add index to the list if any of its shard was added to the count.
            if (indexShardsAdded) {
                indexMetadataList.add(indexMetadata);
            }
            if (shardCount >= numShardsRequired) {
                break;
            }
        }

        return new Tuple<>(shardRoutings, indexMetadataList);
    }

    private PageToken getResponseToken(IndexMetadata pageEndIndex, final String lastFilteredIndexName, final int pageEndShardId) {
        // If all the shards of filtered indices list have been included in pageShardRoutings, then no more
        // shards are remaining to be fetched, and the next_token should thus be null.
        String pageEndIndexName = Objects.isNull(pageEndIndex) ? null : pageEndIndex.getIndex().getName();
        if (Objects.isNull(pageEndIndexName)
            || (pageEndIndexName.equals(lastFilteredIndexName) && pageEndIndex.getNumberOfShards() == pageEndShardId + 1)) {
            return new PageToken(null, DEFAULT_SHARDS_PAGINATED_ENTITY);
        }
        return new PageToken(
            new ShardStrategyToken(pageEndIndexName, pageEndShardId, pageEndIndex.getCreationDate()).generateEncryptedToken(),
            DEFAULT_SHARDS_PAGINATED_ENTITY
        );
    }

    private ShardStrategyToken getShardStrategyToken(String requestedToken) {
        return Objects.isNull(requestedToken) || requestedToken.isEmpty() ? null : new ShardStrategyToken(requestedToken);
    }

    /**
     * Provides the shardId to start an index which is to be included in the page. If the index is same as
     * lastIndex, start from the shardId next to lastShardId, else always start from 0.
     */
    private int getStartShardIdForPageIndex(ShardStrategyToken token, final String pageIndexName) {
        return Objects.isNull(token) ? 0 : token.lastIndexName.equals(pageIndexName) ? token.lastShardId + 1 : 0;
    }

    @Override
    public PageToken getResponseToken() {
        return pageToken;
    }

    @Override
    public List<ShardRouting> getRequestedEntities() {
        return pageShardRoutings;
    }

    public List<String> getRequestedIndices() {
        return pageIndices;
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
}

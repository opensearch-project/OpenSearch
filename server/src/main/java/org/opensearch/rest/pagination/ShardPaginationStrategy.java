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
    private List<ShardRouting> requestedShardRoutings = new ArrayList<>();
    private List<String> requestedIndices = new ArrayList<>();

    public ShardPaginationStrategy(PageParams pageParams, ClusterState clusterState) {
        // Get list of indices metadata sorted by their creation time and filtered by the last sent index
        List<IndexMetadata> sortedIndices = PaginationStrategy.getSortedIndexMetadata(
            clusterState,
            getMetadataFilter(pageParams.getRequestedToken(), pageParams.getSort()),
            PARAM_ASC_SORT_VALUE.equals(pageParams.getSort()) ? ASC_COMPARATOR : DESC_COMPARATOR
        );
        // Get the list of shards and indices belonging to current page.
        Tuple<List<ShardRouting>, List<IndexMetadata>> tuple = getPageData(
            clusterState.getRoutingTable().getIndicesRouting(),
            sortedIndices,
            pageParams.getSize(),
            pageParams.getRequestedToken()
        );
        this.requestedShardRoutings = tuple.v1();
        List<IndexMetadata> metadataSublist = tuple.v2();
        // Get list of index names from the trimmed metadataSublist
        this.requestedIndices = metadataSublist.stream().map(metadata -> metadata.getIndex().getName()).collect(Collectors.toList());
        this.pageToken = getResponseToken(
            pageParams.getSize(),
            sortedIndices.size(),
            metadataSublist.isEmpty() ? null : metadataSublist.get(metadataSublist.size() - 1),
            tuple.v1().isEmpty() ? null : tuple.v1().get(tuple.v1().size() - 1)
        );
    }

    private static Predicate<IndexMetadata> getMetadataFilter(String requestedTokenStr, String sortOrder) {
        boolean isAscendingSort = sortOrder.equals(PARAM_ASC_SORT_VALUE);
        ShardStrategyToken requestedToken = Objects.isNull(requestedTokenStr) || requestedTokenStr.isEmpty()
            ? null
            : new ShardStrategyToken(requestedTokenStr);
        if (Objects.isNull(requestedToken)) {
            return indexMetadata -> true;
        }
        return metadata -> {
            if (metadata.getIndex().getName().equals(requestedToken.lastIndexName)) {
                return true;
            } else if (metadata.getCreationDate() == requestedToken.lastIndexCreationTime) {
                return isAscendingSort
                    ? metadata.getIndex().getName().compareTo(requestedToken.lastIndexName) > 0
                    : metadata.getIndex().getName().compareTo(requestedToken.lastIndexName) < 0;
            }
            return isAscendingSort
                ? metadata.getCreationDate() > requestedToken.lastIndexCreationTime
                : metadata.getCreationDate() < requestedToken.lastIndexCreationTime;
        };
    }

    private Tuple<List<ShardRouting>, List<IndexMetadata>> getPageData(
        Map<String, IndexRoutingTable> indicesRouting,
        List<IndexMetadata> sortedIndices,
        final int pageSize,
        String requestedTokenStr
    ) {
        List<ShardRouting> shardRoutings = new ArrayList<>();
        List<IndexMetadata> indexMetadataList = new ArrayList<>();
        ShardStrategyToken requestedToken = Objects.isNull(requestedTokenStr) || requestedTokenStr.isEmpty()
            ? null
            : new ShardStrategyToken(requestedTokenStr);
        int shardCount = 0;
        for (IndexMetadata indexMetadata : sortedIndices) {
            boolean indexShardsAdded = false;
            Map<Integer, IndexShardRoutingTable> indexShardRoutingTable = indicesRouting.get(indexMetadata.getIndex().getName())
                .getShards();
            int shardId = Objects.isNull(requestedToken) ? 0
                : indexMetadata.getIndex().getName().equals(requestedToken.lastIndexName) ? requestedToken.lastShardId + 1
                : 0;
            for (; shardId < indexShardRoutingTable.size(); shardId++) {
                shardCount += indexShardRoutingTable.get(shardId).size();
                if (shardCount > pageSize) {
                    break;
                }
                shardRoutings.addAll(indexShardRoutingTable.get(shardId).shards());
                indexShardsAdded = true;
            }
            // Add index to the list if any of its shard was added to the count.
            if (indexShardsAdded) {
                indexMetadataList.add(indexMetadata);
            }
            if (shardCount >= pageSize) {
                break;
            }
        }

        return new Tuple<>(shardRoutings, indexMetadataList);
    }

    private PageToken getResponseToken(final int pageSize, final int totalIndices, IndexMetadata lastIndex, ShardRouting lastShard) {
        if (totalIndices <= pageSize && lastIndex.getNumberOfShards() == lastShard.getId()) {
            return new PageToken(null, DEFAULT_SHARDS_PAGINATED_ENTITY);
        }
        return new PageToken(
            new ShardStrategyToken(lastShard.getId(), lastIndex.getCreationDate(), lastIndex.getIndex().getName()).generateEncryptedToken(),
            DEFAULT_SHARDS_PAGINATED_ENTITY
        );
    }

    @Override
    public PageToken getResponseToken() {
        return pageToken;
    }

    @Override
    public List<ShardRouting> getRequestedEntities() {
        return requestedShardRoutings;
    }

    public List<String> getRequestedIndices() {
        return requestedIndices;
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

        public ShardStrategyToken(int lastShardId, long creationTimeOfLastRespondedIndex, String nameOfLastRespondedIndex) {
            this.lastShardId = lastShardId;
            Objects.requireNonNull(nameOfLastRespondedIndex, "index name should be provided");
            this.lastIndexCreationTime = creationTimeOfLastRespondedIndex;
            this.lastIndexName = nameOfLastRespondedIndex;
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

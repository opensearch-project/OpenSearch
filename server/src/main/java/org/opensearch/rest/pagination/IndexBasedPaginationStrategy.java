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
import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.rest.pagination.PaginatedQueryRequest.PAGINATED_QUERY_ASCENDING_SORT;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Indices.
 * The strategy considers create timestamps of indices as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class IndexBasedPaginationStrategy implements PaginationStrategy<String> {

    private final PaginatedQueryResponse paginatedQueryResponse;
    private final List<String> indicesFromRequestedToken;

    private static final String DEFAULT_INDICES_PAGINATED_ELEMENT = "indices";

    public IndexBasedPaginationStrategy(PaginatedQueryRequest paginatedQueryRequest, ClusterState clusterState) {
        // Get list of indices metadata sorted by their creation time and filtered by the last send index
        List<IndexMetadata> sortedIndicesList = PaginationStrategy.getSortedIndexMetadata(
            clusterState,
            getMetadataListFilter(paginatedQueryRequest.getRequestedTokenStr(), paginatedQueryRequest.getSort()),
            getMetadataListComparator(paginatedQueryRequest.getSort())
        );
        List<IndexMetadata> metadataListForRequestedToken = getMetadataListForRequestedToken(sortedIndicesList, paginatedQueryRequest);
        this.indicesFromRequestedToken = metadataListForRequestedToken.stream()
            .map(metadata -> metadata.getIndex().getName())
            .collect(Collectors.toList());
        this.paginatedQueryResponse = getPaginatedResponseForRequestedToken(paginatedQueryRequest.getSize(), sortedIndicesList);
    }

    private static Predicate<IndexMetadata> getMetadataListFilter(String requestedTokenStr, String sortOrder) {
        boolean isAscendingSort = sortOrder.equals(PAGINATED_QUERY_ASCENDING_SORT);
        IndexStrategyToken requestedToken = Objects.isNull(requestedTokenStr) || requestedTokenStr.isEmpty()
            ? null
            : new IndexStrategyToken(requestedTokenStr);
        if (Objects.isNull(requestedToken)) {
            return indexMetadata -> true;
        }
        return indexMetadata -> {
            if (indexMetadata.getIndex().getName().equals(requestedToken.nameOfLastRespondedIndex)) {
                return false;
            } else if (indexMetadata.getCreationDate() == requestedToken.creationTimeOfLastRespondedIndex) {
                return isAscendingSort
                    ? indexMetadata.getIndex().getName().compareTo(requestedToken.nameOfLastRespondedIndex) > 0
                    : indexMetadata.getIndex().getName().compareTo(requestedToken.nameOfLastRespondedIndex) < 0;
            }
            return isAscendingSort
                ? indexMetadata.getCreationDate() > requestedToken.creationTimeOfLastRespondedIndex
                : indexMetadata.getCreationDate() < requestedToken.creationTimeOfLastRespondedIndex;
        };
    }

    private static Comparator<IndexMetadata> getMetadataListComparator(String sortOrder) {
        boolean isAscendingSort = sortOrder.equals(PAGINATED_QUERY_ASCENDING_SORT);
        return (metadata1, metadata2) -> {
            if (metadata1.getCreationDate() == metadata2.getCreationDate()) {
                return isAscendingSort
                    ? metadata1.getIndex().getName().compareTo(metadata2.getIndex().getName())
                    : metadata2.getIndex().getName().compareTo(metadata1.getIndex().getName());
            }
            return isAscendingSort
                ? Long.compare(metadata1.getCreationDate(), metadata2.getCreationDate())
                : Long.compare(metadata2.getCreationDate(), metadata1.getCreationDate());
        };
    }

    private List<IndexMetadata> getMetadataListForRequestedToken(
        List<IndexMetadata> sortedIndicesList,
        PaginatedQueryRequest paginatedQueryRequest
    ) {
        if (sortedIndicesList.isEmpty()) {
            return new ArrayList<>();
        }
        return sortedIndicesList.subList(0, Math.min(paginatedQueryRequest.getSize(), sortedIndicesList.size()));
    }

    private PaginatedQueryResponse getPaginatedResponseForRequestedToken(int pageSize, List<IndexMetadata> sortedIndicesList) {
        if (sortedIndicesList.size() <= pageSize) {
            return new PaginatedQueryResponse(null, DEFAULT_INDICES_PAGINATED_ELEMENT);
        }
        return new PaginatedQueryResponse(
            new IndexStrategyToken(
                sortedIndicesList.get(pageSize - 1).getCreationDate(),
                sortedIndicesList.get(pageSize - 1).getIndex().getName()
            ).generateEncryptedToken(),
            DEFAULT_INDICES_PAGINATED_ELEMENT
        );
    }

    @Override
    @Nullable
    public PaginatedQueryResponse getPaginatedQueryResponse() {
        return paginatedQueryResponse;
    }

    @Override
    public List<String> getElementsFromRequestedToken() {
        return Objects.isNull(indicesFromRequestedToken) ? new ArrayList<>() : indicesFromRequestedToken;
    }

    /**
     * TokenParser to be used by {@link IndexBasedPaginationStrategy}.
     * Token would like: IndexNumberToStartTheNextPageFrom + | + CreationTimeOfLastRespondedIndex + | +
     * QueryStartTime + | + NameOfLastRespondedIndex
     */
    public static class IndexStrategyToken {

        private static final String TOKEN_JOIN_DELIMITER = "|";
        private static final String TOKEN_SPLIT_REGEX = "\\|";
        private static final int CREATION_TIME_FIELD_POSITION_IN_TOKEN = 0;
        private static final int INDEX_NAME_FIELD_POSITION_IN_TOKEN = 1;

        /**
         * Represents creation times of last index which was displayed in the previous page.
         * Used to identify the new start point in case the indices get created/deleted while queries are executed.
         */
        private final long creationTimeOfLastRespondedIndex;

        /**
         * Represents name of the last index which was displayed in the previous page.
         * Used to identify whether the sorted list of indices has changed or not.
         */
        private final String nameOfLastRespondedIndex;

        public IndexStrategyToken(String requestedTokenString) {
            validateIndexStrategyToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split(TOKEN_SPLIT_REGEX);
            this.creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[CREATION_TIME_FIELD_POSITION_IN_TOKEN]);
            this.nameOfLastRespondedIndex = decryptedTokenElements[INDEX_NAME_FIELD_POSITION_IN_TOKEN];
        }

        public IndexStrategyToken(long creationTimeOfLastRespondedIndex, String nameOfLastRespondedIndex) {
            Objects.requireNonNull(nameOfLastRespondedIndex, "index name should be provided");
            this.creationTimeOfLastRespondedIndex = creationTimeOfLastRespondedIndex;
            this.nameOfLastRespondedIndex = nameOfLastRespondedIndex;
        }

        public String generateEncryptedToken() {
            return PaginationStrategy.encryptStringToken(
                String.join(TOKEN_JOIN_DELIMITER, String.valueOf(creationTimeOfLastRespondedIndex), nameOfLastRespondedIndex)
            );
        }

        /**
         * Will perform simple validations on token received in the request and initialize the data members.
         * The token should be base64 encoded, and should contain the expected number of elements separated by "$".
         * The timestamps should also be a valid long.
         *
         * @param requestedTokenString string denoting the encoded next token requested by the user
         */
        public static void validateIndexStrategyToken(String requestedTokenString) {
            Objects.requireNonNull(requestedTokenString, "requestedTokenString can not be null");
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split(TOKEN_SPLIT_REGEX);
            if (decryptedTokenElements.length != 2) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                long creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[CREATION_TIME_FIELD_POSITION_IN_TOKEN]);
                if (creationTimeOfLastRespondedIndex <= 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }
    }

}

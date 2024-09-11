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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Indices.
 * The strategy considers create timestamps of indices as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class IndexBasedPaginationStrategy implements PaginationStrategy<String> {

    private final PaginatedQueryResponse paginatedQueryResponse;
    private final List<String> indicesFromRequestedToken;

    public IndexBasedPaginationStrategy(PaginatedQueryRequest paginatedQueryRequest, String paginatedElement, ClusterState clusterState) {
        // Get list of indices metadata sorted by their creation time
        List<IndexMetadata> sortedIndicesList = PaginationStrategy.getListOfIndicesSortedByCreateTime(
            clusterState,
            paginatedQueryRequest.getSort()
        );
        this.indicesFromRequestedToken = getIndicesFromRequestedToken(sortedIndicesList, paginatedQueryRequest);
        this.paginatedQueryResponse = getPaginatedResponseFromRequestedToken(sortedIndicesList, paginatedQueryRequest, paginatedElement);
    }

    private List<String> getIndicesFromRequestedToken(List<IndexMetadata> sortedIndicesList, PaginatedQueryRequest paginatedQueryRequest) {
        if (sortedIndicesList.isEmpty()) {
            return new ArrayList<>();
        }
        final int requestedPageStartIndexNumber = getRequestedPageIndexStartNumber(paginatedQueryRequest, sortedIndicesList); // inclusive
        int requestedPageEndIndexNumber = Math.min(
            requestedPageStartIndexNumber + paginatedQueryRequest.getSize(),
            sortedIndicesList.size()
        ); // exclusive
        return sortedIndicesList.subList(requestedPageStartIndexNumber, requestedPageEndIndexNumber)
            .stream()
            .map(indexMetadata -> indexMetadata.getIndex().getName())
            .collect(Collectors.toList());
    }

    private PaginatedQueryResponse getPaginatedResponseFromRequestedToken(
        List<IndexMetadata> sortedIndicesList,
        PaginatedQueryRequest paginatedQueryRequest,
        String paginatedElement
    ) {
        if (sortedIndicesList.isEmpty()) {
            return new PaginatedQueryResponse(null, paginatedElement);
        }
        int positionToStartNextPage = Math.min(
            getRequestedPageIndexStartNumber(paginatedQueryRequest, sortedIndicesList) + paginatedQueryRequest.getSize(),
            sortedIndicesList.size()
        );
        return new PaginatedQueryResponse(
            positionToStartNextPage >= sortedIndicesList.size()
                ? null
                : new IndexStrategyToken(
                    positionToStartNextPage,
                    sortedIndicesList.get(positionToStartNextPage - 1).getCreationDate(),
                    sortedIndicesList.get(positionToStartNextPage - 1).getIndex().getName()
                ).generateEncryptedToken(),
            paginatedElement
        );
    }

    private int getRequestedPageIndexStartNumber(PaginatedQueryRequest paginatedQueryRequest, List<IndexMetadata> sortedIndicesList) {
        if (Objects.isNull(paginatedQueryRequest.getRequestedTokenStr())) {
            // first paginated query, start from first index.
            return 0;
        }

        IndexStrategyToken requestedToken = new IndexStrategyToken(paginatedQueryRequest.getRequestedTokenStr());
        // If the already requested indices have been deleted, the position to start in the last token could be
        // greater than the sorted list's size, hence limiting it to current list's size.
        int requestedPageStartIndexNumber = Math.min(requestedToken.posToStartPage, sortedIndicesList.size());
        IndexMetadata currentIndexAtLastSentPosition = sortedIndicesList.get(requestedPageStartIndexNumber - 1);

        if (!Objects.equals(currentIndexAtLastSentPosition.getIndex().getName(), requestedToken.nameOfLastRespondedIndex)) {
            // case denoting already responded index/indices has/have been deleted/added in between the paginated queries.
            // find the index whose creation time is just after/before (based on sortOrder) the index which was last responded.
            if (!DESCENDING_SORT_PARAM_VALUE.equals(paginatedQueryRequest.getSort())) {
                // For ascending sort order, if indices were deleted, the index to start current page could only have
                // moved upwards (at a smaller position) in the sorted list. Traverse backwards to find such index
                while (requestedPageStartIndexNumber > 0
                    && (sortedIndicesList.get(requestedPageStartIndexNumber - 1)
                        .getCreationDate() > requestedToken.creationTimeOfLastRespondedIndex)) {
                    requestedPageStartIndexNumber--;
                }
            } else {
                // For descending order, there could be following 2 possibilities:
                // 1. Number of already responded indices which got deleted is greater than newly created ones.
                // -> The index to start the page from, would have shifted up in the list. Traverse backwards to find it.
                // 2. Number of indices which got created is greater than number of already responded indices which got deleted.
                // -> The index to start the page from, would have shifted down in the list. Traverse forward to find it.
                boolean traverseForward = currentIndexAtLastSentPosition
                    .getCreationDate() >= requestedToken.creationTimeOfLastRespondedIndex;
                if (traverseForward) {
                    while (requestedPageStartIndexNumber < sortedIndicesList.size()
                        && (sortedIndicesList.get(requestedPageStartIndexNumber - 1)
                            .getCreationDate() > requestedToken.creationTimeOfLastRespondedIndex)) {
                        requestedPageStartIndexNumber++;
                    }
                } else {
                    while (requestedPageStartIndexNumber > 0
                        && (sortedIndicesList.get(requestedPageStartIndexNumber - 1)
                            .getCreationDate() < requestedToken.creationTimeOfLastRespondedIndex)) {
                        requestedPageStartIndexNumber--;
                    }
                }
            }
        }
        return requestedPageStartIndexNumber;
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
     * Token would like: IndexNumberToStartTheNextPageFrom + $ + CreationTimeOfLastRespondedIndex + $ +
     * QueryStartTime + $ + NameOfLastRespondedIndex
     */
    public static class IndexStrategyToken {

        private static final String TOKEN_JOIN_DELIMITER = "|";
        private static final String TOKEN_SPLIT_REGEX = "\\|";
        private static final int START_PAGE_FIELD_POSITION_IN_TOKEN = 0;
        private static final int CREATION_TIME_FIELD_POSITION_IN_TOKEN = 1;

        /**
         * Denotes the position in the sorted list of indices to start building the page from.
         */
        private final int posToStartPage;

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
            this.posToStartPage = Integer.parseInt(decryptedTokenElements[START_PAGE_FIELD_POSITION_IN_TOKEN]);
            this.creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[CREATION_TIME_FIELD_POSITION_IN_TOKEN]);
            this.nameOfLastRespondedIndex = decryptedTokenElements[2];
        }

        public IndexStrategyToken(int indexNumberToStartPageFrom, long creationTimeOfLastRespondedIndex, String nameOfLastRespondedIndex) {
            Objects.requireNonNull(nameOfLastRespondedIndex, "index name should be provided");
            this.posToStartPage = indexNumberToStartPageFrom;
            this.creationTimeOfLastRespondedIndex = creationTimeOfLastRespondedIndex;
            this.nameOfLastRespondedIndex = nameOfLastRespondedIndex;
        }

        public String generateEncryptedToken() {
            return PaginationStrategy.encryptStringToken(
                String.join(
                    TOKEN_JOIN_DELIMITER,
                    String.valueOf(posToStartPage),
                    String.valueOf(creationTimeOfLastRespondedIndex),
                    nameOfLastRespondedIndex
                )
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
            if (decryptedTokenElements.length != 3) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                int posToStartPage = Integer.parseInt(decryptedTokenElements[0]);
                long creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[1]);
                if (posToStartPage <= 0 || creationTimeOfLastRespondedIndex <= 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }
    }

}

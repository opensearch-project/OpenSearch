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
import org.opensearch.common.Nullable;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This strategy can be used by the Rest APIs wanting to paginate the responses based on Indices.
 * The strategy considers create timestamps of indices as the keys to iterate over pages.
 *
 * @opensearch.internal
 */
public class IndexBasedPaginationStrategy implements PaginationStrategy<String> {

    private static final String DESCENDING_SORT_PARAM_VALUE = "descending";
    private final IndexStrategyPageToken nextToken;
    private final List<String> indicesFromRequestedToken;

    private static final String INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE =
        "Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token].";

    public IndexBasedPaginationStrategy(
        @Nullable IndexStrategyPageToken requestedToken,
        int maxPageSize,
        String sortOrder,
        ClusterState clusterState
    ) {
        // Get sorted list of indices from metadata and filter out the required number of indices
        List<String> sortedIndicesList = getListOfIndicesSortedByCreateTime(clusterState, sortOrder, requestedToken);
        final int newPageStartIndexNumber = getNewPageIndexStartNumber(requestedToken, sortedIndicesList, clusterState); // inclusive
        int newPageEndIndexNumber = Math.min(newPageStartIndexNumber + maxPageSize, sortedIndicesList.size()); // exclusive
        this.indicesFromRequestedToken = sortedIndicesList.subList(newPageStartIndexNumber, newPageEndIndexNumber);
        long queryStartTime = requestedToken == null
            ? clusterState.metadata().indices().get(sortedIndicesList.get(sortedIndicesList.size() - 1)).getCreationDate()
            : requestedToken.queryStartTime;
        IndexStrategyPageToken nextPageToken = new IndexStrategyPageToken(
            newPageEndIndexNumber,
            clusterState.metadata().indices().get(sortedIndicesList.get(newPageEndIndexNumber - 1)).getCreationDate(),
            queryStartTime,
            sortedIndicesList.get(newPageEndIndexNumber - 1)
        );
        this.nextToken = newPageEndIndexNumber >= sortedIndicesList.size() ? null : nextPageToken;
    }

    @Override
    @Nullable
    public PageToken getNextToken() {
        return nextToken;
    }

    @Override
    @Nullable
    public List<String> getElementsFromRequestedToken() {
        return indicesFromRequestedToken;
    }

    private List<String> getListOfIndicesSortedByCreateTime(
        final ClusterState clusterState,
        String sortOrder,
        IndexStrategyPageToken requestedPageToken
    ) {
        long latestValidIndexCreateTime = requestedPageToken == null ? Long.MAX_VALUE : requestedPageToken.queryStartTime;
        // Filter out the indices which have been created after the latest index which was present when paginated query started.
        // Also, sort the indices list based on their creation timestamps
        return clusterState.getRoutingTable()
            .getIndicesRouting()
            .keySet()
            .stream()
            .filter(index -> (latestValidIndexCreateTime - clusterState.metadata().indices().get(index).getCreationDate()) >= 0)
            .sorted((index1, index2) -> {
                Long index1CreationTimeStamp = clusterState.metadata().indices().get(index1).getCreationDate();
                Long index2CreationTimeStamp = clusterState.metadata().indices().get(index2).getCreationDate();
                if (index1CreationTimeStamp.equals(index2CreationTimeStamp)) {
                    return DESCENDING_SORT_PARAM_VALUE.equals(sortOrder) ? index2.compareTo(index1) : index1.compareTo(index2);
                }
                return DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)
                    ? Long.compare(index2CreationTimeStamp, index1CreationTimeStamp)
                    : Long.compare(index1CreationTimeStamp, index2CreationTimeStamp);
            })
            .collect(Collectors.toList());
    }

    private int getNewPageIndexStartNumber(
        final IndexStrategyPageToken requestedPageToken,
        final List<String> sortedIndicesList,
        final ClusterState clusterState
    ) {
        if (Objects.isNull(requestedPageToken)) {
            return 0;
        }
        int newPageStartIndexNumber = Math.min(requestedPageToken.posToStartPage, sortedIndicesList.size() - 1);
        if (newPageStartIndexNumber > 0
            && !Objects.equals(sortedIndicesList.get(newPageStartIndexNumber - 1), requestedPageToken.nameOfLastRespondedIndex)) {
            // case denoting an already responded index has been deleted while the paginated queries are being executed
            // find the index whose creation time is just after the index which was last responded
            newPageStartIndexNumber--;
            while (newPageStartIndexNumber > 0) {
                if (clusterState.metadata()
                    .indices()
                    .get(sortedIndicesList.get(newPageStartIndexNumber - 1))
                    .getCreationDate() < requestedPageToken.creationTimeOfLastRespondedIndex) {
                    break;
                }
                newPageStartIndexNumber--;
            }
        }
        return newPageStartIndexNumber;
    }

    /**
     * Token to be used by {@link IndexBasedPaginationStrategy}.
     * Token would like: IndexNumberToStartTheNextPageFrom + $ + CreationTimeOfLastRespondedIndex + $ +
     * QueryStartTime + $ + NameOfLastRespondedIndex
     */
    public static class IndexStrategyPageToken implements PageToken {

        private final int posToStartPage;
        private final long creationTimeOfLastRespondedIndex;
        private final long queryStartTime;
        private final String nameOfLastRespondedIndex;

        /**
         * Will perform simple validations on token received in the request and initialize the data members.
         * The token should be base64 encoded, and should contain the expected number of elements separated by "$".
         * The timestamps should also be a valid long.
         *
         * @param requestedTokenString string denoting the encoded next token requested by the user
         */
        public IndexStrategyPageToken(String requestedTokenString) {
            Objects.requireNonNull(requestedTokenString, "requestedTokenString can not be null");
            try {
                requestedTokenString = new String(Base64.getDecoder().decode(requestedTokenString), UTF_8);
            } catch (IllegalArgumentException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }

            final String[] requestedTokenElements = requestedTokenString.split("\\$");
            if (requestedTokenElements.length != 4) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }

            try {
                this.posToStartPage = Integer.parseInt(requestedTokenElements[0]);
                this.creationTimeOfLastRespondedIndex = Long.parseLong(requestedTokenElements[1]);
                this.queryStartTime = Long.parseLong(requestedTokenElements[2]);
                this.nameOfLastRespondedIndex = requestedTokenElements[3];
                if (posToStartPage < 0 || creationTimeOfLastRespondedIndex < 0 || queryStartTime < 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }

        public IndexStrategyPageToken(
            int indexNumberToStartPageFrom,
            long creationTimeOfLastRespondedIndex,
            long queryStartTime,
            String nameOfLastRespondedIndex
        ) {
            Objects.requireNonNull(nameOfLastRespondedIndex, "index name should be provided");
            this.posToStartPage = indexNumberToStartPageFrom;
            this.creationTimeOfLastRespondedIndex = creationTimeOfLastRespondedIndex;
            this.queryStartTime = queryStartTime;
            this.nameOfLastRespondedIndex = nameOfLastRespondedIndex;
        }

        @Override
        public String generateEncryptedToken() {
            return Base64.getEncoder()
                .encodeToString(
                    (posToStartPage + "$" + creationTimeOfLastRespondedIndex + "$" + queryStartTime + "$" + nameOfLastRespondedIndex)
                        .getBytes(UTF_8)
                );
        }
    }

}

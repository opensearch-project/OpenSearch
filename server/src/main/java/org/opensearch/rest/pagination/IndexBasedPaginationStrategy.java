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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        IndexStrategyTokenParser requestedToken = paginatedQueryRequest.getRequestedTokenStr() == null
            ? null
            : new IndexStrategyTokenParser(paginatedQueryRequest.getRequestedTokenStr());

        // Get sorted list of indices not containing the ones created after query start time
        List<String> sortedIndicesList = PaginationStrategy.getListOfIndicesSortedByCreateTime(
            clusterState,
            paginatedQueryRequest.getSort(),
            requestedToken == null ? Long.MAX_VALUE : requestedToken.queryStartTime
        );
        if (sortedIndicesList.isEmpty()) {
            // Denotes, that all the indices which were created before the queryStartTime have been deleted.
            // No nextToken and indices need to be shown in such cases.
            this.indicesFromRequestedToken = new ArrayList<>();
            this.paginatedQueryResponse = new PaginatedQueryResponse(null, paginatedElement);
        } else {
            final int requestedPageStartIndexNumber = getRequestedPageIndexStartNumber(
                requestedToken,
                sortedIndicesList,
                clusterState,
                paginatedQueryRequest.getSort()
            ); // inclusive
            int requestedPageEndIndexNumber = Math.min(
                requestedPageStartIndexNumber + paginatedQueryRequest.getSize(),
                sortedIndicesList.size()
            ); // exclusive

            this.indicesFromRequestedToken = sortedIndicesList.subList(requestedPageStartIndexNumber, requestedPageEndIndexNumber);

            // Set the queryStart time as the timestamp of latest created index if requested token is null.
            long queryStartTime = requestedToken == null
                ? DESCENDING_SORT_PARAM_VALUE.equals(paginatedQueryRequest.getSort())
                    ? clusterState.metadata().indices().get(sortedIndicesList.get(0)).getCreationDate()
                    : clusterState.metadata().indices().get(sortedIndicesList.get(sortedIndicesList.size() - 1)).getCreationDate()
                : requestedToken.queryStartTime;

            this.paginatedQueryResponse = new PaginatedQueryResponse(
                requestedPageEndIndexNumber >= sortedIndicesList.size()
                    ? null
                    : new IndexStrategyTokenParser(
                        requestedPageEndIndexNumber,
                        clusterState.metadata().indices().get(sortedIndicesList.get(requestedPageEndIndexNumber - 1)).getCreationDate(),
                        queryStartTime,
                        sortedIndicesList.get(requestedPageEndIndexNumber - 1)
                    ).generateEncryptedToken(),
                paginatedElement
            );
        }
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

    private int getRequestedPageIndexStartNumber(
        final IndexStrategyTokenParser requestedTokenParser,
        final List<String> sortedIndicesList,
        final ClusterState clusterState,
        final String sortOrder
    ) {
        if (Objects.isNull(requestedTokenParser)) {
            return 0;
        }
        int requestedPageStartIndexNumber = Math.min(requestedTokenParser.posToStartPage, sortedIndicesList.size());
        if (requestedPageStartIndexNumber > 0
            && !Objects.equals(sortedIndicesList.get(requestedPageStartIndexNumber - 1), requestedTokenParser.nameOfLastRespondedIndex)) {
            // case denoting an already responded index has been deleted while the paginated queries are being executed
            // find the index whose creation time is just after/before (based on sortOrder) the index which was last responded.
            while (requestedPageStartIndexNumber > 0) {
                long creationTimeOfIndex = clusterState.metadata()
                    .indices()
                    .get(sortedIndicesList.get(requestedPageStartIndexNumber - 1))
                    .getCreationDate();
                if ((!DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)
                    && creationTimeOfIndex < requestedTokenParser.creationTimeOfLastRespondedIndex)
                    || (DESCENDING_SORT_PARAM_VALUE.equals(sortOrder)
                        && creationTimeOfIndex > requestedTokenParser.creationTimeOfLastRespondedIndex)) {
                    break;
                }
                requestedPageStartIndexNumber--;
            }
        }
        return requestedPageStartIndexNumber;
    }

    /**
     * TokenParser to be used by {@link IndexBasedPaginationStrategy}.
     * Token would like: IndexNumberToStartTheNextPageFrom + $ + CreationTimeOfLastRespondedIndex + $ +
     * QueryStartTime + $ + NameOfLastRespondedIndex
     */
    public static class IndexStrategyTokenParser {

        private final int posToStartPage;
        private final long creationTimeOfLastRespondedIndex;
        private final long queryStartTime;
        private final String nameOfLastRespondedIndex;

        public IndexStrategyTokenParser(String requestedTokenString) {
            validateIndexStrategyToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split("\\$");
            this.posToStartPage = Integer.parseInt(decryptedTokenElements[0]);
            this.creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[1]);
            this.queryStartTime = Long.parseLong(decryptedTokenElements[2]);
            this.nameOfLastRespondedIndex = decryptedTokenElements[3];
        }

        public IndexStrategyTokenParser(
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

        public String generateEncryptedToken() {
            return PaginationStrategy.encryptStringToken(
                posToStartPage + "$" + creationTimeOfLastRespondedIndex + "$" + queryStartTime + "$" + nameOfLastRespondedIndex
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
            final String[] decryptedTokenElements = decryptedToken.split("\\$");
            if (decryptedTokenElements.length != 4) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
            try {
                int posToStartPage = Integer.parseInt(decryptedTokenElements[0]);
                long creationTimeOfLastRespondedIndex = Long.parseLong(decryptedTokenElements[1]);
                long queryStartTime = Long.parseLong(decryptedTokenElements[2]);
                if (posToStartPage < 0 || creationTimeOfLastRespondedIndex < 0 || queryStartTime < 0) {
                    throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
                }
            } catch (NumberFormatException exception) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        }
    }

}

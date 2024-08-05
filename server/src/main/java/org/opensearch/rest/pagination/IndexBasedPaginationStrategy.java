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

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @opensearch.internal
 */
public class IndexBasedPaginationStrategy implements PaginationStrategy<String> {

    private final String nextToken;
    private final String previousToken;
    private final List<String> pageElements;

    private static final String INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE =
        "Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token].";

    public IndexBasedPaginationStrategy(String requestedToken, int maxPageSize, boolean latestIndicesFirst, ClusterState clusterState) {
        // validate the requestedToken again, if the rest layer didn't do so.
        validateRequestedRequest(requestedToken);
        requestedToken = requestedToken == null ? null : new String(Base64.getDecoder().decode(requestedToken), UTF_8);
        // Get sorted list of indices from metadata and filter out the required number of indices
        List<String> sortedIndicesList = getListOfIndicesSortedByCreateTime(clusterState, latestIndicesFirst, requestedToken);
        final int newPageStartIndexNumber = getNewPageIndexStartNumber(requestedToken, sortedIndicesList, clusterState); // inclusive
        int newPageEndIndexNumber = Math.min(newPageStartIndexNumber + maxPageSize, sortedIndicesList.size()); // exclusive
        this.pageElements = sortedIndicesList.subList(newPageStartIndexNumber, newPageEndIndexNumber);

        // Generate the next_token which is to be passed in the response.
        // NextToken = "IndexNumberToStartTheNextPageFrom + $ + CreationTimeOfLastRespondedIndex + $ + QueryStartTime + $
        // NameOfLastRespondedIndex" -> (1$12345678$12345678$testIndex)
        long queryStartTime = requestedToken == null
            ? clusterState.metadata().indices().get(sortedIndicesList.get(sortedIndicesList.size() - 1)).getCreationDate()
            : Long.parseLong(requestedToken.split("\\$")[2]);

        int previousPageStartIndexNumber = Math.max(newPageStartIndexNumber - maxPageSize, 0);
        this.previousToken = newPageStartIndexNumber <= 0
            ? null
            : Base64.getEncoder()
                .encodeToString(
                    (previousPageStartIndexNumber
                        + "$"
                        + clusterState.metadata()
                            .indices()
                            .get(sortedIndicesList.get(Math.max(previousPageStartIndexNumber - 1, 0)))
                            .getCreationDate()
                        + "$"
                        + queryStartTime
                        + "$"
                        + sortedIndicesList.get(Math.max(previousPageStartIndexNumber - 1, 0))).getBytes(UTF_8)
                );

        this.nextToken = newPageEndIndexNumber >= sortedIndicesList.size()
            ? null
            : Base64.getEncoder()
                .encodeToString(
                    (newPageEndIndexNumber
                        + "$"
                        + clusterState.metadata().indices().get(sortedIndicesList.get(newPageEndIndexNumber - 1)).getCreationDate()
                        + "$"
                        + queryStartTime
                        + "$"
                        + sortedIndicesList.get(newPageEndIndexNumber - 1)).getBytes(UTF_8)
                );
    }

    @Override
    public String getNextToken() {
        return nextToken;
    }

    @Override
    public String getPreviousToken() {
        return previousToken;
    }

    @Override
    public List<String> getPageElements() {
        return pageElements;
    }

    private List<String> getListOfIndicesSortedByCreateTime(
        final ClusterState clusterState,
        boolean latestIndicesFirst,
        String requestedToken
    ) {
        long latestValidIndexCreateTime = requestedToken == null ? Long.MAX_VALUE : Long.parseLong(requestedToken.split("\\$")[2]);
        // Filter out the indices which have been created after the latest index which was present when paginated query started
        List<String> indicesList = clusterState.getRoutingTable()
            .getIndicesRouting()
            .keySet()
            .stream()
            .filter(index -> (latestValidIndexCreateTime - clusterState.metadata().indices().get(index).getCreationDate()) >= 0)
            .collect(Collectors.toList());
        // Sort the indices list based on their creation timestamps
        indicesList.sort((index1, index2) -> {
            Long index1CreationTimeStamp = clusterState.metadata().indices().get(index1).getCreationDate();
            Long index2CreationTimeStamp = clusterState.metadata().indices().get(index2).getCreationDate();
            if (index1CreationTimeStamp.equals(index2CreationTimeStamp)) {
                return latestIndicesFirst == true ? index2.compareTo(index1) : index1.compareTo(index2);
            }
            return latestIndicesFirst == true
                ? Long.compare(index2CreationTimeStamp, index1CreationTimeStamp)
                : Long.compare(index1CreationTimeStamp, index2CreationTimeStamp);
        });
        return indicesList;
    }

    private int getNewPageIndexStartNumber(
        final String nextTokenInRequest,
        final List<String> sortedIndicesList,
        final ClusterState clusterState
    ) {
        if (Objects.isNull(nextTokenInRequest)) {
            return 0;
        }
        final String[] nextTokenElements = nextTokenInRequest.split("\\$");
        int newPageStartIndexNumber = Math.min(Integer.parseInt(nextTokenElements[0]), sortedIndicesList.size());
        long lastIndexCreationTime = Long.parseLong(nextTokenElements[1]);
        String lastIndexName = nextTokenElements[3];
        if (newPageStartIndexNumber > 0 && !Objects.equals(sortedIndicesList.get(newPageStartIndexNumber - 1), lastIndexName)) {
            // case denoting an already responded index has been deleted while the paginated queries are being executed
            // find the index whose creation time is just after the index which was last responded
            newPageStartIndexNumber--;
            while (newPageStartIndexNumber > 0) {
                if (clusterState.metadata()
                    .indices()
                    .get(sortedIndicesList.get(newPageStartIndexNumber - 1))
                    .getCreationDate() < lastIndexCreationTime) {
                    break;
                }
                newPageStartIndexNumber--;
            }
        }
        return newPageStartIndexNumber;
    }

    /**
     * Performs simple validations on token received in the request which was generated using {@link IndexBasedPaginationStrategy}. The token should be base64 encoded, and should contain the expected number of elements separated by "$". The timestamps should also be a valid long.
     * @param requestedToken
     */
    public static void validateRequestedRequest(String requestedToken) {
        if (Objects.isNull(requestedToken)) {
            return;
        }
        try {
            requestedToken = new String(Base64.getDecoder().decode(requestedToken), UTF_8);
        } catch (IllegalArgumentException exception) {
            throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
        }

        final String[] requestedTokenElements = requestedToken.split("\\$");
        if (requestedTokenElements.length != 4) {
            throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
        }

        try {
            int newPageStartIndexNumber = Integer.parseInt(requestedTokenElements[0]);
            long lastSentIndexCreationTime = Long.parseLong(requestedTokenElements[1]);
            long queryStartTime = Long.parseLong(requestedTokenElements[2]);
            if (newPageStartIndexNumber < 0 || lastSentIndexCreationTime < 0 || queryStartTime < 0) {
                throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
            }
        } catch (NumberFormatException exception) {
            throw new OpenSearchParseException(INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
        }
    }

}

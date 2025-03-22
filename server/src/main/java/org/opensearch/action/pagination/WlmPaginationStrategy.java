/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.pagination;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.QueryGroupStats;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.ArrayList;

/**
 * Pagination strategy for Workload Management (WLM) Stats.
 * Paginates based on query group IDs.
 */
public class WlmPaginationStrategy implements PaginationStrategy<WlmStats> {
    private static final String DEFAULT_PAGINATED_ENTITY = "wlm_stats";

    private final int pageSize;
    private final String nextToken;
    private final String sortBy;
    private final String sortOrder;
    private final List<WlmStats> paginatedStats;
    private PageToken responseToken;

    public WlmPaginationStrategy(int pageSize, String nextToken, String sortBy, String sortOrder, WlmStatsResponse response) {
        this.pageSize = pageSize;
        this.nextToken = nextToken;
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;

        WlmStrategyToken requestedToken = (nextToken == null || nextToken.isEmpty())
            ? null
            : new WlmStrategyToken(nextToken);

        List<WlmStats> sortedStats = sortStats(response.getNodes(), sortBy, sortOrder);
        this.paginatedStats = applyPagination(sortedStats, requestedToken);
    }

    /**
     * Sorts WlmStats based on the provided attribute (`node_id` or `query_group`).
     */
    private List<WlmStats> sortStats(List<WlmStats> stats, String sortBy, String sortOrder) {
        Comparator<WlmStats> comparator;

        switch (sortBy) {
            case "query_group":
                comparator = Comparator.comparing(
                    (WlmStats wlmStats) -> wlmStats.getQueryGroupStats().getStats().isEmpty()
                        ? ""
                        : wlmStats.getQueryGroupStats().getStats().keySet().iterator().next()
                ).thenComparing(wlmStats -> wlmStats.getNode().getId());
                break;

            case "node_id":  // Default sorting
            default:
                comparator = Comparator.comparing(
                    (WlmStats wlmStats) -> wlmStats.getNode().getId()
                ).thenComparing(
                    wlmStats -> wlmStats.getQueryGroupStats().getStats().isEmpty()
                        ? ""
                        : wlmStats.getQueryGroupStats().getStats().keySet().iterator().next()
                );
                break;
        }

        if ("desc".equals(sortOrder)) {
            comparator = comparator.reversed();
        }

        return stats.stream().sorted(comparator).collect(Collectors.toList());
    }

    /**
     * Applies pagination on already sorted WlmStats.
     */
    private List<WlmStats> applyPagination(List<WlmStats> sortedStats, WlmStrategyToken requestedToken) {
        if (sortedStats.isEmpty()) {
            this.responseToken = null;
            return Collections.emptyList();
        }

        List<WlmStats> allQueryGroups = new ArrayList<>();
        for (WlmStats stat : sortedStats) {
            Map<String, QueryGroupStats.QueryGroupStatsHolder> queryGroups = stat.getQueryGroupStats().getStats();
            for (Map.Entry<String, QueryGroupStats.QueryGroupStatsHolder> entry : queryGroups.entrySet()) {
                String queryGroupId = entry.getKey();
                QueryGroupStats singleQueryGroupStats = new QueryGroupStats(Map.of(queryGroupId, entry.getValue()));
                allQueryGroups.add(new WlmStats(stat.getNode(), singleQueryGroupStats));
            }
        }

        allQueryGroups = sortStats(allQueryGroups, sortBy, sortOrder);

        int startIndex = 0;
        if (requestedToken != null) {
            OptionalInt foundIndex = findIndex(allQueryGroups, requestedToken.lastQueryGroupId);
            if (foundIndex.isEmpty()) {
                throw new OpenSearchParseException("Invalid or outdated token: " + nextToken);
            }
            startIndex = foundIndex.getAsInt();
        }

        int endIndex = Math.min(startIndex + pageSize, allQueryGroups.size());
        List<WlmStats> page = allQueryGroups.subList(startIndex, endIndex);

        if (endIndex < allQueryGroups.size()) {
            String lastQueryGroup = allQueryGroups.get(endIndex - 1).getQueryGroupStats().getStats().keySet().iterator().next();
            this.responseToken = new PageToken(WlmStrategyToken.generateEncryptedToken(lastQueryGroup), DEFAULT_PAGINATED_ENTITY);
        } else {
            this.responseToken = null;
        }

        return page;
    }

    /**
     * Finds the index of a given query group in the list.
     */
    private OptionalInt findIndex(List<WlmStats> stats, String queryGroupId) {
        for (int i = 0; i < stats.size(); i++) {
            if (stats.get(i).getQueryGroupStats().getStats().containsKey(queryGroupId)) {
                return OptionalInt.of(i + 1);
            }
        }
        return OptionalInt.empty();
    }

    @Override
    public PageToken getResponseToken() {
        return responseToken;
    }

    @Override
    public List<WlmStats> getRequestedEntities() {
        return paginatedStats;
    }

    /**
     * Retrieves paginated stats.
     */
    public List<WlmStats> getPaginatedStats() {
        return paginatedStats;
    }

    /**
     * Retrieves the next pagination token.
     */
    public PageToken getNextToken() {
        return responseToken;
    }

    public static class WlmStrategyToken {
        private static final String JOIN_DELIMITER = "|";
        private static final String SPLIT_REGEX = "\\|";

        private final String lastQueryGroupId;

        public WlmStrategyToken(String requestedTokenString) {
            validateToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] decryptedTokenElements = decryptedToken.split(SPLIT_REGEX);
            this.lastQueryGroupId = decryptedTokenElements[0];
        }

        public static String generateEncryptedToken(String lastQueryGroupId) {
            return PaginationStrategy.encryptStringToken(lastQueryGroupId);
        }

        private static void validateToken(String token) {
            Objects.requireNonNull(token, "Token cannot be null");
            String decryptedToken = PaginationStrategy.decryptStringToken(token);
            final String[] elements = decryptedToken.split(SPLIT_REGEX);
            if (elements.length != 1 || elements[0].isEmpty()) {
                throw new OpenSearchParseException("Invalid token format");
            }
        }
    }
}

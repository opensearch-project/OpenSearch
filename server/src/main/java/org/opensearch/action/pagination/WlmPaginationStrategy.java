package org.opensearch.action.pagination;

import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.QueryGroupStats;

import java.util.*;
import java.util.stream.Collectors;

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

        List<WlmStats> sortedStats = sortStats(response.getNodes(), sortBy, sortOrder);
        this.paginatedStats = applyPagination(sortedStats, nextToken);
    }

    /**
     * Sorts WlmStats based on the provided attribute (`node_id` or `query_group`).
     */
    private List<WlmStats> sortStats(List<WlmStats> stats, String sortBy, String sortOrder) {
        Comparator<WlmStats> comparator;

        switch (sortBy) {
            case "query_group":
                comparator = Comparator.comparing(
                    (WlmStats wlmStats)-> wlmStats.getQueryGroupStats().getStats().isEmpty()
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
    private List<WlmStats> applyPagination(List<WlmStats> sortedStats, String nextToken) {
        if (sortedStats.isEmpty()) {
            this.responseToken = new PageToken(null, DEFAULT_PAGINATED_ENTITY);
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
        if (nextToken != null) {
            String lastQueryGroup = PaginationStrategy.decryptStringToken(nextToken);
            OptionalInt foundIndex = findIndex(allQueryGroups, lastQueryGroup);
            startIndex = foundIndex.orElse(0);
        }

        int endIndex = Math.min(startIndex + pageSize, allQueryGroups.size());
        List<WlmStats> page = allQueryGroups.subList(startIndex, endIndex);

        if (endIndex < allQueryGroups.size()) {
            String lastQueryGroup = allQueryGroups.get(endIndex - 1).getQueryGroupStats().getStats().keySet().iterator().next();
            this.responseToken = new PageToken(PaginationStrategy.encryptStringToken(lastQueryGroup), DEFAULT_PAGINATED_ENTITY);
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

    public List<WlmStats> getPaginatedStats(WlmStatsResponse response) {
        return this.paginatedStats;
    }

    public PageToken getNextToken() {
        return responseToken;
    }
}

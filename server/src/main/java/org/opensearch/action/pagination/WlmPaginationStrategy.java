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
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.wlm.stats.SortBy;
import org.opensearch.wlm.stats.SortOrder;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.WorkloadGroupStats;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * Pagination strategy for Workload Management (WLM) Stats.
 * Paginates based on workload group IDs.
 */
public class WlmPaginationStrategy implements PaginationStrategy<WlmStats> {
    private static final String DEFAULT_PAGINATED_ENTITY = "wlm_stats";

    private final int pageSize;
    private final String nextToken;
    private final SortBy sortBy;
    private final SortOrder sortOrder;
    private final List<WlmStats> paginatedStats;
    private final int snapshotWorkloadGroupCount;
    private PageToken responseToken;
    private static final String HASH_ALGORITHM = "SHA-256";

    public WlmPaginationStrategy(int pageSize, String nextToken, SortBy sortBy, SortOrder sortOrder, WlmStatsResponse response) {
        this.pageSize = pageSize;
        this.nextToken = nextToken;
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;

        this.snapshotWorkloadGroupCount = response.getNodes()
            .stream()
            .mapToInt(stat -> stat.getWorkloadGroupStats().getStats().size())
            .sum();

        String currentHash = computeWorkloadGroupHash(response.getNodes());

        WlmStrategyToken requestedToken = (nextToken == null || nextToken.isEmpty()) ? null : new WlmStrategyToken(nextToken);

        this.paginatedStats = applyPagination(response.getNodes(), requestedToken, currentHash);
    }

    // Compute the hash for all (nodeId|workloadGroupId) pairs
    private String computeWorkloadGroupHash(List<WlmStats> stats) {
        return stats.stream()
            .flatMap(
                stat -> stat.getWorkloadGroupStats()
                    .getStats()
                    .keySet()
                    .stream()
                    .map(WorkloadGroupId -> stat.getNode().getId() + "|" + WorkloadGroupId)
            )
            .sorted()
            .collect(Collectors.collectingAndThen(Collectors.joining(","), this::sha256Hex));
    }

    private String sha256Hex(String input) {
        return MessageDigests.toHexString(MessageDigests.sha256().digest(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Applies pagination to a list of WlmStats entries.
     *
     * Flattens stats by workload group, sorts them using the specified sortBy and sortOrder,
     * determines the start index from the given requestedToken, and returns a sublist
     * limited by pageSize. Sets responseToken if more results are available.
     *
     * @param rawStats unpaginated stats from all nodes
     * @param requestedToken pagination token from the client, or null for the first page
     * @param currentHash hash representing the current snapshot, used for token validation
     * @return paginated sublist of WlmStats
     */

    private List<WlmStats> applyPagination(List<WlmStats> rawStats, WlmStrategyToken requestedToken, String currentHash) {
        if (rawStats.isEmpty()) {
            this.responseToken = null;
            return Collections.emptyList();
        }

        List<WlmStats> perWorkloadGroupStats = extractWorkloadGroupStats(rawStats);

        perWorkloadGroupStats = perWorkloadGroupStats.stream().sorted(sortOrder.apply(sortBy.getComparator())).collect(Collectors.toList());

        int startIndex = getStartIndex(perWorkloadGroupStats, requestedToken);

        List<WlmStats> page = getPage(perWorkloadGroupStats, startIndex);

        setResponseToken(perWorkloadGroupStats, startIndex + page.size(), currentHash);

        return page;
    }

    private List<WlmStats> extractWorkloadGroupStats(List<WlmStats> rawStats) {
        List<WlmStats> result = new ArrayList<>();
        for (WlmStats stat : rawStats) {
            Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> WorkloadGroups = stat.getWorkloadGroupStats().getStats();
            for (Map.Entry<String, WorkloadGroupStats.WorkloadGroupStatsHolder> entry : WorkloadGroups.entrySet()) {
                String workloadGroupId = entry.getKey();
                WorkloadGroupStats singleWorkloadGroupStats = new WorkloadGroupStats(Map.of(workloadGroupId, entry.getValue()));
                result.add(new WlmStats(stat.getNode(), singleWorkloadGroupStats));
            }
        }
        return result;
    }

    protected int getStartIndex(List<WlmStats> sortedStats, WlmStrategyToken token) {
        if (token == null) {
            return 0;
        }

        OptionalInt index = findIndex(sortedStats, token.getNodeId(), token.getWorkloadGroupId());

        if (index.isEmpty()) {
            throw new OpenSearchParseException("Invalid or outdated token: " + nextToken);
        }

        return index.getAsInt();
    }

    private List<WlmStats> getPage(List<WlmStats> stats, int startIndex) {
        int endIndex = Math.min(startIndex + pageSize, stats.size());
        return stats.subList(startIndex, endIndex);
    }

    private void setResponseToken(List<WlmStats> stats, int nextIndex, String currentHash) {
        if (nextIndex < stats.size()) {
            WlmStats lastEntry = stats.get(nextIndex - 1);
            String nodeId = lastEntry.getNode().getId();
            String workloadGroupId = lastEntry.getWorkloadGroupStats().getStats().keySet().iterator().next();

            this.responseToken = new PageToken(
                WlmStrategyToken.generateEncryptedToken(
                    nodeId,
                    workloadGroupId,
                    snapshotWorkloadGroupCount,
                    currentHash,
                    sortOrder.name(),
                    sortBy.name()
                ),
                DEFAULT_PAGINATED_ENTITY
            );
        } else {
            this.responseToken = null;
        }
    }

    protected OptionalInt findIndex(List<WlmStats> stats, String nodeId, String workloadGroupId) {
        for (int i = 0; i < stats.size(); i++) {
            WlmStats stat = stats.get(i);
            if (stat.getNode().getId().equals(nodeId) && stat.getWorkloadGroupStats().getStats().containsKey(workloadGroupId)) {
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
     * Represents a token used in the WLM strategy for pagination.
     * This class encapsulates the token data required for identifying the current state of pagination.
     */
    public static class WlmStrategyToken {
        private static final String JOIN_DELIMITER = "|";
        private static final String SPLIT_REGEX = "\\|";
        private static final int NODE_ID_POS = 0;
        private static final int WORKLOAD_GROUP_ID_POS = 1;
        private static final int WORKLOAD_GROUP_COUNT_POS = 2;
        private static final int HASH_POS = 3;
        private static final int SORT_ORDER_POS = 4;
        private static final int SORT_BY_POS = 5;

        private final String nodeId;
        private final String workloadGroupId;
        private final int workloadGroupCount;
        private final String hash;
        private final String sortOrder;
        private final String sortBy;

        public WlmStrategyToken(String requestedTokenString) {
            final String[] parts = validateToken(requestedTokenString);

            this.nodeId = parts[NODE_ID_POS];
            this.workloadGroupId = parts[WORKLOAD_GROUP_ID_POS];
            this.workloadGroupCount = Integer.parseInt(parts[WORKLOAD_GROUP_COUNT_POS]);
            this.hash = parts[HASH_POS];
            this.sortOrder = parts[SORT_ORDER_POS];
            this.sortBy = parts[SORT_BY_POS];
        }

        public static String generateEncryptedToken(
            String nodeId,
            String workloadGroupId,
            int workloadGroupCount,
            String hash,
            String sortOrder,
            String sortBy
        ) {
            String raw = String.join(JOIN_DELIMITER, nodeId, workloadGroupId, String.valueOf(workloadGroupCount), hash, sortOrder, sortBy);
            return PaginationStrategy.encryptStringToken(raw);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getWorkloadGroupId() {
            return workloadGroupId;
        }

        public int getWorkloadGroupCount() {
            return workloadGroupCount;
        }

        public String getHash() {
            return hash;
        }

        public String getSortOrder() {
            return sortOrder;
        }

        public String getSortBy() {
            return sortBy;
        }

        private static boolean isNullOrBlank(String str) {
            return str == null || str.trim().isEmpty();
        }

        private static String[] validateToken(String token) {
            Objects.requireNonNull(token, "Token cannot be null");
            String decrypted = PaginationStrategy.decryptStringToken(token);
            final String[] parts = decrypted.split(SPLIT_REGEX);
            if (parts.length != 6
                || isNullOrBlank(parts[NODE_ID_POS])
                || isNullOrBlank(parts[WORKLOAD_GROUP_ID_POS])
                || isNullOrBlank(parts[HASH_POS])
                || isNullOrBlank(parts[SORT_ORDER_POS])
                || isNullOrBlank(parts[SORT_BY_POS])) {
                throw new OpenSearchParseException("Invalid pagination token format");
            }
            return parts;
        }
    }
}

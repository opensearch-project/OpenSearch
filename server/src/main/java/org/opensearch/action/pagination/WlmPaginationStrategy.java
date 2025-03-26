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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
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
    private final SortBy sortBy;
    private final SortOrder sortOrder;
    private final List<WlmStats> paginatedStats;
    private final int snapshotQueryGroupCount;
    private PageToken responseToken;

    public WlmPaginationStrategy(int pageSize, String nextToken, SortBy sortBy, SortOrder sortOrder, WlmStatsResponse response) {
        this.pageSize = pageSize;
        this.nextToken = nextToken;
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;

        this.snapshotQueryGroupCount = response.getNodes().stream()
            .mapToInt(stat -> stat.getQueryGroupStats().getStats().size())
            .sum();

        String currentHash = computeQueryGroupHash(response.getNodes());

        WlmStrategyToken requestedToken = (nextToken == null || nextToken.isEmpty())
            ? null
            : new WlmStrategyToken(nextToken);

        if (requestedToken != null) {
            if (requestedToken.getQueryGroupCount() != snapshotQueryGroupCount ||
                !requestedToken.getHash().equals(currentHash)) {
                throw new OpenSearchParseException("Query group state has changed since the last request. Pagination is invalidated.");
            }
        }

        this.paginatedStats = applyPagination(response.getNodes(), requestedToken, currentHash);
    }

    // Compute the hash for all (nodeId|queryGroupId) pairs
    private String computeQueryGroupHash(List<WlmStats> stats) {
        return stats.stream()
            .flatMap(stat -> stat.getQueryGroupStats().getStats().keySet().stream()
                .map(queryGroupId -> stat.getNode().getId() + "|" + queryGroupId))
            .sorted()
            .collect(Collectors.collectingAndThen(Collectors.joining(","), this::sha256Hex));
    }

    private String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                hexString.append(String.format("%02x", b));
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    private List<WlmStats> applyPagination(List<WlmStats> rawStats, WlmStrategyToken requestedToken, String currentHash) {
        if (rawStats.isEmpty()) {
            this.responseToken = null;
            return Collections.emptyList();
        }

        List<WlmStats> perQueryGroupStats = new ArrayList<>();
        for (WlmStats stat : rawStats) {
            Map<String, QueryGroupStats.QueryGroupStatsHolder> queryGroups = stat.getQueryGroupStats().getStats();
            for (Map.Entry<String, QueryGroupStats.QueryGroupStatsHolder> entry : queryGroups.entrySet()) {
                String queryGroupId = entry.getKey();
                QueryGroupStats singleQueryGroupStats = new QueryGroupStats(Map.of(queryGroupId, entry.getValue()));
                perQueryGroupStats.add(new WlmStats(stat.getNode(), singleQueryGroupStats));
            }
        }

        perQueryGroupStats = perQueryGroupStats.stream()
            .sorted(sortOrder.apply(sortBy.getComparator()))
            .collect(Collectors.toList());

        int startIndex = 0;
        if (requestedToken != null) {
            OptionalInt foundIndex = findIndex(
                perQueryGroupStats,
                requestedToken.getNodeId(),
                requestedToken.getQueryGroupId()
            );
            if (foundIndex.isEmpty()) {
                throw new OpenSearchParseException("Invalid or outdated token: " + nextToken);
            }
            startIndex = foundIndex.getAsInt();
        }

        int endIndex = Math.min(startIndex + pageSize, perQueryGroupStats.size());
        List<WlmStats> page = perQueryGroupStats.subList(startIndex, endIndex);

        if (endIndex < perQueryGroupStats.size()) {
            WlmStats lastEntry = perQueryGroupStats.get(endIndex - 1);
            String nodeId = lastEntry.getNode().getId();
            String queryGroupId = lastEntry.getQueryGroupStats().getStats().keySet().iterator().next();

            this.responseToken = new PageToken(
                WlmStrategyToken.generateEncryptedToken(nodeId, queryGroupId, snapshotQueryGroupCount, currentHash),
                DEFAULT_PAGINATED_ENTITY
            );
        } else {
            this.responseToken = null;
        }

        return page;
    }

    private OptionalInt findIndex(List<WlmStats> stats, String nodeId, String queryGroupId) {
        for (int i = 0; i < stats.size(); i++) {
            WlmStats stat = stats.get(i);
            if (stat.getNode().getId().equals(nodeId)
                && stat.getQueryGroupStats().getStats().containsKey(queryGroupId)) {
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

    public List<WlmStats> getPaginatedStats() {
        return paginatedStats;
    }

    public PageToken getNextToken() {
        return responseToken;
    }

    public static class WlmStrategyToken {
        private static final String JOIN_DELIMITER = "|";
        private static final String SPLIT_REGEX = "\\|";
        private static final int NODE_ID_POS = 0;
        private static final int QUERY_GROUP_ID_POS = 1;
        private static final int QUERY_GROUP_COUNT_POS = 2;
        private static final int HASH_POS = 3;

        private final String nodeId;
        private final String queryGroupId;
        private final int queryGroupCount;
        private final String hash;

        public WlmStrategyToken(String requestedTokenString) {
            validateToken(requestedTokenString);
            String decryptedToken = PaginationStrategy.decryptStringToken(requestedTokenString);
            final String[] parts = decryptedToken.split(SPLIT_REGEX);

            this.nodeId = parts[NODE_ID_POS];
            this.queryGroupId = parts[QUERY_GROUP_ID_POS];
            this.queryGroupCount = Integer.parseInt(parts[QUERY_GROUP_COUNT_POS]);
            this.hash = parts[HASH_POS];
        }

        public static String generateEncryptedToken(String nodeId, String queryGroupId, int queryGroupCount, String hash) {
            String raw = String.join(JOIN_DELIMITER, nodeId, queryGroupId, String.valueOf(queryGroupCount), hash);
            return PaginationStrategy.encryptStringToken(raw);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getQueryGroupId() {
            return queryGroupId;
        }

        public int getQueryGroupCount() {
            return queryGroupCount;
        }

        public String getHash() {
            return hash;
        }

        private static void validateToken(String token) {
            Objects.requireNonNull(token, "Token cannot be null");
            String decrypted = PaginationStrategy.decryptStringToken(token);
            final String[] parts = decrypted.split(SPLIT_REGEX);
            if (parts.length != 4 || parts[0].isEmpty() || parts[1].isEmpty() || parts[3].isEmpty()) {
                throw new OpenSearchParseException("Invalid pagination token format");
            }
        }
    }
}

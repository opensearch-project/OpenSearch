/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.pruning.FieldDomainEvaluationContext;
import org.opensearch.action.search.pruning.FieldDomainEvaluators;
import org.opensearch.action.search.pruning.MandatoryQueryConstraintExtractor;
import org.opensearch.action.search.pruning.QueryConstraint;
import org.opensearch.action.search.pruning.QueryConstraintExtractor;
import org.opensearch.action.search.pruning.SearchIndexPruningResult;
import org.opensearch.action.search.pruning.SearchIndexPruningSettings;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddomain.FieldDomain;
import org.opensearch.index.fielddomain.FieldDomainProvider;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Computes index-level shard-group pruning decisions for a search request before can-match or query execution.
 *
 * This service is deliberately conservative: it only returns pruned shard groups when a mandatory query constraint
 * is provably disjoint from finalized index-level field domains stored in cluster state. Missing, malformed,
 * unsupported, unfinalized, remote, or ambiguous metadata causes the shard group to be kept.
 */
public final class SearchIndexPruningService {
    private static final Logger logger = LogManager.getLogger(SearchIndexPruningService.class);

    private final QueryConstraintExtractor constraintExtractor;
    private final FieldDomainProvider domainProvider;
    private final FieldDomainEvaluators fieldDomainEvaluators;

    private final AtomicReference<SearchIndexPruningConfig> config;

    /**
     * Creates a pruning service backed by cluster settings and index field domains stored in cluster state.
     *
     * @param clusterSettings cluster settings used to read and subscribe to pruning settings
     * @param domainProvider provider that resolves index-level field domains for candidate shard groups
     */
    public SearchIndexPruningService(ClusterSettings clusterSettings, FieldDomainProvider domainProvider) {
        this(clusterSettings, domainProvider, new MandatoryQueryConstraintExtractor(), FieldDomainEvaluators.defaultEvaluators());
    }

    SearchIndexPruningService(
        ClusterSettings clusterSettings,
        FieldDomainProvider domainProvider,
        QueryConstraintExtractor constraintExtractor
    ) {
        this(clusterSettings, domainProvider, constraintExtractor, FieldDomainEvaluators.defaultEvaluators());
    }

    SearchIndexPruningService(
        ClusterSettings clusterSettings,
        FieldDomainProvider domainProvider,
        QueryConstraintExtractor constraintExtractor,
        FieldDomainEvaluators fieldDomainEvaluators
    ) {
        Objects.requireNonNull(clusterSettings, "clusterSettings must not be null");
        this.domainProvider = Objects.requireNonNull(domainProvider, "domainProvider must not be null");
        this.constraintExtractor = Objects.requireNonNull(constraintExtractor, "constraintExtractor must not be null");
        this.fieldDomainEvaluators = Objects.requireNonNull(fieldDomainEvaluators, "fieldDomainEvaluators must not be null");

        this.config = new AtomicReference<>(SearchIndexPruningConfig.fromClusterSettings(clusterSettings));
        clusterSettings.addSettingsUpdateConsumer(
            settings -> config.set(SearchIndexPruningConfig.fromSettings(settings)),
            SearchIndexPruningSettings.getSettings()
        );
    }

    /**
     * Computes which shard groups can be skipped for the supplied search request.
     *
     * The returned result does not mutate the shard iterators. Callers decide when to materialize the result by marking
     * the selected {@link SearchShardIterator}s as skipped.
     *
     * @param request search request being executed
     * @param shardIterators shard groups resolved for this request
     * @param clusterState cluster state used to read index metadata
     * @param evaluationContext request-scoped values needed while evaluating field domains
     * @return pruning result containing the original shard iterators and any pruned shard group indexes
     */
    public SearchIndexPruningResult prune(
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardIterators,
        ClusterState clusterState,
        FieldDomainEvaluationContext evaluationContext
    ) {
        Objects.requireNonNull(shardIterators, "shardIterators must not be null");
        try {
            return doPrune(request, shardIterators, clusterState, evaluationContext);
        } catch (RuntimeException e) {
            logger.warn("failed to compute search index pruning; continuing without pruning", e);
            return SearchIndexPruningResult.notPruned(shardIterators);
        }
    }

    private SearchIndexPruningResult doPrune(
        SearchRequest request,
        GroupShardsIterator<SearchShardIterator> shardIterators,
        ClusterState clusterState,
        FieldDomainEvaluationContext evaluationContext
    ) {
        Objects.requireNonNull(evaluationContext, "evaluationContext must not be null");
        final int originalSize = size(shardIterators);
        final int activeShardGroups = activeShardGroups(shardIterators);
        final SearchIndexPruningConfig currentConfig = config.get();

        if (shouldSkip(request, activeShardGroups, currentConfig)) {
            return SearchIndexPruningResult.notPruned(shardIterators);
        }

        final Set<String> pruningFields = currentConfig.fields;

        final List<QueryConstraint> constraints = constraintExtractor.extractMandatoryConstraints(request.source(), pruningFields);

        if (constraints.isEmpty()) {
            return SearchIndexPruningResult.notPruned(shardIterators);
        }

        Map<String, Map<String, Optional<FieldDomain>>> domainsByIndexAndField = new HashMap<>();
        BitSet prunedShardGroupIndexes = new BitSet(originalSize);
        int pruned = 0;

        for (int shardGroupIndex = 0; shardGroupIndex < shardIterators.size(); shardGroupIndex++) {
            SearchShardIterator shardIterator = shardIterators.get(shardGroupIndex);
            if (shardIterator.skip()) {
                continue;
            }

            if (shouldAlwaysKeep(shardIterator)) {
                continue;
            }

            final String indexName = shardIterator.shardId().getIndexName();

            boolean prune = false;
            for (QueryConstraint constraint : constraints) {
                final Optional<FieldDomain> maybeDomain = domainsByIndexAndField.computeIfAbsent(indexName, ignored -> new HashMap<>())
                    .computeIfAbsent(constraint.field(), field -> domainProvider.getDomain(clusterState, indexName, field));

                if (maybeDomain.isEmpty()) {
                    continue;
                }

                final FieldDomain domain = maybeDomain.get();

                if (domain.finalized() == false) {
                    continue;
                }

                if (fieldDomainEvaluators.canMatch(domain, constraint, evaluationContext) == false) {
                    prune = true;
                    break;
                }
            }

            if (prune) {
                prunedShardGroupIndexes.set(shardGroupIndex);
                pruned++;
            }
        }

        if (pruned == 0) {
            return SearchIndexPruningResult.notPruned(shardIterators);
        }

        if (pruned == activeShardGroups) {
            /*
             * Pruning is an optimization. If every currently active shard group would be skipped, fall back to
             * the original iterators until zero-shard response semantics are covered explicitly for this
             * pre-can-match stage. Existing skip flags are preserved because they may have been set by another
             * search execution step.
             */
            return SearchIndexPruningResult.notPruned(shardIterators);
        }

        return SearchIndexPruningResult.pruned(shardIterators, prunedShardGroupIndexes);
    }

    private static boolean shouldSkip(SearchRequest request, int activeShardGroups, SearchIndexPruningConfig config) {
        if (config.enabled == false) {
            return true;
        }

        if (request == null || request.source() == null) {
            return true;
        }

        if (request.pointInTimeBuilder() != null) {
            // PIT searches must preserve the shard set captured by the PIT creation snapshot.
            return true;
        }

        if (activeShardGroups < config.minShards) {
            return true;
        }
        return config.fields.isEmpty();
    }

    private static boolean shouldAlwaysKeep(SearchShardIterator shardIterator) {
        return shardIterator.getClusterAlias() != null;
    }

    private static int size(GroupShardsIterator<SearchShardIterator> iterators) {
        return iterators.size();
    }

    private static int activeShardGroups(GroupShardsIterator<SearchShardIterator> iterators) {
        int activeShardGroups = 0;
        for (int i = 0; i < iterators.size(); i++) {
            if (iterators.get(i).skip() == false) {
                activeShardGroups++;
            }
        }
        return activeShardGroups;
    }

    private static final class SearchIndexPruningConfig {
        private final boolean enabled;
        private final int minShards;
        private final Set<String> fields;

        private SearchIndexPruningConfig(boolean enabled, int minShards, Collection<String> fields) {
            this.enabled = enabled;
            this.minShards = minShards;
            this.fields = Set.copyOf(fields);
        }

        private static SearchIndexPruningConfig fromClusterSettings(ClusterSettings clusterSettings) {
            return new SearchIndexPruningConfig(
                clusterSettings.get(SearchIndexPruningSettings.ENABLED),
                clusterSettings.get(SearchIndexPruningSettings.MIN_SHARDS),
                clusterSettings.get(SearchIndexPruningSettings.FIELDS)
            );
        }

        private static SearchIndexPruningConfig fromSettings(Settings settings) {
            return new SearchIndexPruningConfig(
                SearchIndexPruningSettings.ENABLED.get(settings),
                SearchIndexPruningSettings.MIN_SHARDS.get(settings),
                SearchIndexPruningSettings.FIELDS.get(settings)
            );
        }
    }
}

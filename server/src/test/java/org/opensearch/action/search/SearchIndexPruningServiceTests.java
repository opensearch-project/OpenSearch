/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.pruning.FieldDomainEvaluationContext;
import org.opensearch.action.search.pruning.FieldDomainEvaluators;
import org.opensearch.action.search.pruning.QueryConstraint;
import org.opensearch.action.search.pruning.QueryConstraintExtractor;
import org.opensearch.action.search.pruning.SearchIndexPruningResult;
import org.opensearch.action.search.pruning.SearchIndexPruningSettings;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddomain.ClusterStateFieldDomainProvider;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.FieldDomain;
import org.opensearch.index.fielddomain.FieldDomainProvider;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class SearchIndexPruningServiceTests extends OpenSearchTestCase {
    private static final String FIELD = "field_a";

    public void testPrunesOnlyProvablyDisjointLocalShardGroups() {
        SearchShardIterator oldIndex = shardIterator("logs-000001", 0);
        SearchShardIterator matchingIndex = shardIterator("logs-000002", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(oldIndex, matchingIndex));
        SearchIndexPruningService service = serviceWithDomains(
            Map.of("logs-000001", Optional.of(domain(false)), "logs-000002", Optional.of(domain(true)))
        );

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertTrue(result.pruned());
        assertThat(result.originalShardGroups(), equalTo(2));
        assertThat(result.prunedShardGroups(), equalTo(1));
        assertTrue(result.isPrunedShardGroup(0));
        assertFalse(result.isPrunedShardGroup(1));
        assertFalse(oldIndex.skip());
        assertFalse(matchingIndex.skip());

        applyPruningResult(result);

        assertTrue(oldIndex.skip());
        assertFalse(matchingIndex.skip());
    }

    public void testKeepsShardGroupsWhenDomainIsMissingOrUnfinalized() {
        SearchShardIterator missingMetadata = shardIterator("logs-000001", 0);
        SearchShardIterator unfinalizedDomain = shardIterator("logs-000002", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(missingMetadata, unfinalizedDomain));
        SearchIndexPruningService service = serviceWithDomains(
            Map.of("logs-000001", Optional.empty(), "logs-000002", Optional.of(domain(false, false)))
        );

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertFalse(result.pruned());
        assertFalse(missingMetadata.skip());
        assertFalse(unfinalizedDomain.skip());
    }

    public void testKeepsRemoteShardGroups() {
        SearchShardIterator remoteIndex = shardIterator("remote-cluster", "logs-000001", 0);
        SearchShardIterator localIndex = shardIterator("logs-000002", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(remoteIndex, localIndex));
        SearchIndexPruningService service = serviceWithDomains(
            Map.of("logs-000001", Optional.of(domain(false)), "logs-000002", Optional.of(domain(false)))
        );

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertTrue(result.pruned());
        assertFalse(result.isPrunedShardGroup(0));
        assertTrue(result.isPrunedShardGroup(1));
        assertFalse(remoteIndex.skip());
        assertFalse(localIndex.skip());
    }

    public void testPruneIsFailOpenWhenDomainProviderThrows() {
        AtomicInteger lookups = new AtomicInteger();
        FieldDomainProvider provider = (clusterState, indexName, field) -> {
            lookups.incrementAndGet();
            throw new IllegalStateException("exception");
        };
        SearchIndexPruningService service = new SearchIndexPruningService(
            clusterSettings(),
            provider,
            genericConstraintExtractor(),
            genericEvaluators()
        );

        SearchShardIterator shardIterator = shardIterator("logs-000001", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(shardIterator));

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertThat(lookups.get(), equalTo(1));
        assertFalse(result.pruned());
        assertThat(result.originalShardGroups(), equalTo(1));
        assertFalse(shardIterator.skip());
    }

    public void testFallsBackWhenAllShardGroupsWouldBePruned() {
        SearchShardIterator first = shardIterator("logs-000001", 0);
        SearchShardIterator second = shardIterator("logs-000002", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(first, second));
        SearchIndexPruningService service = serviceWithDomains(
            Map.of("logs-000001", Optional.of(domain(false)), "logs-000002", Optional.of(domain(false)))
        );

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertFalse(result.pruned());
        assertThat(result.prunedShardGroups(), equalTo(0));
        assertFalse(first.skip());
        assertFalse(second.skip());
    }

    public void testFallsBackWhenAllActiveShardGroupsWouldBePrunedAndPreservesExistingSkips() {
        SearchShardIterator alreadySkipped = shardIterator("logs-000001", 0);
        SearchShardIterator activeCandidate = shardIterator("logs-000002", 0);
        alreadySkipped.resetAndSkip();
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(alreadySkipped, activeCandidate));
        SearchIndexPruningService service = serviceWithDomains(
            Map.of("logs-000001", Optional.empty(), "logs-000002", Optional.of(domain(false)))
        );

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertFalse(result.pruned());
        assertThat(result.prunedShardGroups(), equalTo(0));
        assertTrue(alreadySkipped.skip());
        assertFalse(activeCandidate.skip());
    }

    public void testDoesNotPrunePointInTimeSearches() {
        SearchShardIterator oldIndex = shardIterator("logs-000001", 0);
        SearchShardIterator matchingIndex = shardIterator("logs-000002", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(oldIndex, matchingIndex));
        SearchIndexPruningService service = serviceWithDomains(
            Map.of("logs-000001", Optional.of(domain(false)), "logs-000002", Optional.of(domain(true)))
        );

        SearchRequest request = searchRequest();
        request.source().pointInTimeBuilder(new PointInTimeBuilder("pit-id"));

        SearchIndexPruningResult result = service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertFalse(result.pruned());
        assertFalse(oldIndex.skip());
        assertFalse(matchingIndex.skip());
    }

    public void testPruningSettingsAreRegisteredAsBuiltInClusterSettings() {
        assertTrue(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.containsAll(SearchIndexPruningSettings.getSettings()));
    }

    public void testAppliesDynamicPruningSettingsAsConsistentConfigSnapshot() {
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        SearchIndexPruningService service = serviceWithDomains(
            clusterSettings,
            Map.of("logs-000001", Optional.of(domain(false)), "logs-000002", Optional.of(domain(true)))
        );

        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(
            List.of(shardIterator("logs-000001", 0), shardIterator("logs-000002", 0))
        );
        SearchRequest request = searchRequest();

        assertFalse(service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext()).pruned());

        clusterSettings.applySettings(pruningSettings(true, 1, FIELD));
        assertTrue(service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext()).isPrunedShardGroup(0));

        clusterSettings.applySettings(pruningSettings(false, 1, FIELD));
        assertFalse(service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext()).pruned());

        clusterSettings.applySettings(pruningSettings(true, 1, "event.ingested"));
        assertFalse(service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext()).pruned());

        clusterSettings.applySettings(pruningSettings(true, 3, FIELD));
        assertFalse(service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext()).pruned());

        clusterSettings.applySettings(pruningSettings(true, 1, FIELD));
        assertTrue(service.prune(request, shardIterators, ClusterState.EMPTY_STATE, evaluationContext()).isPrunedShardGroup(0));
    }

    public void testMinShardThresholdUsesActiveShardGroups() {
        AtomicInteger lookups = new AtomicInteger();
        FieldDomainProvider provider = (clusterState, indexName, field) -> {
            lookups.incrementAndGet();
            return Optional.of(domain(false));
        };
        SearchIndexPruningService service = new SearchIndexPruningService(
            clusterSettings(pruningSettings(true, 2, FIELD)),
            provider,
            genericConstraintExtractor(),
            genericEvaluators()
        );

        SearchShardIterator alreadySkipped = shardIterator("logs-000001", 0);
        SearchShardIterator activeCandidate = shardIterator("logs-000002", 0);
        alreadySkipped.resetAndSkip();
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(alreadySkipped, activeCandidate));

        SearchIndexPruningResult result = service.prune(searchRequest(), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertFalse(result.pruned());
        assertThat(lookups.get(), equalTo(0));
    }

    public void testCachesBoundsLookupPerIndexAndFieldDuringSinglePruningPass() {
        AtomicInteger lookups = new AtomicInteger();
        FieldDomainProvider provider = (clusterState, indexName, field) -> {
            lookups.incrementAndGet();
            return Optional.of(new TestFieldDomain(field));
        };
        QueryConstraintExtractor extractor = (source, fields) -> List.of(new TestQueryConstraint(FIELD));
        FieldDomainEvaluators evaluators = new FieldDomainEvaluators(List.of((bounds, constraint, context) -> false));
        SearchIndexPruningService service = new SearchIndexPruningService(clusterSettings(), provider, extractor, evaluators);

        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(
            List.of(shardIterator("logs-000001", 0), shardIterator("logs-000001", 1))
        );

        service.prune(new SearchRequest().source(new SearchSourceBuilder()), shardIterators, ClusterState.EMPTY_STATE, evaluationContext());

        assertThat(lookups.get(), equalTo(1));
    }

    public void testWiresDateRangeMetadataFromClusterStateProvider() {
        SearchIndexPruningService service = new SearchIndexPruningService(dateClusterSettings(), new ClusterStateFieldDomainProvider());

        SearchShardIterator oldIndex = shardIterator("logs-000001", 0);
        SearchShardIterator matchingIndex = shardIterator("logs-000002", 0);
        GroupShardsIterator<SearchShardIterator> shardIterators = new GroupShardsIterator<>(List.of(oldIndex, matchingIndex));

        ClusterState clusterState = clusterState(
            indexMetadata("logs-000001", new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test")),
            indexMetadata("logs-000002", new DateRangeFieldDomain("@timestamp", 350L, 450L, true, "test"))
        );

        SearchIndexPruningResult result = service.prune(
            dateRangeSearchRequest(300L, 400L),
            shardIterators,
            clusterState,
            evaluationContext()
        );

        assertTrue(result.pruned());
        assertTrue(result.isPrunedShardGroup(0));
        assertFalse(result.isPrunedShardGroup(1));
    }

    private static SearchRequest searchRequest() {
        return new SearchRequest().source(new SearchSourceBuilder());
    }

    private static SearchRequest dateRangeSearchRequest(long from, long to) {
        return new SearchRequest().source(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("@timestamp").gte(from).lte(to)));
    }

    private static FieldDomainEvaluationContext evaluationContext() {
        return new FieldDomainEvaluationContext(() -> 0L);
    }

    private static ClusterSettings clusterSettings() {
        Settings settings = Settings.builder()
            .put(SearchIndexPruningSettings.ENABLED.getKey(), true)
            .put(SearchIndexPruningSettings.MIN_SHARDS.getKey(), 1)
            .putList(SearchIndexPruningSettings.FIELDS.getKey(), FIELD)
            .build();
        return clusterSettings(settings);
    }

    private static ClusterSettings dateClusterSettings() {
        Settings settings = Settings.builder()
            .put(SearchIndexPruningSettings.ENABLED.getKey(), true)
            .put(SearchIndexPruningSettings.MIN_SHARDS.getKey(), 1)
            .putList(SearchIndexPruningSettings.FIELDS.getKey(), "@timestamp")
            .build();
        return clusterSettings(settings);
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        Set<Setting<?>> builtInSettings = Set.copyOf(SearchIndexPruningSettings.getSettings());
        return new ClusterSettings(settings, builtInSettings);
    }

    private static Settings pruningSettings(boolean enabled, int minShards, String... fields) {
        return Settings.builder()
            .put(SearchIndexPruningSettings.ENABLED.getKey(), enabled)
            .put(SearchIndexPruningSettings.MIN_SHARDS.getKey(), minShards)
            .putList(SearchIndexPruningSettings.FIELDS.getKey(), fields)
            .build();
    }

    private static ClusterState clusterState(IndexMetadata... indexMetadata) {
        Metadata.Builder metadata = Metadata.builder();
        for (IndexMetadata metadataEntry : indexMetadata) {
            metadata.put(metadataEntry, false);
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
    }

    private static IndexMetadata indexMetadata(String index, DateRangeFieldDomain domain) {
        return IndexFieldDomainMetadata.getInstance().putFieldDomain(plainIndexMetadata(index), domain);
    }

    private static IndexMetadata plainIndexMetadata(String index) {
        return indexMetadataBuilder(index).build();
    }

    private static IndexMetadata.Builder indexMetadataBuilder(String index) {
        return IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_na_")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0);
    }

    private static SearchShardIterator shardIterator(String index, int shardId) {
        return shardIterator(null, index, shardId);
    }

    private static SearchShardIterator shardIterator(String clusterAlias, String index, int shardId) {
        return new SearchShardIterator(
            clusterAlias,
            new ShardId(index, "_na_", shardId),
            List.of("node-1"),
            OriginalIndices.NONE,
            null,
            null
        );
    }

    private static void applyPruningResult(SearchIndexPruningResult pruningResult) {
        for (int shardGroupIndex = 0; shardGroupIndex < pruningResult.originalShardGroups(); shardGroupIndex++) {
            if (pruningResult.isPrunedShardGroup(shardGroupIndex)) {
                pruningResult.shardIterators().get(shardGroupIndex).resetAndSkip();
            }
        }
    }

    private static SearchIndexPruningService serviceWithDomains(Map<String, Optional<FieldDomain>> domainsByIndex) {
        return serviceWithDomains(clusterSettings(), domainsByIndex);
    }

    private static SearchIndexPruningService serviceWithDomains(
        ClusterSettings clusterSettings,
        Map<String, Optional<FieldDomain>> domainsByIndex
    ) {
        FieldDomainProvider provider = (clusterState, indexName, field) -> domainsByIndex.getOrDefault(indexName, Optional.empty());
        return new SearchIndexPruningService(clusterSettings, provider, genericConstraintExtractor(), genericEvaluators());
    }

    private static QueryConstraintExtractor genericConstraintExtractor() {
        return (source, fields) -> fields.contains(FIELD) ? List.of(new TestQueryConstraint(FIELD)) : List.of();
    }

    private static FieldDomainEvaluators genericEvaluators() {
        return new FieldDomainEvaluators(
            List.of((domain, constraint, context) -> domain instanceof TestFieldDomain ? ((TestFieldDomain) domain).canMatch() : true)
        );
    }

    private static TestFieldDomain domain(boolean canMatch) {
        return domain(true, canMatch);
    }

    private static TestFieldDomain domain(boolean finalized, boolean canMatch) {
        return new TestFieldDomain(FIELD, finalized, canMatch);
    }

    private static final class TestQueryConstraint implements QueryConstraint {
        private final String field;

        private TestQueryConstraint(String field) {
            this.field = field;
        }

        @Override
        public String field() {
            return field;
        }
    }

    private static final class TestFieldDomain implements FieldDomain {
        private final String field;
        private final boolean finalized;
        private final boolean canMatch;

        private TestFieldDomain(String field) {
            this(field, true, true);
        }

        private TestFieldDomain(String field, boolean finalized, boolean canMatch) {
            this.field = field;
            this.finalized = finalized;
            this.canMatch = canMatch;
        }

        @Override
        public String field() {
            return field;
        }

        @Override
        public String type() {
            return "test";
        }

        @Override
        public boolean finalized() {
            return finalized;
        }

        private boolean canMatch() {
            return canMatch;
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.rollover;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsTests;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DataStreamTestHelper;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.MetadataIndexAliasesService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.get.GetStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.warmer.WarmerStats;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.ArgumentCaptor;

import static java.util.Collections.emptyList;
import static org.opensearch.action.admin.indices.rollover.TransportRolloverAction.evaluateConditions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportRolloverActionTests extends OpenSearchTestCase {

    public void testDocStatsSelectionFromPrimariesOnly() {
        long docsInPrimaryShards = 100;
        long docsInShards = 200;

        final Condition<?> condition = createTestCondition();
        String indexName = randomAlphaOfLengthBetween(5, 7);
        evaluateConditions(
            Sets.newHashSet(condition),
            createMetadata(indexName),
            createIndicesStatResponse(indexName, docsInShards, docsInPrimaryShards)
        );
        final ArgumentCaptor<Condition.Stats> argument = ArgumentCaptor.forClass(Condition.Stats.class);
        verify(condition).evaluate(argument.capture());

        assertEquals(docsInPrimaryShards, argument.getValue().numDocs);
    }

    public void testEvaluateConditions() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB));

        long matchMaxDocs = randomIntBetween(100, 1000);
        long notMatchMaxDocs = randomIntBetween(0, 99);
        ByteSizeValue notMatchMaxSize = new ByteSizeValue(randomIntBetween(0, 9), ByteSizeUnit.MB);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
        final Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        Map<String, Boolean> results = evaluateConditions(
            conditions,
            new DocsStats.Builder().count(matchMaxDocs).deleted(0L).totalSizeInBytes(ByteSizeUnit.MB.toBytes(120)).build(),
            metadata
        );
        assertThat(results.size(), equalTo(3));
        for (Boolean matched : results.values()) {
            assertThat(matched, equalTo(true));
        }

        results = evaluateConditions(
            conditions,
            new DocsStats.Builder().count(notMatchMaxDocs).deleted(0).totalSizeInBytes(notMatchMaxSize.getBytes()).build(),
            metadata
        );
        assertThat(results.size(), equalTo(3));
        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutDocStats() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(randomNonNegativeLong());
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(randomIntBetween(1, 3)));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong()));

        Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 1000))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(10))
            .build();

        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        Map<String, Boolean> results = evaluateConditions(conditions, null, metadata);
        assertThat(results.size(), equalTo(3));

        for (Map.Entry<String, Boolean> entry : results.entrySet()) {
            if (entry.getKey().equals(maxAgeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(true));
            } else if (entry.getKey().equals(maxDocsCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else if (entry.getKey().equals(maxSizeCondition.toString())) {
                assertThat(entry.getValue(), equalTo(false));
            } else {
                fail("unknown condition result found " + entry.getKey());
            }
        }
    }

    public void testEvaluateWithoutMetadata() {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(100L);
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(TimeValue.timeValueHours(2));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(randomIntBetween(10, 100), ByteSizeUnit.MB));

        long matchMaxDocs = randomIntBetween(100, 1000);
        final Set<Condition<?>> conditions = Sets.newHashSet(maxDocsCondition, maxAgeCondition, maxSizeCondition);

        Map<String, Boolean> results = evaluateConditions(
            conditions,
            new DocsStats.Builder().count(matchMaxDocs).deleted(0L).totalSizeInBytes(ByteSizeUnit.MB.toBytes(120)).build(),
            null
        );
        assertThat(results.size(), equalTo(3));
        results.forEach((k, v) -> assertFalse(v));

        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 1000))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(10))
            .build();

        final IndexMetadata metadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(randomIntBetween(5, 10)).getMillis())
            .settings(settings)
            .build();
        IndicesStatsResponse indicesStats = randomIndicesStatsResponse(new IndexMetadata[] { metadata });
        Map<String, Boolean> results2 = evaluateConditions(conditions, null, indicesStats);
        assertThat(results2.size(), equalTo(3));
        results2.forEach((k, v) -> assertFalse(v));
    }

    public void testConditionEvaluationWhenAliasToWriteAndReadIndicesConsidersOnlyPrimariesFromWriteIndex() throws Exception {
        final TransportService mockTransportService = mock(TransportService.class);
        final ClusterService mockClusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(mockClusterService.localNode()).thenReturn(mockNode);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        final MetadataCreateIndexService mockCreateIndexService = mock(MetadataCreateIndexService.class);
        final IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        when(mockIndexNameExpressionResolver.resolveDateMathExpression(any())).thenReturn("logs-index-000003");
        final ActionFilters mockActionFilters = mock(ActionFilters.class);
        final MetadataIndexAliasesService mdIndexAliasesService = mock(MetadataIndexAliasesService.class);

        final Client mockClient = mock(Client.class);

        final Map<String, IndexStats> indexStats = new HashMap<>();
        int total = randomIntBetween(500, 1000);
        indexStats.put("logs-index-000001", createIndexStats(200L, total));
        indexStats.put("logs-index-000002", createIndexStats(300L, total));
        final IndicesStatsResponse statsResponse = createAliasToMultipleIndicesStatsResponse(indexStats);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) args[2];
            listener.onResponse(statsResponse);
            return null;
        }).when(mockClient).execute(eq(IndicesStatsAction.INSTANCE), any(ActionRequest.class), any(ActionListener.class));

        assert statsResponse.getPrimaries().getDocs().getCount() == 500L;
        assert statsResponse.getTotal().getDocs().getCount() == (total + total);

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("logs-index-000001")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(false).build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("logs-index-000002")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(true).build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        final ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2))
            .build();

        when(mockCreateIndexService.applyCreateIndexRequest(any(), any(), anyBoolean())).thenReturn(stateBefore);
        when(mdIndexAliasesService.applyAliasActions(any(), any())).thenReturn(stateBefore);
        MetadataRolloverService rolloverService = new MetadataRolloverService(
            mockThreadPool,
            mockCreateIndexService,
            mdIndexAliasesService,
            mockIndexNameExpressionResolver
        );
        final TransportRolloverAction transportRolloverAction = new TransportRolloverAction(
            mockTransportService,
            mockClusterService,
            mockThreadPool,
            mockActionFilters,
            mockIndexNameExpressionResolver,
            rolloverService,
            mockClient
        );

        // For given alias, verify that condition evaluation fails when the condition doc count is greater than the primaries doc count
        // (primaries from only write index is considered)
        PlainActionFuture<RolloverResponse> future = new PlainActionFuture<>();
        RolloverRequest rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.addMaxIndexDocsCondition(500L);
        rolloverRequest.dryRun(true);
        transportRolloverAction.clusterManagerOperation(mock(Task.class), rolloverRequest, stateBefore, future);

        RolloverResponse response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 500]"), is(false));

        // For given alias, verify that the condition evaluation is successful when condition doc count is less than the primaries doc count
        // (primaries from only write index is considered)
        future = new PlainActionFuture<>();
        rolloverRequest = new RolloverRequest("logs-alias", "logs-index-000003");
        rolloverRequest.addMaxIndexDocsCondition(300L);
        rolloverRequest.dryRun(true);
        transportRolloverAction.clusterManagerOperation(mock(Task.class), rolloverRequest, stateBefore, future);

        response = future.actionGet();
        assertThat(response.getOldIndex(), equalTo("logs-index-000002"));
        assertThat(response.getNewIndex(), equalTo("logs-index-000003"));
        assertThat(response.isDryRun(), equalTo(true));
        assertThat(response.isRolledOver(), equalTo(false));
        assertThat(response.getConditionStatus().size(), equalTo(1));
        assertThat(response.getConditionStatus().get("[max_docs: 300]"), is(true));
    }

    public void testResolveIndices() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("logs-index-000001")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(false).build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        IndexMetadata.Builder indexMetadata2 = IndexMetadata.builder("logs-index-000002")
            .putAlias(AliasMetadata.builder("logs-alias").writeIndex(true).build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        ClusterState stateBefore = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata).put(indexMetadata2))
            .build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(stateBefore);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        MetadataCreateIndexService createIndexService = mock(MetadataCreateIndexService.class);
        MetadataIndexAliasesService metadataIndexAliasesService = mock(MetadataIndexAliasesService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        MetadataRolloverService metadataRolloverService = new MetadataRolloverService(
            threadPool,
            createIndexService,
            metadataIndexAliasesService,
            indexNameExpressionResolver
        );

        TransportRolloverAction action = new TransportRolloverAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            indexNameExpressionResolver,
            metadataRolloverService,
            mock(Client.class)
        );

        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new RolloverRequest("logs-alias", null));
            assertEquals(
                ResolvedIndices.of("logs-alias")
                    .withLocalSubActions(CreateIndexAction.INSTANCE, ResolvedIndices.Local.of("logs-index-000003")),
                resolvedIndices
            );
        }

        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new RolloverRequest("logs-alias", "explicit-index"));
            assertEquals(
                ResolvedIndices.of("logs-alias")
                    .withLocalSubActions(CreateIndexAction.INSTANCE, ResolvedIndices.Local.of("explicit-index")),
                resolvedIndices
            );
        }
    }

    private IndicesStatsResponse createIndicesStatResponse(String indexName, long totalDocs, long primariesDocs) {
        final CommonStats primaryStats = mock(CommonStats.class);
        when(primaryStats.getDocs()).thenReturn(
            new DocsStats.Builder().count(primariesDocs).deleted(0).totalSizeInBytes(between(1, 10000)).build()
        );

        final CommonStats totalStats = mock(CommonStats.class);
        when(totalStats.getDocs()).thenReturn(
            new DocsStats.Builder().count(totalDocs).deleted(0).totalSizeInBytes(between(1, 10000)).build()
        );

        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        when(response.getPrimaries()).thenReturn(primaryStats);
        when(response.getTotal()).thenReturn(totalStats);
        final IndexStats indexStats = mock(IndexStats.class);
        when(response.getIndex(indexName)).thenReturn(indexStats);
        when(indexStats.getPrimaries()).thenReturn(primaryStats);
        when(indexStats.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndicesStatsResponse createAliasToMultipleIndicesStatsResponse(Map<String, IndexStats> indexStats) {
        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        final CommonStats primariesStats = new CommonStats();
        final CommonStats totalStats = new CommonStats();
        for (String indexName : indexStats.keySet()) {
            when(response.getIndex(indexName)).thenReturn(indexStats.get(indexName));
            primariesStats.add(indexStats.get(indexName).getPrimaries());
            totalStats.add(indexStats.get(indexName).getTotal());
        }

        when(response.getPrimaries()).thenReturn(primariesStats);
        when(response.getTotal()).thenReturn(totalStats);
        return response;
    }

    private IndexStats createIndexStats(long primaries, long total) {
        final CommonStats primariesCommonStats = mock(CommonStats.class);
        when(primariesCommonStats.getDocs()).thenReturn(
            new DocsStats.Builder().count(primaries).deleted(0).totalSizeInBytes(between(1, 10000)).build()
        );

        final CommonStats totalCommonStats = mock(CommonStats.class);
        when(totalCommonStats.getDocs()).thenReturn(
            new DocsStats.Builder().count(total).deleted(0).totalSizeInBytes(between(1, 10000)).build()
        );

        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getPrimaries()).thenReturn(primariesCommonStats);
        when(indexStats.getTotal()).thenReturn(totalCommonStats);
        return indexStats;
    }

    private static IndexMetadata createMetadata(String indexName) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        return IndexMetadata.builder(indexName)
            .creationDate(System.currentTimeMillis() - TimeValue.timeValueHours(3).getMillis())
            .settings(settings)
            .build();
    }

    private static Condition<?> createTestCondition() {
        final Condition<?> condition = mock(Condition.class);
        when(condition.evaluate(any())).thenReturn(new Condition.Result(condition, true));
        return condition;
    }

    private TransportRolloverAction newRolloverActionForCheckBlock(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        MetadataRolloverService rolloverService = new MetadataRolloverService(
            threadPool,
            mock(MetadataCreateIndexService.class),
            mock(MetadataIndexAliasesService.class),
            resolver
        );
        return new TransportRolloverAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            resolver,
            rolloverService,
            mock(Client.class)
        );
    }

    public void testCheckBlockSkipsBlockOnNonWriteAliasMember() {
        // Given: an alias whose write index is unblocked, and a non-write
        // alias member that carries a METADATA_WRITE block (CCR-style)
        String alias = "logs-alias";
        String writeIndexName = "logs-000002";
        String nonWriteMember = "logs-000001";

        IndexMetadata writeIndex = IndexMetadata.builder(writeIndexName)
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder(alias).writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata blockedNonWriteMember = IndexMetadata.builder(nonWriteMember)
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder(alias).writeIndex(false).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(writeIndex, false).put(blockedNonWriteMember, false))
            // INDEX_WRITE_BLOCK has level WRITE only and would not trip checkBlock's METADATA_WRITE
            // probe; INDEX_METADATA_BLOCK is the closest standard constant that includes METADATA_WRITE,
            // matching the level CCR's INDEX_REPLICATION_BLOCK uses.
            .blocks(ClusterBlocks.builder().addIndexBlock(nonWriteMember, IndexMetadata.INDEX_METADATA_BLOCK).build())
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs for a rollover targeting the alias
        ClusterBlockException result = action.checkBlock(new RolloverRequest(alias, null), state);

        // Then: returns null (the bug fix — non-write member is irrelevant)
        assertThat(result, is(nullValue()));
    }

    public void testCheckBlockAbortsWhenWriteIndexHasMetadataWriteBlock() {
        // Given: an alias whose write index has a METADATA_WRITE block applied
        String alias = "logs-alias";
        String writeIndexName = "logs-000002";

        IndexMetadata writeIndex = IndexMetadata.builder(writeIndexName)
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder(alias).writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(writeIndex, false))
            // INDEX_METADATA_BLOCK includes METADATA_WRITE level (see sibling test for rationale).
            .blocks(ClusterBlocks.builder().addIndexBlock(writeIndexName, IndexMetadata.INDEX_METADATA_BLOCK).build())
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs
        ClusterBlockException result = action.checkBlock(new RolloverRequest(alias, null), state);

        // Then: returns a ClusterBlockException naming only the write index
        // (regression guard for the still-correct abort path)
        assertThat(result, is(notNullValue()));
        assertThat(result.getMessage(), containsString(writeIndexName));
    }

    public void testCheckBlockSkipsBlockOnNonWriteDataStreamBacking() {
        // Given: a data stream with multiple backing indices; an older backing
        // (not the write backing) has a METADATA_WRITE block
        String dataStreamName = "logs-ds";
        int generations = 2;

        ClusterState baseState = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, generations)),
            List.of()
        );

        // The write backing is the highest-generation index; older backings come first.
        String olderBacking = baseState.metadata().dataStreams().get(dataStreamName).getIndices().getFirst().getName();

        ClusterState state = ClusterState.builder(baseState)
            // INDEX_METADATA_BLOCK includes METADATA_WRITE level (see sibling test for rationale).
            .blocks(ClusterBlocks.builder().addIndexBlock(olderBacking, IndexMetadata.INDEX_METADATA_BLOCK).build())
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs for a rollover targeting the data stream
        ClusterBlockException result = action.checkBlock(new RolloverRequest(dataStreamName, null), state);

        // Then: returns null (data-stream parity with the alias case)
        assertThat(result, is(nullValue()));
    }

    public void testCheckBlockSkipsRemoteSnapshotWriteIndexWithoutUserReadOnlyBlock() {
        // Given: an alias whose write index is a remote_snapshot. Such indices come with
        // an implicit METADATA_WRITE block; without a user-set read-only setting, the
        // precheck should defer (parity with ClusterBlocks#indicesWithRemoteSnapshotBlockedException).
        String alias = "logs-alias";
        String writeIndexName = "logs-000002";

        Settings remoteSnapshotSettings = Settings.builder()
            .put(settings(Version.CURRENT).build())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
            .build();

        IndexMetadata writeIndex = IndexMetadata.builder(writeIndexName)
            .settings(remoteSnapshotSettings)
            .putAlias(AliasMetadata.builder(alias).writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(writeIndex, false))
            .blocks(ClusterBlocks.builder().addIndexBlock(writeIndexName, IndexMetadata.INDEX_METADATA_BLOCK).build())
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs
        ClusterBlockException result = action.checkBlock(new RolloverRequest(alias, null), state);

        // Then: returns null (the implicit remote_snapshot block is exempted)
        assertThat(result, is(nullValue()));
    }

    public void testCheckBlockAbortsWhenRemoteSnapshotHasUserReadOnlyBlock() {
        // Given: a remote_snapshot write index with a user-set INDEX_READ_ONLY_SETTING.
        // That's an intentional read-only block, so the precheck must still abort.
        String alias = "logs-alias";
        String writeIndexName = "logs-000002";

        Settings remoteSnapshotSettings = Settings.builder()
            .put(settings(Version.CURRENT).build())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
            .put(IndexMetadata.SETTING_READ_ONLY, true)
            .build();

        IndexMetadata writeIndex = IndexMetadata.builder(writeIndexName)
            .settings(remoteSnapshotSettings)
            .putAlias(AliasMetadata.builder(alias).writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(writeIndex, false))
            .blocks(ClusterBlocks.builder().addIndexBlock(writeIndexName, IndexMetadata.INDEX_METADATA_BLOCK).build())
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs
        ClusterBlockException result = action.checkBlock(new RolloverRequest(alias, null), state);

        // Then: aborts naming the write index (user opted in to read-only)
        assertThat(result, is(notNullValue()));
        assertThat(result.getMessage(), containsString(writeIndexName));
    }

    public void testCheckBlockReturnsNullWhenRolloverTargetUnresolvable() {
        // Given: cluster state with no abstraction matching the rollover target
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()).build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs
        ClusterBlockException result = action.checkBlock(new RolloverRequest("does-not-exist", null), state);

        // Then: returns null (defers to main path's canonical "not found" error)
        assertThat(result, is(nullValue()));
    }

    public void testCheckBlockReturnsNullWhenAliasHasNoWriteMember() {
        // Given: an alias with members but none marked is_write_index=true.
        // (Multiple non-write members with no write member is the only shape that makes
        // Alias#getWriteIndex() return null; a single non-write member is implicitly the write index.)
        String alias = "logs-alias";
        String memberA = "logs-000001";
        String memberB = "logs-000002";

        IndexMetadata indexA = IndexMetadata.builder(memberA)
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder(alias).writeIndex(false).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexMetadata indexB = IndexMetadata.builder(memberB)
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder(alias).writeIndex(false).build())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexA, false).put(indexB, false))
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs
        ClusterBlockException result = action.checkBlock(new RolloverRequest(alias, null), state);

        // Then: returns null — defers to MetadataRolloverService's canonical
        // "rollover target [...] does not point to a write index" error.
        assertThat(result, is(nullValue()));
    }

    public void testCheckBlockAbortsWhenDataStreamWriteBackingBlocked() {
        // Given: a data stream where the write (highest-generation) backing carries a METADATA_WRITE block
        String dataStreamName = "logs-ds";
        int generations = 2;

        ClusterState baseState = DataStreamTestHelper.getClusterStateWithDataStreams(
            List.of(new Tuple<>(dataStreamName, generations)),
            List.of()
        );

        // Write backing is the highest-generation index — the last in the indices list.
        List<org.opensearch.core.index.Index> backings = baseState.metadata().dataStreams().get(dataStreamName).getIndices();
        String writeBacking = backings.getLast().getName();

        ClusterState state = ClusterState.builder(baseState)
            .blocks(ClusterBlocks.builder().addIndexBlock(writeBacking, IndexMetadata.INDEX_METADATA_BLOCK).build())
            .build();

        TransportRolloverAction action = newRolloverActionForCheckBlock(state);

        // When: checkBlock runs for a rollover targeting the data stream
        ClusterBlockException result = action.checkBlock(new RolloverRequest(dataStreamName, null), state);

        // Then: aborts naming the write backing — symmetry with the alias write-index-blocked case.
        assertThat(result, is(notNullValue()));
        assertThat(result.getMessage(), containsString(writeBacking));
    }

    public static IndicesStatsResponse randomIndicesStatsResponse(final IndexMetadata[] indices) {
        List<ShardStats> shardStats = new ArrayList<>();
        for (final IndexMetadata index : indices) {
            int numShards = randomIntBetween(1, 3);
            int primaryIdx = randomIntBetween(-1, numShards - 1); // -1 means there is no primary shard.
            for (int i = 0; i < numShards; i++) {
                ShardId shardId = new ShardId(index.getIndex(), i);
                boolean primary = (i == primaryIdx);
                Path path = createTempDir().resolve("indices").resolve(index.getIndexUUID()).resolve(String.valueOf(i));
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    primary,
                    primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
                );
                shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                shardRouting = shardRouting.moveToStarted();
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats();
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();
                shardStats.add(
                    new ShardStats.Builder().shardRouting(shardRouting)
                        .shardPath(new ShardPath(false, path, path, shardId))
                        .commonStats(stats)
                        .commitStats(null)
                        .seqNoStats(null)
                        .retentionLeaseStats(null)
                        .pollingIngestStats(null)
                        .build()
                );
            }
        }
        return IndicesStatsTests.newIndicesStatsResponse(
            shardStats.toArray(new ShardStats[0]),
            shardStats.size(),
            shardStats.size(),
            0,
            emptyList()
        );
    }
}

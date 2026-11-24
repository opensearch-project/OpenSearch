/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.get;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DocumentLevelReadAccessPolicy;
import org.opensearch.action.support.ReadAccessContext;
import org.opensearch.action.support.ReadAccessPolicyProvider;
import org.opensearch.action.support.ReadAccessPolicyService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.get.ShardGetService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.AccessPolicyProviderPlugin;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.opensearch.common.UUIDs.randomBase64UUID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardMultiGetActionTests extends OpenSearchTestCase {

    public void testShardOperationResolvesPolicyOnceAndPassesItToEveryGet() {
        String indexName = "concrete-index";
        Index index = new Index(indexName, randomBase64UUID());
        ShardId shardId = new ShardId(index, 0);

        DocumentLevelReadAccessPolicy policy = mock(DocumentLevelReadAccessPolicy.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.get(any(Engine.Get.class), same(policy))).thenReturn(Engine.GetResult.NOT_EXISTS);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, Settings.EMPTY);
        ShardGetService shardGetService = new ShardGetService(indexSettings, indexShard, mock(MapperService.class));
        when(indexShard.getService()).thenReturn(shardGetService);
        IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        AtomicInteger policyLookups = new AtomicInteger();
        AtomicReference<ReadAccessContext> policyContext = new AtomicReference<>();
        AccessPolicyProviderPlugin policyPlugin = () -> new ReadAccessPolicyProvider() {
            @Override
            public DocumentLevelReadAccessPolicy getDocumentLevelReadAccessPolicy(ReadAccessContext context) {
                policyLookups.incrementAndGet();
                policyContext.set(context);
                return policy;
            }
        };
        ReadAccessPolicyService policyService = new ReadAccessPolicyService(List.of(policyPlugin));
        policyService.initialize();

        TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(mock(TaskManager.class));
        TransportShardMultiGetAction action = new TransportShardMultiGetAction(
            mock(ClusterService.class),
            transportService,
            indicesService,
            mock(ThreadPool.class),
            new ActionFilters(Set.of()),
            mock(IndexNameExpressionResolver.class),
            policyService
        );

        MultiGetRequest multiGetRequest = new MultiGetRequest().realtime(false);
        MultiGetShardRequest shardRequest = new MultiGetShardRequest(multiGetRequest, "request-alias", shardId.id());
        shardRequest.add(3, new MultiGetRequest.Item("request-alias", "first"));
        shardRequest.add(7, new MultiGetRequest.Item("request-alias", "second"));

        MultiGetShardResponse response = action.shardOperation(shardRequest, shardId);

        assertEquals(1, policyLookups.get());
        assertEquals(List.of(indexName), policyContext.get().concreteIndices());
        ArgumentCaptor<Engine.Get> getCaptor = ArgumentCaptor.forClass(Engine.Get.class);
        verify(indexShard, times(2)).get(getCaptor.capture(), same(policy));
        assertEquals(List.of("first", "second"), getCaptor.getAllValues().stream().map(Engine.Get::id).toList());
        assertEquals(List.of(3, 7), response.locations);
        assertEquals("first", response.responses.get(0).getId());
        assertEquals("second", response.responses.get(1).getId());
        assertFalse(response.responses.get(0).isExists());
        assertFalse(response.responses.get(1).isExists());
        assertEquals(2, response.failures.size());
        assertNull(response.failures.get(0));
        assertNull(response.failures.get(1));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Verifies response-typing mapping resolution on a coordinating-only node (hosting no shard)
 * while dynamic mapping updates arrive from documents indexed on a separate data node. Drives
 * {@link RequestScopedMapperService} against the coordinator's real {@link IndicesService} and
 * cluster state, as {@code TransportDslExecuteAction} wires it; the DSL plugins are not
 * installed because the machinery under test does not depend on the engine.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RequestScopedMapperServiceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "dynamic-probe";

    public void testCoordinatorResolvesPinnedMappingSnapshotUnderDynamicUpdates() throws Exception {
        internalCluster().startNode();
        String coordNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createSingleShardIndex();

        client().prepareIndex(INDEX).setSource("{\"brand\":\"brandX\"}", XContentType.JSON).get();

        ClusterService coordClusterService = internalCluster().getInstance(ClusterService.class, coordNode);
        assertBusy(() -> {
            IndexMetadata metadata = coordClusterService.state().metadata().index(INDEX);
            assertNotNull(metadata);
            assertNotNull(metadata.mapping());
            assertTrue("dynamic mapping for [brand] must reach the coordinator", metadata.mapping().source().string().contains("brand"));
        });

        IndicesService coordIndicesService = internalCluster().getInstance(IndicesService.class, coordNode);
        Index index = coordClusterService.state().metadata().index(INDEX).getIndex();
        assertNull("coordinating-only node must not host the index", coordIndicesService.indexService(index));

        IndexMetadata pinned = coordClusterService.state().metadata().index(INDEX);

        try (RequestScopedMapperService holder = new RequestScopedMapperService(pinned, coordIndicesService::createIndexMapperService)) {
            MapperService mapperService = holder.get();
            assertNotNull(mapperService);
            assertEquals("keyword", mapperService.fieldType("brand.keyword").typeName());
            assertNull("a field the snapshot does not carry must not resolve", mapperService.fieldType("price"));
        }

        client().prepareIndex(INDEX).setSource("{\"brand\":\"brandY\",\"price\":42}", XContentType.JSON).get();
        final long pinnedMappingVersion = pinned.getMappingVersion();
        assertBusy(() -> {
            IndexMetadata current = coordClusterService.state().metadata().index(INDEX);
            assertNotNull(current);
            assertTrue("the [price] mapping update must reach the coordinator", current.getMappingVersion() > pinnedMappingVersion);
        });

        try (RequestScopedMapperService holder = new RequestScopedMapperService(pinned, coordIndicesService::createIndexMapperService)) {
            MapperService mapperService = holder.get();
            assertNotNull(mapperService);
            assertEquals("keyword", mapperService.fieldType("brand.keyword").typeName());
            assertNull("the pinned snapshot must be immune to later dynamic updates", mapperService.fieldType("price"));
        }

        IndexMetadata current = coordClusterService.state().metadata().index(INDEX);
        try (RequestScopedMapperService holder = new RequestScopedMapperService(current, coordIndicesService::createIndexMapperService)) {
            MapperService mapperService = holder.get();
            assertNotNull(mapperService);
            assertEquals("keyword", mapperService.fieldType("brand.keyword").typeName());
            assertEquals("long", mapperService.fieldType("price").typeName());
        }
    }

    public void testPinnedSnapshotSurvivesIndexDeletion() throws Exception {
        internalCluster().startNode();
        String coordNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createSingleShardIndex();

        client().prepareIndex(INDEX).setSource("{\"brand\":\"brandX\"}", XContentType.JSON).get();

        ClusterService coordClusterService = internalCluster().getInstance(ClusterService.class, coordNode);
        assertBusy(() -> {
            IndexMetadata metadata = coordClusterService.state().metadata().index(INDEX);
            assertNotNull(metadata);
            assertNotNull(metadata.mapping());
            assertTrue(metadata.mapping().source().string().contains("brand"));
        });

        IndicesService coordIndicesService = internalCluster().getInstance(IndicesService.class, coordNode);
        IndexMetadata pinned = coordClusterService.state().metadata().index(INDEX);

        assertAcked(client().admin().indices().prepareDelete(INDEX));
        assertBusy(() -> assertNull(coordClusterService.state().metadata().index(INDEX)));

        try (RequestScopedMapperService holder = new RequestScopedMapperService(pinned, coordIndicesService::createIndexMapperService)) {
            MapperService mapperService = holder.get();
            assertNotNull("the pinned snapshot must keep resolving after index deletion", mapperService);
            assertEquals("keyword", mapperService.fieldType("brand.keyword").typeName());
        }
    }

    private void createSingleShardIndex() {
        createIndex(
            INDEX,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(INDEX);
    }
}

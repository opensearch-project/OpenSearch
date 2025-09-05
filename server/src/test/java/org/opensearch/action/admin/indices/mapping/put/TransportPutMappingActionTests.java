/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.mapping.put;

import org.opensearch.Version;
import org.opensearch.action.RequestValidators;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataMappingService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.MappingTransformerRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutMappingActionTests extends OpenSearchTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ThreadPool threadPool;

    @Mock
    private MetadataMappingService metadataMappingService;

    @Mock
    private MappingTransformerRegistry mappingTransformerRegistry;

    @Mock
    private RequestValidators<PutMappingRequest> requestValidators;

    private ClusterState clusterState;

    @Mock
    private ActionListener<AcknowledgedResponse> responseListener;

    private TransportPutMappingAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        IndexMetadata.Builder index = IndexMetadata.builder("index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        IndexMetadata.Builder indexA1 = IndexMetadata.builder("index_a1")
            .putAlias(AliasMetadata.builder("alias_a").build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        IndexMetadata.Builder indexA2 = IndexMetadata.builder("index_a2")
            .putAlias(AliasMetadata.builder("alias_a").writeIndex(true).build())
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1);
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(index).put(indexA1).put(indexA2)).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportPutMappingAction(
            transportService,
            clusterService,
            threadPool,
            metadataMappingService,
            actionFilters,
            indexNameExpressionResolver,
            requestValidators,
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_transformedMappingUsed() {
        // Arrange: Create a test request
        final PutMappingRequest request = new PutMappingRequest("index");
        final String originalMapping = "{\"properties\": {\"field\": {\"type\": \"text\"}}}";
        request.source(originalMapping, MediaTypeRegistry.JSON);

        String transformedMapping = "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}";

        // Mock the transformer registry to return the transformed mapping
        ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(anyString(), any(), listenerCaptor.capture());

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate transformation completion
        listenerCaptor.getValue().onResponse(transformedMapping);

        // Assert: Verify the transformed mapping is passed to metadataMappingService
        ArgumentCaptor<PutMappingClusterStateUpdateRequest> updateRequestCaptor = ArgumentCaptor.forClass(
            PutMappingClusterStateUpdateRequest.class
        );
        verify(metadataMappingService, times(1)).putMapping(updateRequestCaptor.capture(), any());

        // Ensure the transformed mapping is used correctly
        PutMappingClusterStateUpdateRequest capturedRequest = updateRequestCaptor.getValue();
        assertEquals(transformedMapping, capturedRequest.source());
    }

    public void testResolveIndices() {
        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new PutMappingRequest("index_a*"));
            assertEquals(ResolvedIndices.of("index_a1", "index_a2"), resolvedIndices);
        }
        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new PutMappingRequest("alias_a"));
            assertEquals(ResolvedIndices.of("alias_a"), resolvedIndices);
        }
        {
            ResolvedIndices resolvedIndices = action.resolveIndices(new PutMappingRequest("alias_a").writeIndexOnly(true));
            assertEquals(ResolvedIndices.of("index_a2"), resolvedIndices);
        }
        {
            ResolvedIndices resolvedIndices = action.resolveIndices(
                new PutMappingRequest().setConcreteIndex(new Index("index_c", "index_c_uuid"))
            );
            assertEquals(ResolvedIndices.of("index_c"), resolvedIndices);
        }
    }
}

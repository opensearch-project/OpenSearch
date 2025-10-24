/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.create;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.alias.IndicesAliasesAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateIndexActionTests extends OpenSearchTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private MetadataCreateIndexService createIndexService;

    @Mock
    private MappingTransformerRegistry mappingTransformerRegistry;

    @Mock
    private ClusterState clusterState;

    @Mock
    private ActionListener<CreateIndexResponse> responseListener;

    private TransportCreateIndexAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportCreateIndexAction(
            transportService,
            null, // ClusterService not needed for this test
            threadPool,
            createIndexService,
            actionFilters,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_usesTransformedMapping() {
        // Arrange: Create a test request
        final CreateIndexRequest request = new CreateIndexRequest("test-index");
        request.mapping("{\"properties\": {\"field\": {\"type\": \"text\"}}}");

        // Mock transformed mapping result
        final String transformedMapping = "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}";

        // Capture ActionListener passed to applyTransformers
        final ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(anyString(), any(), listenerCaptor.capture());

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate transformation completion
        listenerCaptor.getValue().onResponse(transformedMapping);

        // Assert: Capture request sent to createIndexService
        ArgumentCaptor<CreateIndexClusterStateUpdateRequest> updateRequestCaptor = ArgumentCaptor.forClass(
            CreateIndexClusterStateUpdateRequest.class
        );
        verify(createIndexService, times(1)).createIndex(updateRequestCaptor.capture(), any());

        // Ensure transformed mapping is passed correctly
        CreateIndexClusterStateUpdateRequest capturedRequest = updateRequestCaptor.getValue();
        assertEquals(transformedMapping, capturedRequest.mappings());
    }

    public void testResolveIndices() {
        assertEquals(ResolvedIndices.of("test_index"), action.resolveIndices(new CreateIndexRequest("test_index")));
        assertEquals(
            ResolvedIndices.of("test_index").withLocalSubActions(IndicesAliasesAction.INSTANCE, ResolvedIndices.Local.of("test_alias")),
            action.resolveIndices(new CreateIndexRequest("test_index").alias(new Alias("test_alias")))
        );
    }

}

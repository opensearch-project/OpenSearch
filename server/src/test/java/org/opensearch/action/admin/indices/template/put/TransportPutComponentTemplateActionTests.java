/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.template.put;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.MappingTransformerRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutComponentTemplateActionTests extends OpenSearchTestCase {
    @Mock
    private TransportService transportService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private MappingTransformerRegistry mappingTransformerRegistry;
    @Mock
    private MetadataIndexTemplateService indexTemplateService;
    @Mock
    private ActionListener<AcknowledgedResponse> responseListener;
    @Mock
    private ClusterState clusterState;

    private TransportPutComponentTemplateAction action;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        ActionFilter[] emptyActionFilters = new ActionFilter[] {};
        when(actionFilters.filters()).thenReturn(emptyActionFilters);
        action = new TransportPutComponentTemplateAction(
            transportService,
            null, // ClusterService not needed for this test
            threadPool,
            indexTemplateService,
            actionFilters,
            null, // IndexNameExpressionResolver not needed
            null, // IndexScopedSettings not needed
            mappingTransformerRegistry
        );
    }

    public void testClusterManagerOperation_mappingTransformationApplied() throws IOException {
        // Arrange: Create a test request and mock dependencies
        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request("test");
        ComponentTemplate componentTemplate = mock(ComponentTemplate.class);
        Template template = mock(Template.class);
        when(componentTemplate.template()).thenReturn(template);
        when(template.mappings()).thenReturn(new CompressedXContent("{\"properties\": {\"field\": {\"type\": \"text\"}}}"));
        request.componentTemplate(componentTemplate);

        String transformedMapping = "{\"properties\": {\"field\": {\"type\": \"keyword\"}}}";

        // Mock mapping transformer to return transformed mapping
        ArgumentCaptor<ActionListener<String>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doNothing().when(mappingTransformerRegistry).applyTransformers(anyString(), any(), listenerCaptor.capture());

        // Act: Call the method
        action.clusterManagerOperation(request, clusterState, responseListener);

        // Simulate mapping transformation
        listenerCaptor.getValue().onResponse(transformedMapping);

        // Assert: Verify the transformed mappings are set correctly
        verify(template, times(1)).setMappings(new CompressedXContent(transformedMapping));

        // Verify that indexTemplateService.putComponentTemplate is called
        verify(indexTemplateService, times(1)).putComponentTemplate(
            eq(request.cause()),
            eq(request.create()),
            eq(request.name()),
            eq(request.clusterManagerNodeTimeout()),
            eq(componentTemplate),
            eq(responseListener)
        );
    }
}

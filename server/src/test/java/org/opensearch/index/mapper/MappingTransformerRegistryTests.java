/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import joptsimple.internal.Strings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MappingTransformerRegistryTests extends OpenSearchTestCase {
    @Mock
    private MapperPlugin mapperPlugin;
    @Mock
    private NamedXContentRegistry xContentRegistry;
    @Mock
    private ActionListener<String> listener;
    @Mock
    private MappingTransformer transformer;

    private MappingTransformerRegistry registry;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(mapperPlugin.getMappingTransformers()).thenReturn(List.of(transformer));
        registry = new MappingTransformerRegistry(List.of(mapperPlugin), xContentRegistry);
        // Mock dummy transformer behavior which does not modify the mapping
        doAnswer(invocation -> {
            final ActionListener<Void> actionListener = invocation.getArgument(2);
            actionListener.onResponse(null);
            return null;
        }).when(transformer).transform(any(), any(), any());
    }

    public void testApplyTransformers_whenNoTransformers_returnMappingDirectly() {
        final String mapping = Strings.EMPTY;
        final MappingTransformerRegistry mappingTransformerRegistry = new MappingTransformerRegistry(
            Collections.emptyList(),
            xContentRegistry
        );

        mappingTransformerRegistry.applyTransformers(mapping, null, listener);

        verify(listener, Mockito.times(1)).onResponse(mapping);
    }

    public void testApplyTransformers_WithNullMapping_ShouldReturnNull() {
        registry.applyTransformers(null, null, listener);

        verify(listener).onResponse(null);
    }

    public void testApplyTransformers_WithTransformerApplied() throws IOException {
        final String inputMappingString = "{\"field\": \"value\"}";
        final String expectedTransformedMappingString = "{\"field\":\"transformedValue\"}";

        doAnswer(invocation -> {
            Map<String, Object> mapping = invocation.getArgument(0);
            assertEquals(mapping.get("field"), "value");
            mapping.put("field", "transformedValue"); // Simulating transformation
            ActionListener<Void> actionListener = invocation.getArgument(2);
            actionListener.onResponse(null);
            return null;
        }).when(transformer).transform(any(), any(), any());

        registry.applyTransformers(inputMappingString, null, listener);

        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener).onResponse(responseCaptor.capture());

        String transformedMapping = responseCaptor.getValue();
        assertEquals(expectedTransformedMappingString, transformedMapping);
    }

    public void testApplyTransformers_WhenTransformerFails_ShouldCallOnFailure() {
        final String inputMappingString = "{\"field\": \"value\"}";
        final String errorMsg = "Transformation failed";

        doAnswer(invocation -> {
            ActionListener<Void> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException(errorMsg));
            return null;
        }).when(transformer).transform(any(), any(), any());

        registry.applyTransformers(inputMappingString, null, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());

        assertTrue(exceptionCaptor.getValue() instanceof RuntimeException);
        assertTrue(exceptionCaptor.getValue().getMessage().contains(errorMsg));
    }
}

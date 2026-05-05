/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FieldStorageResolver} field storage resolution.
 */
public class FieldStorageResolverTests extends OpenSearchTestCase {

    public void testTextFieldGetsDocValuesInPrimaryFormat() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("name", Map.of("type", "text")));

        FieldStorageInfo info = resolver.resolve(List.of("name")).get(0);

        assertEquals("name", info.getFieldName());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of("lucene"), info.getIndexFormats());
    }

    public void testLongFieldGetsDocValuesInPrimaryFormat() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("age", Map.of("type", "long")));

        FieldStorageInfo info = resolver.resolve(List.of("age")).get(0);

        assertEquals("age", info.getFieldName());
        assertEquals(List.of("parquet"), info.getDocValueFormats());
        assertEquals(List.of("lucene"), info.getIndexFormats());
    }

    public void testFieldWithAllStorageDisabledHasNoStorage() {
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> newResolver("parquet", Map.of("name", Map.of("type", "text", "doc_values", false, "index", false)))
        );
        assertTrue("expected 'no storage' error, got: " + ex.getMessage(), ex.getMessage().contains("has no storage in any format"));
    }

    private static FieldStorageResolver newResolver(String primaryFormat, Map<String, Map<String, Object>> fieldMappings) {
        Map<String, Object> mappingSource = Map.of("properties", fieldMappings);

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.composite.primary_data_format", primaryFormat).build());
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);

        return new FieldStorageResolver(indexMetadata);
    }
}

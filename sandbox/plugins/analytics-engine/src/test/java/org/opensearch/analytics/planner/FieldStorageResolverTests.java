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

    public void testTextFieldWithKeywordSubfieldCapturesSubfieldName() {
        FieldStorageResolver resolver = newResolver(
            "parquet",
            Map.of("gender", Map.of("type", "text", "fields", Map.of("keyword", Map.of("type", "keyword"))))
        );
        FieldStorageInfo info = resolver.resolve(List.of("gender")).get(0);
        assertEquals("keyword", info.getExactMatchSubfield());
    }

    public void testTextFieldWithoutKeywordSubfieldHasNullSubfield() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("name", Map.of("type", "text")));
        assertNull(resolver.resolve(List.of("name")).get(0).getExactMatchSubfield());
    }

    public void testKeywordFieldHasNullSubfield() {
        FieldStorageResolver resolver = newResolver("parquet", Map.of("tag", Map.of("type", "keyword")));
        assertNull(resolver.resolve(List.of("tag")).get(0).getExactMatchSubfield());
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

    public void testMergedResolverUnionsFieldsAcrossIndices() {
        // Mirrors an index pattern (test*) whose backing indices declare different field sets:
        // index A = {name, age}, index B = {name, alias}. The union scan row type references all
        // three; a single-index resolver would throw on the field its index lacks.
        FieldStorageResolver a = newResolver("parquet", Map.of("name", Map.of("type", "text"), "age", Map.of("type", "long")));
        FieldStorageResolver b = newResolver("parquet", Map.of("name", Map.of("type", "text"), "alias", Map.of("type", "keyword")));

        FieldStorageResolver merged = FieldStorageResolver.merged(List.of(a, b));
        List<FieldStorageInfo> infos = merged.resolve(List.of("name", "age", "alias"));

        assertEquals(3, infos.size());
        assertEquals("name", infos.get(0).getFieldName());
        assertEquals("age", infos.get(1).getFieldName());
        assertEquals("alias", infos.get(2).getFieldName());
    }

    public void testIndexWithoutMappingContributesNoFields() {
        // An index created empty (no mapping, no data) declares no fields. It must construct
        // cleanly — aliases legitimately span such indices next to populated ones — and resolving
        // any field against it alone fails with the standard "not found" error.
        FieldStorageResolver resolver = newMappinglessResolver();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> resolver.resolve(List.of("name")));
        assertTrue("expected 'not found' error, got: " + ex.getMessage(), ex.getMessage().contains("not found in field storage"));
    }

    public void testMergedResolverSkipsMappinglessIndex() {
        // Alias over {empty index, populated index}: the union must equal the populated index's
        // field set — the empty member contributes nothing and must not fail the merge.
        FieldStorageResolver empty = newMappinglessResolver();
        FieldStorageResolver populated = newResolver("parquet", Map.of("name", Map.of("type", "text"), "age", Map.of("type", "long")));

        FieldStorageResolver merged = FieldStorageResolver.merged(List.of(empty, populated));
        List<FieldStorageInfo> infos = merged.resolve(List.of("name", "age"));

        assertEquals(2, infos.size());
        assertEquals("name", infos.get(0).getFieldName());
        assertEquals("age", infos.get(1).getFieldName());
    }

    private static FieldStorageResolver newMappinglessResolver() {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("empty_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder()
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", "lucene")
                .build()
        );
        when(indexMetadata.mapping()).thenReturn(null);
        return new FieldStorageResolver(indexMetadata);
    }

    private static FieldStorageResolver newResolver(String primaryFormat, Map<String, Map<String, Object>> fieldMappings) {
        Map<String, Object> mappingSource = Map.of("properties", fieldMappings);

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder()
                .put("index.composite.primary_data_format", primaryFormat)
                .putList("index.composite.secondary_data_formats", "lucene")
                .build()
        );
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);

        return new FieldStorageResolver(indexMetadata);
    }
}

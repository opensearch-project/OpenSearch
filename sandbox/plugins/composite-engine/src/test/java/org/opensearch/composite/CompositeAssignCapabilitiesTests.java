/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeDataFormatPlugin#assignCapabilities(MappedFieldType, IndexSettings, DataFormatRegistry)}.
 */
public class CompositeAssignCapabilitiesTests extends OpenSearchTestCase {

    public void testPrimaryCoversAllCapabilities() {
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.COLUMNAR_STORAGE, Capability.FULL_TEXT_SEARCH)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet").build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        Map<DataFormat, Set<Capability>> map = field.getCapabilityMap();
        assertEquals(1, map.size());
        assertEquals(Set.of(Capability.COLUMNAR_STORAGE, Capability.FULL_TEXT_SEARCH), map.get(parquet));
    }

    public void testPrimaryPartialSecondaryCompletes() {
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.COLUMNAR_STORAGE)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.FULL_TEXT_SEARCH)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene")
                .build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        Map<DataFormat, Set<Capability>> map = field.getCapabilityMap();
        assertEquals(2, map.size());
        assertEquals(Set.of(Capability.COLUMNAR_STORAGE), map.get(parquet));
        assertEquals(Set.of(Capability.FULL_TEXT_SEARCH), map.get(lucene));
    }

    public void testNeitherFormatCoversAll_Throws() {
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.BLOOM_FILTER)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.STORED_FIELDS)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene")
                .build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> plugin.assignCapabilities(field, indexSettings, registry)
        );
        assertTrue(ex.getMessage().contains("FULL_TEXT_SEARCH"));
        assertTrue(ex.getMessage().contains("COLUMNAR_STORAGE"));
    }

    public void testNoFormatsConfigured_EmptyMap() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of());

        // Use a primary format name that doesn't match anything in the registry
        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "nonexistent").build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        assertTrue(field.getCapabilityMap().isEmpty());
    }

    public void testFieldNotSupportedByAnyFormat_Throws() {
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene")
                .build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> plugin.assignCapabilities(field, indexSettings, registry)
        );
        assertTrue(ex.getMessage().contains("keyword"));
    }

    public void testAllCapabilitiesSatisfiedBySecondaryOnly() {
        // Primary doesn't support keyword at all
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.POINT_RANGE)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene")
                .build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        Map<DataFormat, Set<Capability>> map = field.getCapabilityMap();
        assertEquals(1, map.size());
        assertEquals(Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE), map.get(lucene));
    }

    public void testPriorityOrderPrimaryClaimsFirst() {
        // Both support COLUMNAR_STORAGE for keyword, but primary is first in configured order
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.COLUMNAR_STORAGE)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.COLUMNAR_STORAGE, Capability.FULL_TEXT_SEARCH)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene")
                .build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        Map<DataFormat, Set<Capability>> map = field.getCapabilityMap();
        assertEquals(2, map.size());
        assertEquals(Set.of(Capability.COLUMNAR_STORAGE), map.get(parquet));
        assertEquals(Set.of(Capability.FULL_TEXT_SEARCH), map.get(lucene));
    }

    public void testMultipleSecondariesSplitCapabilities() {
        // Use "integer" type which requests POINT_RANGE + COLUMNAR_STORAGE + STORED_FIELDS when all three are enabled
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.COLUMNAR_STORAGE)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.POINT_RANGE)))
        );
        DataFormat arrow = CompositeTestHelper.stubFormat(
            "arrow",
            3,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.STORED_FIELDS)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene, arrow));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene", "arrow")
                .build()
        );

        // NumberFieldType with isSearchable=true, isStored=true, hasDocValues=true → POINT_RANGE + STORED_FIELDS + COLUMNAR_STORAGE
        MappedFieldType field = new NumberFieldMapper.NumberFieldType(
            "f",
            NumberFieldMapper.NumberType.INTEGER,
            true,
            true,
            true,
            false,
            true,
            null,
            Map.of()
        );
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        Map<DataFormat, Set<Capability>> map = field.getCapabilityMap();
        assertEquals(3, map.size());
        assertEquals(Set.of(Capability.COLUMNAR_STORAGE), map.get(parquet));
        assertEquals(Set.of(Capability.POINT_RANGE), map.get(lucene));
        assertEquals(Set.of(Capability.STORED_FIELDS), map.get(arrow));
    }

    public void testFieldWithNoRequestedCapabilities_EmptyMap() {
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("integer", Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet").build()
        );

        // No-caps field: isSearchable=false, isStored=false, hasDocValues=false
        MappedFieldType field = new NumberFieldMapper.NumberFieldType(
            "x",
            NumberFieldMapper.NumberType.INTEGER,
            false,
            false,
            false,
            false,
            true,
            null,
            Map.of()
        );
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();
        plugin.assignCapabilities(field, indexSettings, registry);

        assertTrue(field.getCapabilityMap().isEmpty());
    }

    public void testPartialAcrossFormatsButStillIncomplete_Throws() {
        DataFormat parquet = CompositeTestHelper.stubFormat(
            "parquet",
            1,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.COLUMNAR_STORAGE)))
        );
        DataFormat lucene = CompositeTestHelper.stubFormat(
            "lucene",
            2,
            Set.of(new FieldTypeCapabilities("keyword", Set.of(Capability.STORED_FIELDS)))
        );
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(parquet, lucene));

        IndexSettings indexSettings = buildIndexSettings(
            Settings.builder()
                .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
                .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "lucene")
                .build()
        );

        MappedFieldType field = new KeywordFieldMapper.KeywordFieldType("name");
        CompositeDataFormatPlugin plugin = new CompositeDataFormatPlugin();

        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> plugin.assignCapabilities(field, indexSettings, registry)
        );
        assertTrue(ex.getMessage().contains("FULL_TEXT_SEARCH"));
    }

    private static IndexSettings buildIndexSettings(Settings extra) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(extra)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }
}

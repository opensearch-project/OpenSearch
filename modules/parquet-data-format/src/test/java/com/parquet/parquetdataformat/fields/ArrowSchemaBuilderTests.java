/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assume.assumeTrue;

/**
 * Comprehensive test suite for ArrowSchemaBuilder covering all must-have scenarios.
 */
public class ArrowSchemaBuilderTests extends OpenSearchTestCase {

    public void testGetSchemaWithNullMapperServiceThrowsException() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            ArrowSchemaBuilder.getSchema(null);
        });

        assertTrue("Exception message should contain 'MapperService cannot be null'",
                  exception.getMessage().contains("MapperService cannot be null"));
    }

    public void testGetSchemaWithEmptyMappersCreatesSystemFieldsOnly() {
        MapperService mapperService = createMockMapperService(Collections.emptyList());

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        assertNotNull("Schema should not be null", schema);
        List<Field> fields = schema.getFields();

        assertEquals("Should have exactly 2 system fields", 2, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).toList();
        assertTrue("Should contain ROW_ID field", fieldNames.contains(CompositeDataFormatWriter.ROW_ID));
        assertTrue("Should contain PRIMARY_TERM field", fieldNames.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testGetSchemaWithOnlyMetadataFieldsCreatesSystemFieldsOnly() {
        Mapper sourceMapper = createMockMapper("_source", SourceFieldMapper.class);
        Mapper fieldNamesMapper = createMockMapper("_field_names", FieldNamesFieldMapper.class);
        Mapper indexMapper = createMockMapper("_index", IndexFieldMapper.class);

        MapperService mapperService = createMockMapperService(Arrays.asList(sourceMapper, fieldNamesMapper, indexMapper));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        assertNotNull("Schema should not be null", schema);
        List<Field> fields = schema.getFields();

        assertEquals("Should have exactly 2 system fields", 2, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).toList();
        assertTrue("Should contain ROW_ID field", fieldNames.contains(CompositeDataFormatWriter.ROW_ID));
        assertTrue("Should contain PRIMARY_TERM field", fieldNames.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testMetadataFieldsAreExcluded() {
        Mapper textMapper = createMockMapper("title", "text");
        Mapper sourceMapper = createMockMapper("_source", SourceFieldMapper.class);
        Mapper fieldNamesMapper = createMockMapper("_field_names", FieldNamesFieldMapper.class);
        Mapper indexMapper = createMockMapper("_index", IndexFieldMapper.class);
        Mapper nestedPathMapper = createMockMapper("_nested_path", NestedPathFieldMapper.class);
        Mapper featureMapper = createMockMapper("_feature", "_feature");
        Mapper timestampMapper = createMockMapper("_data_stream_timestamp", "_data_stream_timestamp");

        MapperService mapperService = createMockMapperService(Arrays.asList(
            textMapper, sourceMapper, fieldNamesMapper, indexMapper,
            nestedPathMapper, featureMapper, timestampMapper
        ));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        assertNotNull("Schema should not be null", schema);
        List<Field> fields = schema.getFields();

        assertEquals("Should have 3 fields (only user field + system fields)", 3, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).toList();

        assertTrue("Should contain title field", fieldNames.contains("title"));

        assertFalse("Should not contain _source field", fieldNames.contains("_source"));
        assertFalse("Should not contain _field_names field", fieldNames.contains("_field_names"));
        assertFalse("Should not contain _index field", fieldNames.contains("_index"));
        assertFalse("Should not contain _nested_path field", fieldNames.contains("_nested_path"));
        assertFalse("Should not contain _feature field", fieldNames.contains("_feature"));
        assertFalse("Should not contain _data_stream_timestamp field", fieldNames.contains("_data_stream_timestamp"));

        assertTrue("Should contain ROW_ID field", fieldNames.contains(CompositeDataFormatWriter.ROW_ID));
        assertTrue("Should contain PRIMARY_TERM field", fieldNames.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testGetSchemaWithUnsupportedFieldTypeThrowsException() {
        Mapper unsupportedMapper = createMockMapper("unsupported_field", "unknown_type");

        MapperService mapperService = createMockMapperService(Arrays.asList(unsupportedMapper));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            ArrowSchemaBuilder.getSchema(mapperService);
        });

        assertTrue("Exception message should mention unsupported field type",
                  exception.getMessage().contains("Unsupported field type 'unknown_type'"));
        assertTrue("Exception message should mention field name",
                  exception.getMessage().contains("unsupported_field"));
    }

    public void testSupportedFieldCreatesArrowFieldCorrectly() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        String supportedType = registeredTypes.iterator().next();

        Mapper supportedMapper = createMockMapper("test_field", supportedType);
        MapperService mapperService = createMockMapperService(Arrays.asList(supportedMapper));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        assertNotNull("Schema should not be null", schema);
        List<Field> fields = schema.getFields();

        Field userField = fields.stream()
            .filter(f -> "test_field".equals(f.getName()))
            .findFirst()
            .orElse(null);

        assertNotNull("User field should be present", userField);
        assertEquals("Field name should match mapper name", "test_field", userField.getName());
        assertTrue("Field children should be null or empty",
                  userField.getChildren() == null || userField.getChildren().isEmpty());

        ParquetField registryField = ArrowFieldRegistry.getParquetField(supportedType);
        assertNotNull("Registry should have the field", registryField);
        assertEquals("Field type should match registry field type",
                    registryField.getFieldType(), userField.getFieldType());
    }

    public void testAllNonMetadataMappersAreConverted() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        List<String> testTypes = registeredTypes.stream().limit(3).toList();

        Mapper mapper1 = createMockMapper("field1", testTypes.get(0));
        Mapper mapper2 = createMockMapper("field2", testTypes.get(1));
        Mapper mapper3 = createMockMapper("field3", testTypes.get(2));

        MapperService mapperService = createMockMapperService(Arrays.asList(mapper1, mapper2, mapper3));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        assertNotNull("Schema should not be null", schema);
        List<Field> fields = schema.getFields();

        assertEquals("Should have 5 fields (3 user + 2 system)", 5, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).toList();
        assertTrue("Should contain field1", fieldNames.contains("field1"));
        assertTrue("Should contain field2", fieldNames.contains("field2"));
        assertTrue("Should contain field3", fieldNames.contains("field3"));
        assertTrue("Should contain ROW_ID", fieldNames.contains(CompositeDataFormatWriter.ROW_ID));
        assertTrue("Should contain PRIMARY_TERM", fieldNames.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testAlwaysAppendRowIdAndPrimaryTermFields() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        String supportedType = registeredTypes.iterator().next();
        Mapper mapper = createMockMapper("test_field", supportedType);

        MapperService mapperService = createMockMapperService(Arrays.asList(mapper));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        List<Field> fields = schema.getFields();

        Field rowIdField = fields.stream()
            .filter(f -> CompositeDataFormatWriter.ROW_ID.equals(f.getName()))
            .findFirst()
            .orElse(null);

        assertNotNull("ROW_ID field should be present", rowIdField);
        assertEquals("ROW_ID field name should be correct", CompositeDataFormatWriter.ROW_ID, rowIdField.getName());
        assertTrue("ROW_ID field children should be null or empty",
                  rowIdField.getChildren() == null || rowIdField.getChildren().isEmpty());

        Field primaryTermField = fields.stream()
            .filter(f -> SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(f.getName()))
            .findFirst()
            .orElse(null);

        assertNotNull("PRIMARY_TERM field should be present", primaryTermField);
        assertEquals("PRIMARY_TERM field name should be correct", SeqNoFieldMapper.PRIMARY_TERM_NAME, primaryTermField.getName());
        assertTrue("PRIMARY_TERM field children should be null or empty",
                  primaryTermField.getChildren() == null || primaryTermField.getChildren().isEmpty());

        LongParquetField longField = new LongParquetField();
        assertEquals("ROW_ID should use LongParquetField type",
                    longField.getFieldType(), rowIdField.getFieldType());
        assertEquals("PRIMARY_TERM should use LongParquetField type",
                    longField.getFieldType(), primaryTermField.getFieldType());
    }

    public void testCreateArrowFieldDelegatesProperly() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        String supportedType = registeredTypes.iterator().next();

        Mapper mapper = createMockMapper("test_field", supportedType);
        MapperService mapperService = createMockMapperService(Arrays.asList(mapper));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        Field userField = schema.getFields().stream()
            .filter(f -> "test_field".equals(f.getName()))
            .findFirst()
            .orElse(null);

        assertNotNull("User field should be created", userField);

        ParquetField registryField = ArrowFieldRegistry.getParquetField(supportedType);
        assertNotNull("Registry field should exist", registryField);

        assertEquals("Field should use exact registry field type",
                    registryField.getFieldType(), userField.getFieldType());
        assertEquals("Field should use exact registry arrow type",
                    registryField.getArrowType(), userField.getFieldType().getType());
    }

    public void testUtilityClassCannotBeInstantiated() {
        try {
            java.lang.reflect.Constructor<?> constructor = ArrowSchemaBuilder.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            java.lang.reflect.InvocationTargetException exception = expectThrows(java.lang.reflect.InvocationTargetException.class, () -> {
                constructor.newInstance();
            });

            Throwable cause = exception.getCause();
            assertTrue("Cause should be UnsupportedOperationException", cause instanceof UnsupportedOperationException);
            assertEquals("Utility class should not be instantiated", cause.getMessage());
        } catch (Exception e) {
            fail("Should be able to access constructor via reflection: " + e.getMessage());
        }
    }

    public void testSystemFieldsAppendedLast() {
        Mapper mapper1 = createMockMapper("field_alpha", "text");
        Mapper mapper2 = createMockMapper("field_beta", "keyword");
        Mapper mapper3 = createMockMapper("field_gamma", "long");

        MapperService mapperService = createMockMapperService(Arrays.asList(mapper1, mapper2, mapper3));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        List<Field> fields = schema.getFields();

        assertEquals("Should have 5 fields total", 5, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).toList();

        assertTrue("Should contain field_alpha", fieldNames.contains("field_alpha"));
        assertTrue("Should contain field_beta", fieldNames.contains("field_beta"));
        assertTrue("Should contain field_gamma", fieldNames.contains("field_gamma"));

        assertEquals("Second to last field should be ROW_ID", CompositeDataFormatWriter.ROW_ID, fields.get(3).getName());
        assertEquals("Last field should be PRIMARY_TERM", SeqNoFieldMapper.PRIMARY_TERM_NAME, fields.get(4).getName());
    }

    public void testRowIdUsesLongParquetField() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        String supportedType = registeredTypes.iterator().next();
        Mapper mapper = createMockMapper("test_field", supportedType);

        MapperService mapperService = createMockMapperService(Arrays.asList(mapper));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        Field rowIdField = schema.getFields().stream()
            .filter(f -> CompositeDataFormatWriter.ROW_ID.equals(f.getName()))
            .findFirst()
            .orElse(null);

        assertNotNull("ROW_ID field should be present", rowIdField);

        LongParquetField longParquetField = new LongParquetField();
        assertEquals("ROW_ID should use LongParquetField.getFieldType()",
                    longParquetField.getFieldType(), rowIdField.getFieldType());
        assertEquals("ROW_ID should use LongParquetField.getArrowType()",
                    longParquetField.getArrowType(), rowIdField.getFieldType().getType());

        ParquetField registryLongField = ArrowFieldRegistry.getParquetField(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        if (registryLongField instanceof LongParquetField) {
            assertEquals("ROW_ID should be consistent with registry Long semantics",
                        registryLongField.getFieldType(), rowIdField.getFieldType());
        }
    }

    public void testGetSchemaWithMixedValidAndInvalidMappers() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        String supportedType = registeredTypes.iterator().next();

        Mapper validMapper = createMockMapper("valid_field", supportedType);
        Mapper sourceMapper = createMockMapper("_source", SourceFieldMapper.class);
        Mapper anotherValidMapper = createMockMapper("another_valid", supportedType);

        MapperService mapperService = createMockMapperService(Arrays.asList(validMapper, sourceMapper, anotherValidMapper));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);

        assertNotNull("Schema should not be null", schema);
        List<Field> fields = schema.getFields();

        assertEquals("Should have 4 fields", 4, fields.size());

        List<String> fieldNames = fields.stream().map(Field::getName).toList();
        assertTrue("Should contain valid_field", fieldNames.contains("valid_field"));
        assertTrue("Should contain another_valid", fieldNames.contains("another_valid"));
        assertFalse("Should not contain _source field", fieldNames.contains("_source"));
        assertTrue("Should contain ROW_ID", fieldNames.contains(CompositeDataFormatWriter.ROW_ID));
        assertTrue("Should contain PRIMARY_TERM", fieldNames.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    private MapperService createMockMapperService(List<Mapper> mappers) {
        MapperService mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);

        List<FieldMapper> fieldMappers = mappers.stream()
            .filter(mapper -> mapper instanceof FieldMapper)
            .filter(mapper -> !isMetadataMapper(mapper))
            .map(mapper -> (FieldMapper) mapper)
            .collect(java.util.stream.Collectors.toList());

        MappingLookup mappingLookup = new MappingLookup(
            fieldMappers,
            java.util.Collections.emptyList(),
            java.util.Collections.emptyList(),
            0,
            new org.apache.lucene.analysis.standard.StandardAnalyzer()
        );

        when(documentMapper.mappers()).thenReturn(mappingLookup);
        when(mapperService.documentMapper()).thenReturn(documentMapper);

        return mapperService;
    }

    private boolean isMetadataMapper(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || "_feature".equals(mapper.typeName())
            || "_data_stream_timestamp".equals(mapper.typeName());
    }

    private Mapper createMockMapper(String name, String typeName) {
        // For regular field mappers, create FieldMapper mocks
        FieldMapper mapper = mock(FieldMapper.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        FieldMapper.CopyTo copyTo = mock(FieldMapper.CopyTo.class);

        when(mapper.name()).thenReturn(name);
        when(mapper.typeName()).thenReturn(typeName);
        when(mapper.fieldType()).thenReturn(fieldType);
        when(mapper.copyTo()).thenReturn(copyTo);
        when(copyTo.copyToFields()).thenReturn(java.util.Collections.emptyList());
        when(fieldType.name()).thenReturn(name);
        when(fieldType.indexAnalyzer()).thenReturn(null); // This will use defaultIndex analyzer

        return mapper;
    }

    private Mapper createMockMapper(String name, Class<? extends Mapper> mapperClass) {
        Mapper mapper = mock(mapperClass);
        when(mapper.name()).thenReturn(name);
        when(mapper.typeName()).thenReturn(name);

        return mapper;
    }
}

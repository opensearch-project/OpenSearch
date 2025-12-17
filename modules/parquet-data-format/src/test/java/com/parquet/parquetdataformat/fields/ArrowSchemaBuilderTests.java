/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for {@link ArrowSchemaBuilder}.
 * 
 * <p>This test suite validates the Arrow schema generation functionality including:
 * <ul>
 *   <li>System field handling and positioning</li>
 *   <li>User field type conversion and validation</li>
 *   <li>Metadata field exclusion</li>
 *   <li>Error handling for invalid inputs</li>
 *   <li>Utility class instantiation prevention</li>
 * </ul>
 * 
 * 
 * @see ArrowSchemaBuilder
 * @see ArrowFieldRegistry
 */
public class ArrowSchemaBuilderTests extends OpenSearchTestCase {

    private static final String ROW_ID = CompositeDataFormatWriter.ROW_ID;
    private static final String PRIMARY_TERM = SeqNoFieldMapper.PRIMARY_TERM_NAME;
    
    private static final int EXPECTED_SYSTEM_FIELDS_COUNT = 2;
    private static final int EXPECTED_TOTAL_FIELDS_WITH_USER_DATA = 4;
    private static final int EXPECTED_FIELDS_WITH_ONE_USER_FIELD = 3;
    private static final int EXPECTED_MULTI_FIELD_COUNT = 5;
    private static final int TEST_FIELD_LIMIT = 3;
    
    private static final String PRODUCT_NAME_FIELD = "product_name";
    private static final String CATEGORY_FIELD = "category";
    private static final String SAMPLE_FIELD = "sample_field";
    private static final String USER_ID_FIELD = "user_id";
    private static final String EMAIL_FIELD = "email";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String UNSUPPORTED_FIELD = "unsupported_field";
    private static final String TITLE_FIELD = "title";
    
    private static final String TEXT_TYPE = "text";
    private static final String KEYWORD_TYPE = "keyword";
    private static final String UNKNOWN_TYPE = "unknown_type";
    
    private static final String SOURCE_FIELD = "_source";
    private static final String FIELD_NAMES_FIELD = "_field_names";
    private static final String INDEX_FIELD = "_index";
    private static final String NESTED_PATH_FIELD = "_nested_path";
    private static final String FEATURE_FIELD = "_feature";
    private static final String DATA_STREAM_TIMESTAMP_FIELD = "_data_stream_timestamp";
    
    private static final String MAPPER_SERVICE_NULL_ERROR = "MapperService cannot be null";
    private static final String UNSUPPORTED_TYPE_ERROR = "Unsupported field type 'unknown_type'";
    private static final String UTILITY_CLASS_ERROR = "Utility class should not be instantiated";


    /**
     * Verifies that {@link ArrowSchemaBuilder#getSchema(MapperService)} throws
     * a {@link NullPointerException} when provided with a null MapperService.
     * 
     * <p>This test ensures proper input validation and prevents runtime errors
     * when the schema builder is called with invalid parameters.
     */
    public void testGetSchemaWithNullMapperServiceThrowsException() {
        NullPointerException exception = expectThrows(NullPointerException.class,
            () -> ArrowSchemaBuilder.getSchema(null));

        assertTrue("Exception message should mention MapperService",
            exception.getMessage().contains(MAPPER_SERVICE_NULL_ERROR));
    }


    /**
     * Validates the behavior of system fields in Arrow schema generation.
     * 
     * <p>This comprehensive test verifies:
     * <ul>
     *   <li>System fields are always present regardless of user fields</li>
     *   <li>System fields use the correct LongParquetField type</li>
     *   <li>System fields are positioned at the end of the schema</li>
     *   <li>ROW_ID and PRIMARY_TERM fields are correctly included</li>
     * </ul>
     * 
     * <p>Tests both empty schemas and schemas with user-defined fields
     * to ensure consistent system field handling.
     */
    public void testSystemFieldsBehavior() {
        MapperService emptyMapperService = createMockMapperService(Collections.emptyList());
        MapperService mapperServiceWithFields = createMockMapperService(Arrays.asList(
            createFieldMapper(PRODUCT_NAME_FIELD, TEXT_TYPE),
            createFieldMapper(CATEGORY_FIELD, KEYWORD_TYPE)
        ));

        Schema emptySchema = ArrowSchemaBuilder.getSchema(emptyMapperService);
        Schema schemaWithFields = ArrowSchemaBuilder.getSchema(mapperServiceWithFields);
        
        List<String> emptyFieldNames = getFieldNames(emptySchema);
        List<Field> fieldsWithUserData = schemaWithFields.getFields();
        LongParquetField expectedType = new LongParquetField();

        assertEquals("Should have exactly 2 system fields when no user fields", EXPECTED_SYSTEM_FIELDS_COUNT, emptySchema.getFields().size());
        assertTrue("Should contain ROW_ID field", emptyFieldNames.contains(ROW_ID));
        assertTrue("Should contain PRIMARY_TERM field", emptyFieldNames.contains(PRIMARY_TERM));

        Field rowIdField = findField(emptySchema, ROW_ID);
        Field primaryTermField = findField(emptySchema, PRIMARY_TERM);
        assertNotNull("ROW_ID field should be present", rowIdField);
        assertNotNull("PRIMARY_TERM field should be present", primaryTermField);
        assertEquals("ROW_ID should use LongParquetField type", expectedType.getFieldType(), rowIdField.getFieldType());
        assertEquals("PRIMARY_TERM should use LongParquetField type", expectedType.getFieldType(), primaryTermField.getFieldType());

        assertEquals("Should have 4 fields total with user fields", EXPECTED_TOTAL_FIELDS_WITH_USER_DATA, fieldsWithUserData.size());
        assertEquals("Second to last should be ROW_ID", ROW_ID, fieldsWithUserData.get(fieldsWithUserData.size() - 2).getName());
        assertEquals("Last should be PRIMARY_TERM", PRIMARY_TERM, fieldsWithUserData.get(fieldsWithUserData.size() - 1).getName());
    }


    /**
     * Ensures that OpenSearch metadata fields are properly excluded from Arrow schema generation.
     * 
     * <p>This test validates that internal OpenSearch fields such as:
     * <ul>
     *   <li>_source - document source storage</li>
     *   <li>_field_names - field name tracking</li>
     *   <li>_index - index metadata</li>
     *   <li>_nested_path - nested document paths</li>
     *   <li>_feature - feature fields</li>
     *   <li>_data_stream_timestamp - data stream timestamps</li>
     * </ul>
     * 
     * <p>Are filtered out while user-defined fields are preserved in the schema.
     */
    public void testMetadataFieldsAreExcluded() {
        Mapper textMapper = createFieldMapper(TITLE_FIELD, TEXT_TYPE);

        MapperService mapperService = createMockMapperService(Arrays.asList(
            textMapper,
            createMetadataMapper(SOURCE_FIELD, SourceFieldMapper.class),
            createMetadataMapper(FIELD_NAMES_FIELD, FieldNamesFieldMapper.class),
            createMetadataMapper(INDEX_FIELD, IndexFieldMapper.class),
            createMetadataMapper(NESTED_PATH_FIELD, NestedPathFieldMapper.class),
            createFieldMapper(FEATURE_FIELD, FEATURE_FIELD),
            createFieldMapper(DATA_STREAM_TIMESTAMP_FIELD, DATA_STREAM_TIMESTAMP_FIELD)
        ));

        Schema schema = ArrowSchemaBuilder.getSchema(mapperService);
        List<String> fieldNames = getFieldNames(schema);

        assertEquals("Should have 3 fields (1 user + 2 system)", EXPECTED_FIELDS_WITH_ONE_USER_FIELD, schema.getFields().size());
        assertTrue("Should contain user field", fieldNames.contains(TITLE_FIELD));
        assertFalse("Should not contain _source", fieldNames.contains(SOURCE_FIELD));
        assertFalse("Should not contain _field_names", fieldNames.contains(FIELD_NAMES_FIELD));
        assertFalse("Should not contain _index", fieldNames.contains(INDEX_FIELD));
        assertFalse("Should not contain _nested_path", fieldNames.contains(NESTED_PATH_FIELD));
        assertFalse("Should not contain _feature", fieldNames.contains(FEATURE_FIELD));
        assertFalse("Should not contain _data_stream_timestamp", fieldNames.contains(DATA_STREAM_TIMESTAMP_FIELD));
    }


    /**
     * Validates the conversion of OpenSearch field types to Arrow field types.
     * 
     * <p>This test ensures that:
     * <ul>
     *   <li>Supported field types are correctly converted using ArrowFieldRegistry</li>
     *   <li>Field names are preserved during conversion</li>
     *   <li>Arrow field types match the registry definitions</li>
     *   <li>Multiple fields are handled correctly in a single schema</li>
     * </ul>
     * 
     * <p>Uses realistic field names (user_id, email, timestamp) to simulate
     * production scenarios and validate end-to-end field conversion.
     */
    public void testFieldConversionBehavior() {
        String supportedType = getFirstRegisteredType();
        List<String> testTypes = ArrowFieldRegistry.getRegisteredFieldNames().stream()
            .limit(TEST_FIELD_LIMIT)
            .collect(Collectors.toList());

        MapperService singleFieldService = createMockMapperService(
            Collections.singletonList(createFieldMapper(SAMPLE_FIELD, supportedType)));
        MapperService multiFieldService = createMockMapperService(Arrays.asList(
            createFieldMapper(USER_ID_FIELD, testTypes.get(0)),
            createFieldMapper(EMAIL_FIELD, testTypes.get(1)),
            createFieldMapper(TIMESTAMP_FIELD, testTypes.get(2))
        ));

        Schema singleFieldSchema = ArrowSchemaBuilder.getSchema(singleFieldService);
        Schema multiFieldSchema = ArrowSchemaBuilder.getSchema(multiFieldService);
        
        Field userField = findField(singleFieldSchema, SAMPLE_FIELD);
        List<String> multiFieldNames = getFieldNames(multiFieldSchema);

        assertNotNull("User field should be present", userField);
        assertEquals("Field name should match", SAMPLE_FIELD, userField.getName());

        ParquetField registryField = ArrowFieldRegistry.getParquetField(supportedType);
        assertEquals("Field type should match registry", registryField.getFieldType(), userField.getFieldType());
        assertEquals("Arrow type should match registry", registryField.getArrowType(), userField.getFieldType().getType());

        assertEquals("Should have 5 fields (3 user + 2 system)", EXPECTED_MULTI_FIELD_COUNT, multiFieldSchema.getFields().size());
        assertTrue("Should contain user_id field", multiFieldNames.contains(USER_ID_FIELD));
        assertTrue("Should contain email field", multiFieldNames.contains(EMAIL_FIELD));
        assertTrue("Should contain timestamp field", multiFieldNames.contains(TIMESTAMP_FIELD));
    }

    /**
     * Verifies that unsupported field types result in appropriate exceptions.
     * 
     * <p>This test ensures that when an OpenSearch field type is not registered
     * in the ArrowFieldRegistry, the schema builder throws an {@link IllegalStateException}
     * with a descriptive error message containing both the unsupported type name
     * and the field name for debugging purposes.
     */
    public void testUnsupportedFieldTypeThrowsException() {
        MapperService mapperService = createMockMapperService(
            Collections.singletonList(createFieldMapper(UNSUPPORTED_FIELD, UNKNOWN_TYPE)));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> ArrowSchemaBuilder.getSchema(mapperService));

        assertTrue("Should mention unsupported type", exception.getMessage().contains(UNSUPPORTED_TYPE_ERROR));
        assertTrue("Should mention field name", exception.getMessage().contains(UNSUPPORTED_FIELD));
    }


    /**
     * Validates that ArrowSchemaBuilder cannot be instantiated as it is a utility class.
     * 
     * <p>This test ensures proper utility class design by verifying that:
     * <ul>
     *   <li>The private constructor throws UnsupportedOperationException</li>
     *   <li>Reflection-based instantiation attempts are properly handled</li>
     *   <li>The error message clearly indicates the class should not be instantiated</li>
     * </ul>
     * 
     * <p>This prevents misuse of the utility class and enforces the static-only API design.
     */
    public void testUtilityClassCannotBeInstantiated() throws Exception {
        Constructor<?> constructor = ArrowSchemaBuilder.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        InvocationTargetException exception = expectThrows(InvocationTargetException.class, constructor::newInstance);

        assertTrue("Should throw UnsupportedOperationException",
            exception.getCause() instanceof UnsupportedOperationException);
        assertEquals(UTILITY_CLASS_ERROR, exception.getCause().getMessage());
    }


    /**
     * Creates a mock MapperService with the specified mappers for testing.
     * 
     * <p>This helper method constructs a properly configured mock MapperService
     * that filters out metadata mappers and provides the necessary mapping lookup
     * functionality for schema generation tests.
     * 
     * @param mappers the list of mappers to include in the service
     * @return a configured mock MapperService
     */
    private MapperService createMockMapperService(List<Mapper> mappers) {
        MapperService mapperService = mock(MapperService.class);
        org.opensearch.index.mapper.DocumentMapper documentMapper = mock(org.opensearch.index.mapper.DocumentMapper.class);

        List<FieldMapper> fieldMappers = mappers.stream()
            .filter(m -> m instanceof FieldMapper)
            .filter(m -> !isMetadataMapper(m))
            .map(m -> (FieldMapper) m)
            .collect(Collectors.toList());

        org.opensearch.index.mapper.MappingLookup mappingLookup = new org.opensearch.index.mapper.MappingLookup(
            fieldMappers,
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            new org.apache.lucene.analysis.standard.StandardAnalyzer()
        );

        when(documentMapper.mappers()).thenReturn(mappingLookup);
        when(mapperService.documentMapper()).thenReturn(documentMapper);

        return mapperService;
    }

    /**
     * Creates a mock FieldMapper with the specified name and type for testing.
     * 
     * <p>This helper method constructs a fully configured mock FieldMapper
     * with all necessary properties set for schema generation testing.
     * 
     * @param name the field name
     * @param typeName the OpenSearch field type name
     * @return a configured mock FieldMapper
     */
    private FieldMapper createFieldMapper(String name, String typeName) {
        FieldMapper mapper = mock(FieldMapper.class);
        MappedFieldType fieldType = mock(MappedFieldType.class);
        FieldMapper.CopyTo copyTo = mock(FieldMapper.CopyTo.class);

        when(mapper.name()).thenReturn(name);
        when(mapper.typeName()).thenReturn(typeName);
        when(mapper.fieldType()).thenReturn(fieldType);
        when(mapper.copyTo()).thenReturn(copyTo);
        when(copyTo.copyToFields()).thenReturn(Collections.emptyList());
        when(fieldType.name()).thenReturn(name);
        when(fieldType.indexAnalyzer()).thenReturn(null);

        return mapper;
    }

    /**
     * Creates a mock metadata mapper for testing metadata field exclusion.
     * 
     * <p>This helper method creates mock mappers that represent OpenSearch
     * internal metadata fields which should be excluded from Arrow schema generation.
     * 
     * @param name the metadata field name
     * @param mapperClass the specific mapper class type
     * @param <T> the mapper type extending Mapper
     * @return a configured mock metadata mapper
     */
    private <T extends Mapper> Mapper createMetadataMapper(String name, Class<T> mapperClass) {
        Mapper mapper = mock(mapperClass);
        when(mapper.name()).thenReturn(name);
        when(mapper.typeName()).thenReturn(name);
        return mapper;
    }

    /**
     * Determines if a mapper represents an OpenSearch metadata field.
     * 
     * <p>This method identifies mappers that should be excluded from Arrow
     * schema generation by checking against known metadata mapper types
     * and field name patterns.
     * 
     * @param mapper the mapper to check
     * @return true if the mapper represents a metadata field, false otherwise
     */
    private boolean isMetadataMapper(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || FEATURE_FIELD.equals(mapper.typeName())
            || DATA_STREAM_TIMESTAMP_FIELD.equals(mapper.typeName());
    }

    /**
     * Retrieves the first registered field type from ArrowFieldRegistry for testing.
     * 
     * <p>This helper method provides a supported field type that can be used
     * in tests to validate successful field conversion scenarios.
     * 
     * @return the first registered field type name
     * @throws IllegalStateException if no field types are registered
     */
    private String getFirstRegisteredType() {
        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        if (registeredTypes.isEmpty()) {
            throw new IllegalStateException("No field types are registered in ArrowFieldRegistry");
        }
        return registeredTypes.iterator().next();
    }

    /**
     * Extracts field names from an Arrow schema for validation purposes.
     * 
     * <p>This utility method simplifies test assertions by providing
     * a list of field names from the schema for easy verification.
     * 
     * @param schema the Arrow schema to extract names from
     * @return a list of field names in the schema
     */
    private List<String> getFieldNames(Schema schema) {
        return schema.getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());
    }

    /**
     * Finds a specific field by name within an Arrow schema.
     * 
     * <p>This utility method enables targeted field validation by locating
     * a specific field within the schema for detailed property verification.
     * 
     * @param schema the Arrow schema to search
     * @param name the field name to find
     * @return the matching Field, or null if not found
     */
    private Field findField(Schema schema, String name) {
        return schema.getFields().stream()
            .filter(f -> name.equals(f.getName()))
            .findFirst()
            .orElse(null);
    }
}
